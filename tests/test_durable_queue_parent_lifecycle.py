import json
from contextlib import contextmanager

import pytest

from orchestration.transform_runtime.durable_queue import DurableQueueError, LeaseLost, PostgresDurableQueue


TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "ORPHANED"}


class Result:
    def __init__(self, *, one=None, many=None):
        self.one = one
        self.many = many or []

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.many


class LifecycleDatabase:
    def __init__(self, *, child_status="RUNNING", parent_status="ADMITTED", expired=False):
        self.child = {
            "batch_execution_id": "be_" + "a" * 32,
            "transform_run_id": "tr_" + "b" * 32,
            "status": child_status,
            "attempt": 1,
            "lease_owner": "worker-1" if child_status in {"RUNNING", "TESTING"} else None,
            "lease_token": "lease-1" if child_status in {"RUNNING", "TESTING"} else None,
            "cancel_requested": False,
            "failure_class": None,
            "nessie_branch": None,
        }
        self.parent = {"transform_run_id": self.child["transform_run_id"], "status": parent_status}
        self.attempt_status = child_status
        self.events = []
        self.expired = expired

    def execute(self, statement, params=()):
        sql = " ".join(statement.split())

        if sql.startswith("SELECT * FROM batch_executions"):
            if "lease_owner=%s" in sql:
                eid, owner, token = params
                matches = (
                    eid == self.child["batch_execution_id"]
                    and owner == self.child["lease_owner"]
                    and token == self.child["lease_token"]
                    and self.child["status"] in {"RUNNING", "TESTING"}
                )
                return Result(one=dict(self.child) if matches else None)
            if "lease_expires_at<clock_timestamp()" in sql:
                matches = self.expired and self.child["status"] in {"RUNNING", "TESTING"}
                return Result(many=[dict(self.child)] if matches else [])
            return Result(one=dict(self.child) if params[0] == self.child["batch_execution_id"] else None)

        if sql.startswith("UPDATE batch_executions"):
            if "SET nessie_branch=%s" in sql:
                branch, eid, owner, token, expected_branch = params
                matches = (
                    eid == self.child["batch_execution_id"]
                    and owner == self.child["lease_owner"]
                    and token == self.child["lease_token"]
                    and self.child["status"] in {"RUNNING", "TESTING"}
                    and not self.expired
                    and self.child["nessie_branch"] in {None, expected_branch}
                )
                if not matches:
                    return Result()
                self.child["nessie_branch"] = branch
                return Result(one={"nessie_branch": branch})
            if "status=%s" in sql:
                status, test_status, failure_class, eid, owner, token = params
                if not (
                    eid == self.child["batch_execution_id"]
                    and owner == self.child["lease_owner"]
                    and token == self.child["lease_token"]
                    and self.child["status"] in {"RUNNING", "TESTING"}
                    and not self.expired
                ):
                    return Result()
                self.child.update(status=status, test_status=test_status, failure_class=failure_class)
            elif "status='CANCELLED'" in sql:
                self.child.update(status="CANCELLED", cancel_requested=True, failure_class="OPERATOR_CANCELLED")
            elif "status='ORPHANED'" in sql:
                failure_class = params[0] if len(params) > 1 else "WORKER_LEASE_EXPIRED_REQUIRES_RECONCILIATION"
                self.child.update(status="ORPHANED", failure_class=failure_class)
            else:  # pragma: no cover - catches unexpected queue SQL in these focused tests
                raise AssertionError(sql)
            self.child.update(lease_owner=None, lease_token=None)
            return Result(one=dict(self.child))

        if sql.startswith("UPDATE batch_attempts"):
            self.attempt_status = params[0] if "status=%s" in sql else "ORPHANED"
            return Result()

        if sql.startswith("UPDATE transform_runs"):
            status, run_id = params
            if run_id == self.parent["transform_run_id"] and self.parent["status"] not in TERMINAL:
                self.parent["status"] = status
                return Result(one={"transform_run_id": run_id})
            return Result()

        if sql.startswith("INSERT INTO execution_events"):
            _, run_id, execution_id, event_type, metadata = params
            self.events.append(
                {
                    "transform_run_id": run_id,
                    "batch_execution_id": execution_id,
                    "event_type": event_type,
                    "metadata": json.loads(metadata),
                }
            )
            return Result()

        raise AssertionError(sql)


class QueueHarness(PostgresDurableQueue):
    def __init__(self, database):
        self.database = database

    @contextmanager
    def tx(self):
        yield self.database


def parent_events(database, event_type):
    return [
        event
        for event in database.events
        if event["event_type"] == event_type and event["batch_execution_id"] is None
    ]


@pytest.mark.parametrize(
    ("status", "event_type"),
    [("FAILED", "RUN_FAILED"), ("CANCELLED", "RUN_CANCELLED"), ("ORPHANED", "RUN_ORPHANED")],
)
def test_finish_terminalizes_parent_once(status, event_type):
    database = LifecycleDatabase()
    queue = QueueHarness(database)

    queue.finish(
        database.child["batch_execution_id"],
        "worker-1",
        "lease-1",
        status,
        failure_class="CHILD_FAILURE",
    )

    assert database.child["status"] == status
    assert database.attempt_status == status
    assert database.parent["status"] == status
    assert parent_events(database, event_type) == [
        {
            "transform_run_id": database.parent["transform_run_id"],
            "batch_execution_id": None,
            "event_type": event_type,
            "metadata": {
                "batch_execution_id": database.child["batch_execution_id"],
                "failure_class": "CHILD_FAILURE",
            },
        }
    ]


def test_orphan_if_owned_terminalizes_parent_once():
    database = LifecycleDatabase()
    queue = QueueHarness(database)

    assert queue.orphan_if_owned(database.child["batch_execution_id"], "worker-1", "lease-1", "WORKER_ERROR")
    assert database.child["status"] == "ORPHANED"
    assert database.parent["status"] == "ORPHANED"
    assert len(parent_events(database, "RUN_ORPHANED")) == 1


def test_reconcile_expired_terminalizes_parent_once():
    database = LifecycleDatabase(expired=True)
    queue = QueueHarness(database)

    assert queue.reconcile_expired() == [database.child["batch_execution_id"]]
    assert database.child["status"] == "ORPHANED"
    assert database.parent["status"] == "ORPHANED"
    assert len(parent_events(database, "RUN_ORPHANED")) == 1


@pytest.mark.parametrize("child_status", ["ADMITTED", "QUEUED"])
def test_request_cancel_terminalizes_parent_once(child_status):
    database = LifecycleDatabase(child_status=child_status)
    queue = QueueHarness(database)

    queue.request_cancel(database.child["batch_execution_id"])
    assert database.child["status"] == "CANCELLED"
    assert database.parent["status"] == "CANCELLED"
    assert len(parent_events(database, "RUN_CANCELLED")) == 1


def test_success_does_not_guess_parent_completion():
    database = LifecycleDatabase()
    queue = QueueHarness(database)

    queue.finish(database.child["batch_execution_id"], "worker-1", "lease-1", "SUCCEEDED")
    assert database.child["status"] == "SUCCEEDED"
    assert database.parent["status"] == "ADMITTED"
    assert not parent_events(database, "RUN_SUCCEEDED")


def test_already_terminal_parent_is_not_overwritten_or_duplicated():
    database = LifecycleDatabase(parent_status="FAILED")
    queue = QueueHarness(database)

    queue.finish(database.child["batch_execution_id"], "worker-1", "lease-1", "ORPHANED")
    assert database.parent["status"] == "FAILED"
    assert not parent_events(database, "RUN_ORPHANED")


@pytest.mark.parametrize(
    ("worker", "token", "expired"),
    [("wrong-worker", "lease-1", False), ("worker-1", "wrong-token", False), ("worker-1", "lease-1", True)],
)
def test_finish_preserves_lease_fencing(worker, token, expired):
    database = LifecycleDatabase(expired=expired)
    queue = QueueHarness(database)

    with pytest.raises(LeaseLost, match="worker lease lost"):
        queue.finish(database.child["batch_execution_id"], worker, token, "FAILED")

    assert database.child["status"] == "RUNNING"
    assert database.parent["status"] == "ADMITTED"
    assert database.events == []


def test_record_nessie_branch_from_null():
    database = LifecycleDatabase()
    queue = QueueHarness(database)
    branch = "transform_" + database.child["transform_run_id"]
    assert queue.record_nessie_branch(database.child["batch_execution_id"], "worker-1", "lease-1", branch) == branch
    assert database.child["nessie_branch"] == branch


def test_record_nessie_branch_is_idempotent():
    database = LifecycleDatabase()
    queue = QueueHarness(database)
    branch = "transform_" + database.child["transform_run_id"]
    database.child["nessie_branch"] = branch
    assert queue.record_nessie_branch(database.child["batch_execution_id"], "worker-1", "lease-1", branch) == branch
    assert database.child["nessie_branch"] == branch


def test_record_nessie_branch_conflict_fails_closed():
    database = LifecycleDatabase()
    queue = QueueHarness(database)
    database.child["nessie_branch"] = "transform_tr_" + "c" * 32
    with pytest.raises(DurableQueueError, match="conflicts with persisted branch"):
        queue.record_nessie_branch(database.child["batch_execution_id"], "worker-1", "lease-1", "transform_" + database.child["transform_run_id"])
    assert database.child["nessie_branch"] == "transform_tr_" + "c" * 32


@pytest.mark.parametrize(("worker", "token"), [("wrong-worker", "lease-1"), ("worker-1", "wrong-token")])
def test_record_nessie_branch_preserves_lease_fencing(worker, token):
    database = LifecycleDatabase()
    queue = QueueHarness(database)
    with pytest.raises(LeaseLost, match="worker lease lost"):
        queue.record_nessie_branch(database.child["batch_execution_id"], worker, token, "transform_" + database.child["transform_run_id"])
    assert database.child["nessie_branch"] is None
