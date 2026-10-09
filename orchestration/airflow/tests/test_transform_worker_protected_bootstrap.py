import os
from unittest import mock

import pytest

from orchestration.job_runner import transform_worker


class FakeQueue:
    def __init__(self, claim_row):
        self._claim_row = claim_row
        self.finished = []
        self.heartbeats = 0
        self.cancel_requested = False

    def claim_protected_bootstrap(self, worker, lease):
        return self._claim_row

    def heartbeat_protected_bootstrap(self, eid, worker, token, lease):
        self.heartbeats += 1
        return True

    def cancel_requested_protected_bootstrap(self, eid, worker, token):
        return self.cancel_requested

    def finish_protected_bootstrap(self, eid, worker, token, status, publication_state, failure_class=None):
        self.finished.append((eid, status, publication_state, failure_class))
        return {"bootstrap_execution_id": eid, "status": status}


def _row(**overrides):
    row = {
        "bootstrap_execution_id": "be_" + "a" * 32,
        "operation_id": "initial_token_link_creation",
        "operation_version": 1,
        "transform_run_id": "tr_" + "b" * 32,
        "lease_token": "tok",
        "reconciliation_only": False,
    }
    row.update(overrides)
    return row


@pytest.fixture(autouse=True)
def _enabled(monkeypatch):
    monkeypatch.setenv("PLATFORM_RUNNER_EXECUTION_ENABLED", "true")
    monkeypatch.setenv("PLATFORM_JOB_EXECUTION_ENABLED", "true")


def _fake_result(status):
    class R:
        pass
    r = R()
    r.status = status
    r.return_code = 0 if status == "SUCCEEDED" else 1
    r.output = ""
    r.output_truncated = False
    r.failure_class = None
    return r


def test_success_path_writes_published(monkeypatch):
    q = FakeQueue(_row())
    monkeypatch.setattr(transform_worker, "build_protected_bootstrap_command",
                        lambda *a, **kw: mock.MagicMock())
    monkeypatch.setattr(transform_worker, "execute",
                        lambda *a, **kw: _fake_result("SUCCEEDED"))
    assert transform_worker.run_once_protected_bootstrap(queue=q, worker="w") is True
    assert q.finished == [("be_" + "a" * 32, "SUCCEEDED", "PUBLISHED", None)]


def test_failure_writes_not_started(monkeypatch):
    q = FakeQueue(_row())
    monkeypatch.setattr(transform_worker, "build_protected_bootstrap_command",
                        lambda *a, **kw: mock.MagicMock())
    monkeypatch.setattr(transform_worker, "execute",
                        lambda *a, **kw: _fake_result("FAILED"))
    assert transform_worker.run_once_protected_bootstrap(queue=q, worker="w") is True
    eid, status, pub, fc = q.finished[0]
    assert status == "FAILED"
    assert pub == "NOT_STARTED"


def test_command_rejection_is_terminal_failure(monkeypatch):
    q = FakeQueue(_row())
    def _raise(*a, **kw):
        raise ValueError("bad op")
    monkeypatch.setattr(transform_worker, "build_protected_bootstrap_command", _raise)
    assert transform_worker.run_once_protected_bootstrap(queue=q, worker="w") is True
    eid, status, pub, fc = q.finished[0]
    assert status == "FAILED"
    assert pub == "NOT_STARTED"
    assert fc and fc.startswith("BOOTSTRAP_COMMAND_REJECTED")


def test_nothing_to_claim_returns_false(monkeypatch):
    q = FakeQueue(None)
    assert transform_worker.run_once_protected_bootstrap(queue=q, worker="w") is False
