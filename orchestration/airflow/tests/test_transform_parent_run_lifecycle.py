from types import SimpleNamespace

import pytest

from orchestration.transform_runtime.ledger import LedgerError, TransformLedger


def batch():
    return SimpleNamespace(
        batch_id="C4_RAW_02",
        authority="ordinary_transform",
        prerequisite_batches=(),
    )


def new_run(ledger, key):
    return ledger.create_run(
        "airflow",
        key,
        "snapshot",
        "a" * 64,
        "b" * 64,
    )


def submit(ledger, run, key):
    return ledger.submit_batch(
        run["transform_run_id"],
        "airflow",
        key,
        batch(),
        "c" * 64,
    )


def to_testing(ledger, execution_id):
    ledger.transition(execution_id, "QUEUED")
    ledger.transition(execution_id, "RUNNING")
    ledger.transition(execution_id, "TESTING")


def parent_status(ledger, run):
    row = ledger.connection.execute(
        "SELECT status FROM transform_runs "
        "WHERE transform_run_id=?",
        (run["transform_run_id"],),
    ).fetchone()
    return row["status"]


def test_failed_batch_fails_parent():
    ledger = TransformLedger()
    run = new_run(ledger, "run-failed")
    execution = submit(ledger, run, "batch-failed")
    eid = execution["batch_execution_id"]
    to_testing(ledger, eid)
    ledger.transition(
        eid,
        "FAILED",
        test_status="NOT_RUN",
        failure_class="TEST_FAILURE",
    )
    assert parent_status(ledger, run) == "FAILED"


def test_cancelled_batch_cancels_parent():
    ledger = TransformLedger()
    run = new_run(ledger, "run-cancelled")
    execution = submit(ledger, run, "batch-cancelled")
    ledger.transition(
        execution["batch_execution_id"],
        "CANCELLED",
    )
    assert parent_status(ledger, run) == "CANCELLED"


def test_orphaned_batch_orphans_parent():
    ledger = TransformLedger()
    run = new_run(ledger, "run-orphaned")
    execution = submit(ledger, run, "batch-orphaned")
    eid = execution["batch_execution_id"]
    ledger.transition(eid, "QUEUED")
    ledger.transition(eid, "RUNNING")
    ledger.transition(
        eid,
        "ORPHANED",
        failure_class="TEST_ORPHAN",
    )
    assert parent_status(ledger, run) == "ORPHANED"


def test_failed_parent_releases_next_run():
    ledger = TransformLedger()
    run = new_run(ledger, "run-one")
    execution = submit(ledger, run, "batch-one")
    eid = execution["batch_execution_id"]
    to_testing(ledger, eid)
    ledger.transition(
        eid,
        "FAILED",
        test_status="NOT_RUN",
        failure_class="TEST_FAILURE",
    )
    replacement = new_run(ledger, "run-two")
    assert replacement["status"] == "ADMITTED"


def test_success_does_not_guess_parent_completion():
    ledger = TransformLedger()
    run = new_run(ledger, "run-success")
    execution = submit(ledger, run, "batch-success")
    eid = execution["batch_execution_id"]
    to_testing(ledger, eid)
    ledger.transition(
        eid,
        "SUCCEEDED",
        test_status="PASSED",
    )
    assert parent_status(ledger, run) == "ADMITTED"


def test_active_parent_still_blocks_second_run():
    ledger = TransformLedger()
    new_run(ledger, "active-one")
    with pytest.raises(
        LedgerError,
        match="one active transform run",
    ):
        new_run(ledger, "active-two")
