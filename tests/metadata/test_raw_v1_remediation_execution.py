"""Offline controlled-remediation orchestration acceptance."""

import json
import sys

from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/metadata"))

import raw_v1_metadata_remediation as executor
import raw_v1_remediation_safety as safety


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    directory = tmp_path / "execution"
    directory.mkdir()

    approval_path = directory / "execution-approval.json"
    manifest_path = tmp_path / "manifest.json"

    objects = [
        {
            "object": f"object-{i}",
            "execution_authorised": False,
            "remediation_type": "STANDARD_METADATA_REMEDIATION"
        }
        for i in range(10)
    ]

    objects[0].update({
        "object": executor.CONFLICT_OBJECT,
        "remediation_type": "EXPLICIT_CONFLICT_MIGRATION",
        "before": {"metadata": {"data-domain": "payments"}},
        "expected_metadata": {"data-domain": "agent_management"}
    })

    manifest = {
        "objects": objects,
        "execution_authorised": False,
        "bulk_execution_authorised": False
    }

    manifest_path.write_text(json.dumps(manifest))

    approval = {
        "capability": "raw_metadata_controlled_live_remediation",
        "approval_authority": "user",
        "approval_status": "APPROVED",
        "project": executor.PROJECT,
        "bucket": executor.BUCKET,
        "metadata_contract_version": "1.0.0",
        "pilot_manifest_sha256": executor.PILOT_SHA,
        "pilot_objects": 10,
        "live_write_authorised": True,
        "conflict_migration_authorised": True,
        "bulk_execution_authorised": False
    }

    approval_path.write_text(json.dumps(approval))

    monkeypatch.setattr(safety, "APPROVAL_PATH", approval_path)
    monkeypatch.setattr(safety, "EXECUTION_DIRECTORY", directory)
    monkeypatch.setattr(
        safety, "LOCK_PATH", tmp_path / "executor.lock"
    )

    # The production manifest loader is tested separately.
    # Here the approved scope is a deterministic offline fixture.
    monkeypatch.setattr(
        executor, "load_manifest", lambda path: manifest
    )

    names = [item["object"] for item in objects]

    state = {
        "compliant": set(),
        "writes": [],
        "assessments": [],
        "failure": None,
        "failure_index": 3
    }

    def assess(bucket, item, decoder, allow_conflict):
        name = item["object"]
        state["assessments"].append(name)

        return (
            None, None, None, None,
            name in state["compliant"]
        )

    def remediate(
        bucket, item, decoder, apply,
        allow_conflict, record
    ):
        assert apply is True
        assert allow_conflict is True

        name = item["object"]
        index = names.index(name)

        record({
            "object": name,
            "status": "PATCH_INTENT"
        })

        if (
            state["failure"] == "BEFORE_PATCH"
            and index == state["failure_index"]
        ):
            raise RuntimeError("Simulated conditional patch failure")

        state["writes"].append(name)
        state["compliant"].add(name)

        if (
            state["failure"] == "AFTER_PATCH"
            and index == state["failure_index"]
        ):
            raise RuntimeError("Simulated ambiguous patch outcome")

        if (
            state["failure"] == "INVALID_RESULT"
            and index == state["failure_index"]
        ):
            return {
                "object": name,
                "status": "UPDATED_VERIFIED",
                "payload_verified": False
            }

        return {
            "object": name,
            "status": "UPDATED_VERIFIED",
            "payload_verified": True
        }

    monkeypatch.setattr(executor, "assess", assess)
    monkeypatch.setattr(executor, "remediate", remediate)

    return {
        "state": state,
        "names": names,
        "manifest": manifest,
        "manifest_path": manifest_path,
        "approval_path": approval_path,
        "journal": directory / "execution-journal.jsonl",
        "bucket": Mock(),
        "decoder": Mock()
    }


def run(runtime):
    return safety.run_controlled_apply(
        runtime["bucket"],
        runtime["manifest_path"],
        runtime["decoder"]
    )


def journal_state(runtime):
    return safety.reconcile_journal(
        runtime["journal"],
        set(runtime["names"])
    )


def test_complete_ten_object_execution(runtime):
    results = run(runtime)

    assert len(results) == 10
    assert runtime["state"]["writes"] == runtime["names"]

    assert all(
        item["status"] == "UPDATED_VERIFIED"
        for item in results
    )

    state = journal_state(runtime)

    assert set(state["completed"]) == set(runtime["names"])
    assert not state["pending"]
    assert not state["quarantined"]


def test_successful_restart_performs_no_new_writes(runtime):
    run(runtime)
    results = run(runtime)

    assert len(runtime["state"]["writes"]) == 10

    assert all(
        item["status"] == "PREVIOUSLY_VERIFIED"
        for item in results
    )


def test_all_objects_reassessed_after_execution(runtime):
    run(runtime)

    assert runtime["state"]["assessments"] == (
        runtime["names"] + runtime["names"]
    )


def test_missing_approval_prevents_all_writes(runtime):
    runtime["approval_path"].unlink()

    with pytest.raises(ValueError):
        run(runtime)

    assert runtime["state"]["writes"] == []
    assert not runtime["journal"].exists()


def test_duplicate_object_prevents_all_writes(runtime):
    objects = runtime["manifest"]["objects"]
    objects[2] = objects[1]

    with pytest.raises(ValueError):
        run(runtime)

    assert runtime["state"]["writes"] == []


@pytest.mark.parametrize("failure,expected_writes", [
    ("BEFORE_PATCH", 3),
    ("AFTER_PATCH", 4),
    ("INVALID_RESULT", 4)
])
def test_failure_containment(
    runtime, failure, expected_writes
):
    runtime["state"]["failure"] = failure

    with pytest.raises((RuntimeError, ValueError)):
        run(runtime)

    assert len(runtime["state"]["writes"]) == expected_writes

    state = journal_state(runtime)

    assert runtime["names"][3] in state["pending"]
    assert runtime["names"][3] in state["quarantined"]

    previous = list(runtime["state"]["writes"])

    with pytest.raises(ValueError):
        run(runtime)

    assert runtime["state"]["writes"] == previous


def test_journal_intent_failure_prevents_patch(
    runtime, monkeypatch
):
    original = safety.durable_event

    def failing_event(path, event):
        if event["status"] == "PATCH_INTENT":
            raise OSError("Simulated journal failure")

        return original(path, event)

    monkeypatch.setattr(
        safety, "durable_event", failing_event
    )

    with pytest.raises(OSError):
        run(runtime)

    assert runtime["state"]["writes"] == []


def test_completion_journal_failure_blocks_restart(
    runtime, monkeypatch
):
    original = safety.durable_event

    def failing_event(path, event):
        if event["status"] == "UPDATED_VERIFIED":
            raise OSError("Simulated completion journal failure")

        return original(path, event)

    monkeypatch.setattr(
        safety, "durable_event", failing_event
    )

    with pytest.raises(OSError):
        run(runtime)

    assert len(runtime["state"]["writes"]) == 1

    state = journal_state(runtime)

    assert runtime["names"][0] in state["pending"]
    assert runtime["names"][0] in state["quarantined"]

    monkeypatch.setattr(
        safety, "durable_event", original
    )

    with pytest.raises(ValueError):
        run(runtime)

    assert len(runtime["state"]["writes"]) == 1


def test_completed_object_drift_blocks_restart(runtime):
    run(runtime)

    runtime["state"]["compliant"].remove(
        runtime["names"][0]
    )

    with pytest.raises(ValueError):
        run(runtime)

    assert len(runtime["state"]["writes"]) == 10


def test_unjournalled_compliance_blocks_execution(runtime):
    runtime["state"]["compliant"].add(
        runtime["names"][0]
    )

    with pytest.raises(ValueError):
        run(runtime)

    assert runtime["state"]["writes"] == []


def test_changed_approval_blocks_restart(runtime):
    run(runtime)

    approval = json.loads(
        runtime["approval_path"].read_text()
    )

    approval["unexpected_change"] = True

    runtime["approval_path"].write_text(
        json.dumps(approval)
    )

    with pytest.raises(ValueError):
        run(runtime)

    assert len(runtime["state"]["writes"]) == 10


def test_lock_contention_prevents_execution(runtime):
    with safety.execution_lock(safety.LOCK_PATH):
        with pytest.raises(RuntimeError):
            run(runtime)

    assert runtime["state"]["writes"] == []
