import copy
import json
import sys

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/metadata"))

import raw_v1_remediation_safety as safety
import raw_v1_metadata_remediation as executor


@pytest.fixture
def manifest():
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

    return {
        "objects": objects,
        "execution_authorised": False,
        "bulk_execution_authorised": False
    }


def test_valid_scope(manifest):
    assert len(safety.validate_scope(manifest, manifest)) == 10


def test_duplicate_rejected(manifest):
    manifest["objects"][2] = manifest["objects"][1]

    with pytest.raises(ValueError, match="Duplicate"):
        safety.validate_scope(manifest, manifest)


@pytest.mark.parametrize("count", [0, 1, 9, 11])
def test_invalid_count(manifest, count):
    manifest["objects"] = [
        manifest["objects"][0]
    ] * count

    with pytest.raises(ValueError):
        safety.validate_scope(manifest, manifest)


def test_changed_scope(manifest):
    changed = copy.deepcopy(manifest)
    changed["objects"][1]["object"] = "unapproved"

    with pytest.raises(ValueError, match="approved scope"):
        safety.validate_scope(changed, manifest)


@pytest.mark.parametrize("field", [
    "execution_authorised",
    "bulk_execution_authorised"
])
def test_embedded_authorisation(manifest, field):
    manifest[field] = True

    with pytest.raises(ValueError):
        safety.validate_scope(manifest, manifest)


def test_object_authorisation(manifest):
    manifest["objects"][1]["execution_authorised"] = True

    with pytest.raises(ValueError):
        safety.validate_scope(manifest, manifest)


def test_wrong_conflict_identity(manifest):
    manifest["objects"][0]["object"] = "wrong"

    with pytest.raises(ValueError):
        safety.validate_scope(manifest, manifest)


def test_wrong_conflict_target(manifest):
    manifest["objects"][0]["expected_metadata"]["data-domain"] = "wrong"

    with pytest.raises(ValueError):
        safety.validate_scope(manifest, manifest)


def test_lock_exclusion(tmp_path):
    lock = tmp_path / "executor.lock"

    with safety.execution_lock(lock):
        with pytest.raises(RuntimeError):
            with safety.execution_lock(lock):
                pass


def test_lock_released_after_failure(tmp_path):
    lock = tmp_path / "executor.lock"

    with pytest.raises(ValueError):
        with safety.execution_lock(lock):
            raise ValueError("fixture failure")

    with safety.execution_lock(lock):
        pass


def journal(tmp_path, events):
    path = tmp_path / "journal.jsonl"

    path.write_text(
        "".join(json.dumps(event) + "\n" for event in events)
    )

    return path


def test_unresolved_patch_blocks_restart(tmp_path):
    path = journal(tmp_path, [
        {"object": "a", "status": "PATCH_INTENT"}
    ])

    state = safety.reconcile_journal(path, {"a"})

    with pytest.raises(ValueError, match="Unresolved"):
        safety.require_reconciled(state)


def test_verified_completion_reconciles(tmp_path):
    path = journal(tmp_path, [
        {"object": "a", "status": "PATCH_INTENT"},
        {
            "object": "a",
            "status": "UPDATED_VERIFIED",
            "payload_verified": True
        }
    ])

    state = safety.reconcile_journal(path, {"a"})

    assert state["completed"] == ["a"]
    assert state["pending"] == []

    safety.require_reconciled(state)


def test_completion_without_intent_rejected(tmp_path):
    path = journal(tmp_path, [
        {
            "object": "a",
            "status": "UPDATED_VERIFIED",
            "payload_verified": True
        }
    ])

    with pytest.raises(ValueError):
        safety.reconcile_journal(path, {"a"})


def test_quarantine_blocks_restart(tmp_path):
    path = journal(tmp_path, [
        {
            "object": "a",
            "status": "QUARANTINED_REVIEW_REQUIRED"
        }
    ])

    with pytest.raises(ValueError):
        safety.require_reconciled(
            safety.reconcile_journal(path, {"a"})
        )


def test_unapproved_journal_object(tmp_path):
    path = journal(tmp_path, [
        {"object": "b", "status": "PATCH_INTENT"}
    ])

    with pytest.raises(ValueError):
        safety.reconcile_journal(path, {"a"})


def test_ambiguous_patch_cannot_be_skipped(tmp_path):
    path = journal(tmp_path, [
        {"object": "a", "status": "PATCH_INTENT"},
        {
            "object": "a",
            "status": "ALREADY_COMPLIANT",
            "payload_verified": True
        }
    ])

    with pytest.raises(ValueError):
        safety.reconcile_journal(path, {"a"})


def test_duplicate_patch_intent_rejected(tmp_path):
    path = journal(tmp_path, [
        {"object": "a", "status": "PATCH_INTENT"},
        {"object": "a", "status": "PATCH_INTENT"}
    ])

    with pytest.raises(ValueError):
        safety.reconcile_journal(path, {"a"})


def test_corrupt_journal_rejected(tmp_path):
    path = tmp_path / "journal.jsonl"
    path.write_text('{"object":')

    with pytest.raises(ValueError):
        safety.reconcile_journal(path, {"a"})


def test_readonly_patch_prohibited():
    blob = safety.ReadOnlyBlob(object())

    with pytest.raises(RuntimeError):
        blob.patch()


@pytest.mark.parametrize("method", [
    "delete", "upload_from_string", "rewrite", "update"
])
def test_other_write_methods_prohibited(method):
    blob = safety.ReadOnlyBlob(object())

    with pytest.raises(AttributeError):
        getattr(blob, method)
