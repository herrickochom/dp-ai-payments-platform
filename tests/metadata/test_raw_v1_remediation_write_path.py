
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
def approval(tmp_path):
    path = tmp_path / "approval.json"

    value = {
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

    path.write_text(json.dumps(value))
    return path, value


def test_missing_approval_rejected(tmp_path):
    with pytest.raises(ValueError, match="approval required"):
        safety.validate_approval(tmp_path / "missing.json")


def test_valid_approval(approval):
    path, value = approval
    assert safety.validate_approval(path) == value


@pytest.mark.parametrize("field,value", [
    ("approval_status", "PENDING"),
    ("approval_authority", "unknown"),
    ("project", "wrong"),
    ("bucket", "wrong"),
    ("metadata_contract_version", "2.0.0"),
    ("pilot_manifest_sha256", "wrong"),
    ("pilot_objects", 5267),
    ("live_write_authorised", False),
    ("conflict_migration_authorised", False),
    ("bulk_execution_authorised", True)
])
def test_invalid_approval(approval, field, value):
    path, data = approval
    data[field] = value
    path.write_text(json.dumps(data))

    with pytest.raises(ValueError):
        safety.validate_approval(path)


def test_legacy_batch_apply_blocked(tmp_path):
    bucket = Mock()

    with pytest.raises(ValueError, match="controlled safety"):
        executor.run_pilot(
            bucket,
            {"objects": []},
            Mock(),
            tmp_path / "journal.jsonl",
            apply=True
        )

    bucket.blob.assert_not_called()


def test_journal_identity_binding(tmp_path, approval):
    path, _ = approval
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    directory = tmp_path / "execution"

    first = safety.bind_journal(directory, manifest, path)
    second = safety.bind_journal(directory, manifest, path)

    assert first == second
    assert (directory / "execution-identity.json").exists()


def test_changed_approval_blocks_resume(tmp_path, approval):
    path, _ = approval
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    directory = tmp_path / "execution"

    safety.bind_journal(directory, manifest, path)
    path.write_text('{"changed":true}')

    with pytest.raises(ValueError, match="identity mismatch"):
        safety.bind_journal(directory, manifest, path)


def test_changed_manifest_blocks_resume(tmp_path, approval):
    path, _ = approval
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")
    directory = tmp_path / "execution"

    safety.bind_journal(directory, manifest, path)
    manifest.write_text('{"changed":true}')

    with pytest.raises(ValueError, match="identity mismatch"):
        safety.bind_journal(directory, manifest, path)


def test_unbound_journal_rejected(tmp_path, approval):
    path, _ = approval
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}")

    directory = tmp_path / "execution"
    directory.mkdir()
    (directory / "execution-journal.jsonl").touch()

    with pytest.raises(ValueError, match="Unbound"):
        safety.bind_journal(directory, manifest, path)


@pytest.mark.parametrize("completed,compliant,allowed", [
    ([], False, True),
    (["a"], True, True),
    ([], True, False),
    (["a"], False, False)
])
def test_resume_state(
    monkeypatch, completed, compliant, allowed
):
    monkeypatch.setattr(
        executor, "assess",
        lambda *a, **k: (None, None, None, None, compliant)
    )

    state = {
        "pending": [],
        "completed": completed,
        "quarantined": []
    }

    args = (
        Mock(), [{"object": "a"}], Mock(), state
    )

    if allowed:
        safety.validate_resume_state(*args)
    else:
        with pytest.raises(ValueError):
            safety.validate_resume_state(*args)


def test_unresolved_intent_blocks_live_resume():
    bucket = Mock()

    with pytest.raises(ValueError, match="Unresolved"):
        safety.validate_resume_state(
            bucket,
            [{"object": "a"}],
            Mock(),
            {
                "pending": ["a"],
                "completed": [],
                "quarantined": []
            }
        )

    bucket.blob.assert_not_called()


def test_durable_event(tmp_path):
    journal = tmp_path / "journal.jsonl"

    safety.durable_event(journal, {
        "object": "a",
        "status": "PATCH_INTENT"
    })

    event = json.loads(journal.read_text())

    assert event["object"] == "a"
    assert event["status"] == "PATCH_INTENT"
    assert "timestamp_utc" in event
