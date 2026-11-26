import copy
import hashlib
import sys

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

sys.path.insert(0, str(ROOT / "scripts/metadata"))

import raw_v1_metadata_remediation as r


class Blob:
    def __init__(self, bucket):
        self.bucket = bucket

    def reload(self):
        self.state = copy.deepcopy(self.bucket.state)

    def patch(self, **kwargs):
        self.bucket.patch_calls += 1

        if kwargs["if_generation_match"] != self.bucket.state["generation"]:
            raise RuntimeError("Stale generation")

        if kwargs["if_metageneration_match"] != self.bucket.state["metageneration"]:
            raise RuntimeError("Stale metageneration")

        self.bucket.state["metadata"] = copy.deepcopy(self.metadata)
        self.bucket.state["cache_control"] = self.cache_control
        self.bucket.state["metageneration"] += 1

        if self.bucket.corrupt_after_patch:
            self.bucket.body = b"changed"


class Bucket:
    def __init__(self, state, body):
        self.state = copy.deepcopy(state)
        self.body = body
        self.patch_calls = 0
        self.corrupt_after_patch = False

    def blob(self, name):
        return Blob(self)


class Decoder:
    def __init__(self, expected, source_sha):
        self.expected = expected
        self.source_sha = source_sha

    def derive_metadata(self, name, body):
        return self.expected, {"payload_sha256": self.source_sha}


@pytest.fixture
def setup(monkeypatch):
    body = b"offline-payload-fixture"

    expected = {
        "data-domain": "agent_management",
        "data-layer": "raw",
        "storage-contract-version": "v1",
        "metadata-contract-version": "1.0.0",
        "processing-mode": "streaming",
        "category": "agent_network",
        "source-group": "agent",
        "source-system": "agent",
        "source-topic": "agent.locations",
        "source-partition": "0",
        "source-offset": "0"
    }

    state = {
        "generation": 100,
        "metageneration": 1,
        "crc32c": "fixture",
        "md5_hash": "fixture",
        "size": len(body),
        "content_type": "application/avro",
        "cache_control": None,
        "content_encoding": None,
        "content_disposition": None,
        "content_language": None,
        "custom_time": None,
        "metadata": {"additional": "preserve"}
    }

    proposed = dict(state["metadata"])
    proposed.update(expected)

    source_sha = hashlib.sha256(b"source").hexdigest()

    item = {
        "object": r.CONFLICT_OBJECT,
        "expected_generation": 100,
        "expected_metageneration": 1,
        "expected_size_bytes": len(body),
        "expected_raw_sha256": hashlib.sha256(body).hexdigest(),
        "expected_source_payload_sha256": source_sha,
        "expected_metadata": expected,
        "proposed_metadata": proposed,
        "before": copy.deepcopy(state),
        "remediation_type": "STANDARD_METADATA_REMEDIATION"
    }

    bucket = Bucket(state, body)
    decoder = Decoder(expected, source_sha)

    monkeypatch.setattr(r, "snapshot", lambda blob: copy.deepcopy(blob.state))
    monkeypatch.setattr(r, "read_payload", lambda blob, state: blob.bucket.body)

    return bucket, item, decoder


def test_dry_run_no_patch(setup):
    bucket, item, decoder = setup
    result = r.remediate(bucket, item, decoder)
    assert result["status"] == "DRY_RUN_PASS"
    assert bucket.patch_calls == 0


def test_apply_preserves_payload(setup):
    bucket, item, decoder = setup
    original = bucket.body
    result = r.remediate(bucket, item, decoder, apply=True)
    assert result["status"] == "UPDATED_VERIFIED"
    assert bucket.body == original
    assert bucket.state["generation"] == 100
    assert bucket.state["metageneration"] == 2


def test_additional_metadata_preserved(setup):
    bucket, item, decoder = setup
    r.remediate(bucket, item, decoder, apply=True)
    assert bucket.state["metadata"]["additional"] == "preserve"


def test_idempotent_restart(setup):
    bucket, item, decoder = setup
    r.remediate(bucket, item, decoder, apply=True)
    result = r.remediate(bucket, item, decoder, apply=True)
    assert result["status"] == "ALREADY_COMPLIANT"
    assert bucket.patch_calls == 1


@pytest.mark.parametrize("field,value", [
    ("generation", 101),
    ("metageneration", 2),
    ("size", 999),
    ("content_type", "application/json"),
    ("content_encoding", "gzip"),
    ("content_disposition", "attachment"),
    ("content_language", "en"),
    ("custom_time", "unexpected"),
    ("crc32c", "changed"),
    ("md5_hash", "changed")
])
def test_changed_baseline_rejected(setup, field, value):
    bucket, item, decoder = setup
    bucket.state[field] = value
    with pytest.raises(ValueError):
        r.remediate(bucket, item, decoder, apply=True)
    assert bucket.patch_calls == 0


def test_payload_change_rejected(setup):
    bucket, item, decoder = setup
    bucket.body = b"modified"
    with pytest.raises(ValueError):
        r.remediate(bucket, item, decoder, apply=True)
    assert bucket.patch_calls == 0


def test_source_digest_mismatch(setup):
    bucket, item, decoder = setup
    decoder.source_sha = "invalid"
    with pytest.raises(ValueError):
        r.remediate(bucket, item, decoder, apply=True)
    assert bucket.patch_calls == 0


def test_post_patch_corruption_detected(setup):
    bucket, item, decoder = setup
    bucket.corrupt_after_patch = True
    with pytest.raises(ValueError):
        r.remediate(bucket, item, decoder, apply=True)
    assert bucket.patch_calls == 1


def conflict_fixture(setup):
    bucket, item, decoder = setup
    bucket.state["metadata"]["data-domain"] = "payments"
    item["before"] = copy.deepcopy(bucket.state)
    item["remediation_type"] = "EXPLICIT_CONFLICT_MIGRATION"
    return bucket, item, decoder


def test_conflict_denied_by_default(setup):
    bucket, item, decoder = conflict_fixture(setup)
    with pytest.raises(ValueError):
        r.remediate(bucket, item, decoder, apply=True)
    assert bucket.patch_calls == 0


def test_exact_conflict_migration(setup):
    bucket, item, decoder = conflict_fixture(setup)
    result = r.remediate(
        bucket, item, decoder,
        apply=True, allow_conflict=True
    )
    assert result["status"] == "UPDATED_VERIFIED"
    assert bucket.state["metadata"]["data-domain"] == "agent_management"


def test_wrong_conflict_object_rejected(setup):
    bucket, item, decoder = conflict_fixture(setup)
    item["object"] = "different-object"
    with pytest.raises(ValueError):
        r.remediate(
            bucket, item, decoder,
            apply=True, allow_conflict=True
        )
    assert bucket.patch_calls == 0


def test_unexpected_conflict_value_rejected(setup):
    bucket, item, decoder = conflict_fixture(setup)
    bucket.state["metadata"]["data-domain"] = "unknown"
    item["before"] = copy.deepcopy(bucket.state)
    with pytest.raises(ValueError):
        r.remediate(
            bucket, item, decoder,
            apply=True, allow_conflict=True
        )
    assert bucket.patch_calls == 0


def test_unexpected_conflict_key_rejected(setup):
    bucket, item, decoder = conflict_fixture(setup)
    bucket.state["metadata"]["data-layer"] = "silver"
    item["before"] = copy.deepcopy(bucket.state)
    with pytest.raises(ValueError):
        r.remediate(
            bucket, item, decoder,
            apply=True, allow_conflict=True
        )
    assert bucket.patch_calls == 0


def test_changed_additional_metadata_rejected(setup):
    bucket, item, decoder = setup
    bucket.state["metadata"]["additional"] = "changed"
    with pytest.raises(ValueError):
        r.remediate(bucket, item, decoder, apply=True)
    assert bucket.patch_calls == 0


def test_tampered_manifest_rejected(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text("{}")
    with pytest.raises(ValueError, match="checksum"):
        r.load_manifest(path)


@pytest.mark.parametrize("count", [0, 1, 9, 11, 5267])
def test_nonpilot_scope_rejected(setup, tmp_path, count):
    bucket, item, decoder = setup
    with pytest.raises(ValueError):
        r.run_pilot(
            bucket,
            {"objects": [item] * count},
            decoder,
            tmp_path / "journal.jsonl",
            apply=True
        )
    assert bucket.patch_calls == 0


def test_durable_patch_intent(setup, tmp_path):
    bucket, item, decoder = setup
    journal = tmp_path / "journal.jsonl"

    r.remediate(
        bucket, item, decoder, apply=True,
        record=lambda event: r.append_event(journal, event)
    )

    assert '"status": "PATCH_INTENT"' in journal.read_text()


def test_unexpected_compliant_metageneration_rejected(setup):
    bucket, item, decoder = setup
    r.remediate(bucket, item, decoder, apply=True)
    bucket.state["metageneration"] = 9

    with pytest.raises(ValueError):
        r.remediate(bucket, item, decoder, apply=True)

    assert bucket.patch_calls == 1
