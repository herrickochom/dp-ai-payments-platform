"""Contract tests for payment source quarantine."""

import importlib.util
from pathlib import Path

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "services/payment-producer/source_quarantine.py"
)

spec = importlib.util.spec_from_file_location(
    "source_quarantine", MODULE_PATH
)
module = importlib.util.module_from_spec(spec)

import sys
sys.modules[spec.name] = module
spec.loader.exec_module(module)


def test_source_identity_is_deterministic(tmp_path):
    path = tmp_path / "pdmis" / "loans.json"
    path.parent.mkdir()
    path.write_bytes(b'{"loan_id":"L1"}')

    first = module.source_identity(
        str(path), str(tmp_path), path.read_bytes()
    )
    second = module.source_identity(
        str(path), str(tmp_path), path.read_bytes()
    )

    assert first == second
    assert len(first.source_id) == 64


def test_changed_content_changes_source_identity(tmp_path):
    path = tmp_path / "loans.json"

    first = module.source_identity(
        str(path), str(tmp_path), b"version-one"
    )
    second = module.source_identity(
        str(path), str(tmp_path), b"version-two"
    )

    assert first.source_id != second.source_id
    assert first.content_sha256 != second.content_sha256


def test_different_source_locations_have_distinct_ids(tmp_path):
    first = module.source_identity(
        str(tmp_path / "a.json"), str(tmp_path), b"same"
    )
    second = module.source_identity(
        str(tmp_path / "b.json"), str(tmp_path), b"same"
    )

    assert first.source_id != second.source_id
    assert first.content_sha256 == second.content_sha256


def test_source_outside_data_root_rejected(tmp_path):
    with pytest.raises(module.SourceQuarantineError):
        module.source_identity(
            str(tmp_path.parent / "outside.json"),
            str(tmp_path),
            b"content",
        )


def test_metadata_excludes_source_path_and_raw_content(tmp_path):
    identity = module.source_identity(
        str(tmp_path / "loans.json"),
        str(tmp_path),
        b"private-payment-data",
    )

    metadata = module.failure_metadata(identity, "INVALID_JSON")
    module.validate_failure_metadata(metadata)

    assert "source_path" not in metadata
    assert "raw_content" not in metadata
    assert "error_message" not in metadata
    assert "private-payment-data" not in str(metadata)


def test_unapproved_metadata_fields_rejected(tmp_path):
    identity = module.source_identity(
        str(tmp_path / "loans.json"), str(tmp_path), b"content"
    )
    metadata = module.failure_metadata(identity, "INVALID_RECORD")
    metadata["raw_content"] = "sensitive"

    with pytest.raises(module.SourceQuarantineError):
        module.validate_failure_metadata(metadata)


def test_unknown_failure_category_rejected(tmp_path):
    identity = module.source_identity(
        str(tmp_path / "loans.json"), str(tmp_path), b"content"
    )

    with pytest.raises(module.SourceQuarantineError):
        module.failure_metadata(identity, "ARBITRARY_EXCEPTION")


class FakeMissingObject(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakePreconditionFailed(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class FakeGCSClient:
    def __init__(self):
        self.objects = {}
        self.puts = 0
        self.fail_put = False
        self.corrupt_readback = False
        self.fail_read = False
        self.preconditions = []

    def bucket(self, name):
        assert name == "restricted"
        return self

    def blob(self, key):
        return FakeGCSBlob(self, key)


class FakeGCSBlob:
    def __init__(self, client, key):
        self.client = client
        self.key = key

    def upload_from_string(
        self, data, *, content_type, if_generation_match
    ):
        from google.api_core.exceptions import PreconditionFailed

        assert content_type == "application/json"
        assert if_generation_match == 0

        self.client.preconditions.append(if_generation_match)

        if self.client.fail_put:
            raise RuntimeError("simulated storage failure")

        if self.key in self.client.objects:
            raise PreconditionFailed("object already exists")

        self.client.objects[self.key] = data
        self.client.puts += 1

    def download_as_bytes(self):
        if self.client.fail_read:
            raise RuntimeError("simulated read failure")

        if self.client.corrupt_readback:
            return b"corrupted readback"

        return self.client.objects[self.key]


def quarantine_metadata(tmp_path):
    identity = module.source_identity(
        str(tmp_path / "loans.json"),
        str(tmp_path),
        b"private-source-content",
    )
    return module.failure_metadata(identity, "INVALID_JSON")


def test_gcs_quarantine_persists_and_verifies(tmp_path):
    client = FakeGCSClient()
    store = module.GCSSourceQuarantineStore(client, "restricted")

    key = store.persist(quarantine_metadata(tmp_path))

    assert key.startswith(store.PREFIX)
    assert client.puts == 1
    assert key in client.objects


def test_gcs_quarantine_replay_is_idempotent(tmp_path):
    client = FakeGCSClient()
    store = module.GCSSourceQuarantineStore(client, "restricted")
    metadata = quarantine_metadata(tmp_path)

    first = store.persist(metadata)
    second = store.persist(metadata)

    assert first == second
    assert client.puts == 1


def test_gcs_quarantine_rejects_conflicting_existing_object(tmp_path):
    client = FakeGCSClient()
    store = module.GCSSourceQuarantineStore(client, "restricted")
    metadata = quarantine_metadata(tmp_path)

    key = store.persist(metadata)
    client.objects[key] = b"conflicting"

    with pytest.raises(module.SourceQuarantinePersistenceError):
        store.persist(metadata)


def test_gcs_quarantine_storage_failure_is_fail_closed(tmp_path):
    client = FakeGCSClient()
    client.fail_put = True
    store = module.GCSSourceQuarantineStore(client, "restricted")

    with pytest.raises(module.SourceQuarantinePersistenceError):
        store.persist(quarantine_metadata(tmp_path))


def test_gcs_quarantine_readback_mismatch_is_fail_closed(tmp_path):
    client = FakeGCSClient()
    client.corrupt_readback = True
    store = module.GCSSourceQuarantineStore(client, "restricted")

    with pytest.raises(module.SourceQuarantinePersistenceError):
        store.persist(quarantine_metadata(tmp_path))


def test_gcs_quarantine_rejects_unapproved_metadata(tmp_path):
    client = FakeGCSClient()
    store = module.GCSSourceQuarantineStore(client, "restricted")
    metadata = quarantine_metadata(tmp_path)
    metadata["raw_content"] = "private"

    with pytest.raises(module.SourceQuarantineError):
        store.persist(metadata)

    assert client.puts == 0


def test_gcs_quarantine_conditional_creation_is_required(tmp_path):
    client = FakeGCSClient()
    store = module.GCSSourceQuarantineStore(client, "restricted")

    store.persist(quarantine_metadata(tmp_path))

    assert client.puts == 1
    assert client.preconditions == [0]


def test_gcs_quarantine_precondition_conflict_verifies_existing(tmp_path):
    client = FakeGCSClient()
    store = module.GCSSourceQuarantineStore(client, "restricted")
    metadata = quarantine_metadata(tmp_path)

    first = store.persist(metadata)
    second = store.persist(metadata)

    assert first == second
    assert client.puts == 1


def test_gcs_quarantine_read_failure_is_fail_closed(tmp_path):
    client = FakeGCSClient()
    client.fail_read = True

    store = module.GCSSourceQuarantineStore(
        client, "restricted"
    )

    with pytest.raises(module.SourceQuarantinePersistenceError):
        store.persist(quarantine_metadata(tmp_path))
