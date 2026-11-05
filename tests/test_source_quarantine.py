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


class FakeS3Client:
    def __init__(self):
        self.objects = {}
        self.puts = 0
        self.fail_put = False
        self.corrupt_readback = False

    def get_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise FakeMissingObject()
        payload = self.objects[Key]
        if self.corrupt_readback:
            payload = b"corrupt"

        class Body:
            def read(self):
                return payload

        return {"Body": Body()}

    def put_object(self, *, Bucket, Key, Body, ContentType, IfNoneMatch):
        assert IfNoneMatch == "*"
        if self.fail_put:
            raise RuntimeError("simulated storage failure")
        if Key in self.objects:
            raise FakePreconditionFailed()
        self.puts += 1
        self.objects[Key] = Body


def quarantine_metadata(tmp_path):
    identity = module.source_identity(
        str(tmp_path / "loans.json"),
        str(tmp_path),
        b"private-source-content",
    )
    return module.failure_metadata(identity, "INVALID_JSON")


def test_s3_quarantine_persists_and_verifies(tmp_path):
    client = FakeS3Client()
    store = module.S3SourceQuarantineStore(client, "restricted")

    key = store.persist(quarantine_metadata(tmp_path))

    assert key.startswith(store.PREFIX)
    assert client.puts == 1
    assert key in client.objects


def test_s3_quarantine_replay_is_idempotent(tmp_path):
    client = FakeS3Client()
    store = module.S3SourceQuarantineStore(client, "restricted")
    metadata = quarantine_metadata(tmp_path)

    first = store.persist(metadata)
    second = store.persist(metadata)

    assert first == second
    assert client.puts == 1


def test_s3_quarantine_rejects_conflicting_existing_object(tmp_path):
    client = FakeS3Client()
    store = module.S3SourceQuarantineStore(client, "restricted")
    metadata = quarantine_metadata(tmp_path)

    key = store.persist(metadata)
    client.objects[key] = b"conflicting"

    with pytest.raises(module.SourceQuarantinePersistenceError):
        store.persist(metadata)


def test_s3_quarantine_storage_failure_is_fail_closed(tmp_path):
    client = FakeS3Client()
    client.fail_put = True
    store = module.S3SourceQuarantineStore(client, "restricted")

    with pytest.raises(module.SourceQuarantinePersistenceError):
        store.persist(quarantine_metadata(tmp_path))


def test_s3_quarantine_readback_mismatch_is_fail_closed(tmp_path):
    client = FakeS3Client()
    client.corrupt_readback = True
    store = module.S3SourceQuarantineStore(client, "restricted")

    with pytest.raises(module.SourceQuarantinePersistenceError):
        store.persist(quarantine_metadata(tmp_path))


def test_s3_quarantine_rejects_unapproved_metadata(tmp_path):
    client = FakeS3Client()
    store = module.S3SourceQuarantineStore(client, "restricted")
    metadata = quarantine_metadata(tmp_path)
    metadata["raw_content"] = "private"

    with pytest.raises(module.SourceQuarantineError):
        store.persist(metadata)

    assert client.puts == 0


def test_s3_quarantine_conditional_creation_is_required(tmp_path):
    client = FakeS3Client()
    store = module.S3SourceQuarantineStore(client, "restricted")

    store.persist(quarantine_metadata(tmp_path))

    assert client.puts == 1


def test_s3_quarantine_precondition_conflict_verifies_existing(tmp_path):
    client = FakeS3Client()
    store = module.S3SourceQuarantineStore(client, "restricted")
    metadata = quarantine_metadata(tmp_path)

    first = store.persist(metadata)
    second = store.persist(metadata)

    assert first == second
    assert client.puts == 1
