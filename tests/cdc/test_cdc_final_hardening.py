import importlib.util
import io
import json
from pathlib import Path

import pytest
from botocore.exceptions import ClientError


ROOT = Path(__file__).resolve().parents[2]

CONSUMER = (
    ROOT
    / "services"
    / "kafka-consumer-events"
    / "kafka_consumer_events.py"
)


def load_consumer():
    spec = importlib.util.spec_from_file_location(
        "cdc_final_hardening_consumer",
        CONSUMER,
    )

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


class Message:
    def __init__(
        self,
        *,
        key=b'{"id":1}',
        value=b'not-json',
        topic="cdc.future.public.account",
        partition=2,
        offset=41,
    ):
        self._key = key
        self._value = value
        self._topic = topic
        self._partition = partition
        self._offset = offset

    def key(self):
        return self._key

    def value(self):
        return self._value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset


class Consumer:
    def __init__(self):
        self.commits = []

    def commit(
        self,
        msg,
        asynchronous=False,
    ):
        self.commits.append(
            (
                msg.topic(),
                msg.partition(),
                msg.offset(),
                asynchronous,
            )
        )


def not_found():
    return ClientError(
        {
            "Error": {
                "Code": "404",
                "Message": "Not Found",
            }
        },
        "HeadObject",
    )


class ExistingObjectClient:
    def __init__(self, payload):
        self.payload = payload
        self.puts = []

    def head_object(self, **kwargs):
        return {
            "ContentLength": len(self.payload)
        }

    def get_object(self, **kwargs):
        return {
            "Body": io.BytesIO(self.payload)
        }

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


class MissingObjectClient:
    def __init__(self):
        self.puts = []

    def head_object(self, **kwargs):
        raise not_found()

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


class FailingObjectClient:
    def head_object(self, **kwargs):
        raise not_found()

    def put_object(self, **kwargs):
        raise OSError("storage unavailable")


def valid_record(module):
    env = module.normalize_debezium_event(
        source_system="future_source",
        key={"id": 1},
        value={
            "payload": {
                "before": None,
                "after": {
                    "id": 1,
                    "status": "ACTIVE",
                },
                "source": {
                    "schema": "public",
                    "table": "account",
                    "lsn": 100,
                    "ts_ms": 1760000000000,
                },
                "op": "c",
                "ts_ms": 1760000000123,
            }
        },
        ingestion_timestamp=(
            "2026-09-18T12:00:00Z"
        ),
    )

    return module.cdc_avro_record(
        env,
        topic="cdc.future.public.account",
        partition=2,
        offset=41,
    )


def test_existing_raw_object_requires_matching_content(
    monkeypatch,
):
    module = load_consumer()
    record = valid_record(module)

    # Deliberately serialise independently. Avro OCF container
    # bytes may differ while representing the same logical datum.
    existing_payload = module.serialize_cdc_to_avro(
        record
    )

    candidate_payload = module.serialize_cdc_to_avro(
        record
    )

    assert (
        module.deserialize_single_cdc_avro_record(
            existing_payload
        )
        == module.deserialize_single_cdc_avro_record(
            candidate_payload
        )
    )

    client = ExistingObjectClient(
        existing_payload
    )

    monkeypatch.setattr(
        module,
        "get_minio_client",
        lambda: client,
    )

    key = module.store_cdc_event_to_s3(
        record,
        source_system="future_source",
        topic="cdc.future.public.account",
        partition=2,
        offset=41,
        timestamp=module.datetime.fromisoformat(
            "2026-09-18T12:00:00+00:00"
        ),
    )

    assert key.endswith("/record.avro")
    assert client.puts == []


def test_existing_raw_object_collision_fails_closed(
    monkeypatch,
):
    module = load_consumer()
    record = valid_record(module)

    client = ExistingObjectClient(
        b"different-existing-object"
    )

    monkeypatch.setattr(
        module,
        "get_minio_client",
        lambda: client,
    )

    with pytest.raises(
        module.CDCRawCollisionError
    ):
        module.store_cdc_event_to_s3(
            record,
            source_system="future_source",
            topic="cdc.future.public.account",
            partition=2,
            offset=41,
            timestamp=module.datetime.fromisoformat(
                "2026-09-18T12:00:00+00:00"
            ),
        )

    assert client.puts == []


def test_permanent_bad_record_quarantines_then_commits(
    monkeypatch,
):
    module = load_consumer()

    consumer = Consumer()
    msg = Message()

    stored = []

    def fake_store(
        metadata,
        *,
        timestamp,
    ):
        stored.append(
            dict(metadata)
        )
        return (
            "restricted/cdc-quarantine/"
            "metadata.json"
        )

    monkeypatch.setattr(
        module,
        "store_cdc_quarantine_metadata",
        fake_store,
    )

    outcome = module.process_cdc_message(
        consumer,
        msg,
        source_system="future_source",
        ingestion_timestamp=(
            "2026-09-18T12:00:00Z"
        ),
    )

    assert outcome == "cdc_quarantine"

    assert len(stored) == 1

    rendered = json.dumps(stored[0])

    assert "not-json" not in rendered
    assert '{"id":1}' not in rendered

    assert set(stored[0]) == {
        "failure_id",
        "source_system",
        "topic",
        "partition",
        "offset",
        "failure_category",
        "error_class",
        "event_timestamp",
    }

    assert consumer.commits == [
        (
            "cdc.future.public.account",
            2,
            41,
            False,
        )
    ]


def test_quarantine_failure_never_commits(
    monkeypatch,
):
    module = load_consumer()

    consumer = Consumer()
    msg = Message()

    def fail_store(*args, **kwargs):
        raise module.CDCQuarantineWriteFailedError(
            "quarantine unavailable"
        )

    monkeypatch.setattr(
        module,
        "store_cdc_quarantine_metadata",
        fail_store,
    )

    with pytest.raises(
        module.CDCQuarantineWriteFailedError
    ):
        module.process_cdc_message(
            consumer,
            msg,
            source_system="future_source",
            ingestion_timestamp=(
                "2026-09-18T12:00:00Z"
            ),
        )

    assert consumer.commits == []


def test_transient_raw_storage_failure_never_quarantines(
    monkeypatch,
):
    module = load_consumer()

    consumer = Consumer()

    value = json.dumps(
        {
            "payload": {
                "before": None,
                "after": {
                    "id": 1
                },
                "source": {
                    "schema": "public",
                    "table": "account",
                    "lsn": 100,
                    "ts_ms": 1760000000000,
                },
                "op": "c",
                "ts_ms": 1760000000123,
            }
        }
    ).encode()

    msg = Message(
        value=value
    )

    quarantines = []

    monkeypatch.setattr(
        module,
        "store_cdc_event_to_s3",
        lambda *args, **kwargs: (
            (_ for _ in ()).throw(
                OSError("temporary object-store failure")
            )
        ),
    )

    monkeypatch.setattr(
        module,
        "store_cdc_quarantine_metadata",
        lambda *args, **kwargs: quarantines.append(
            kwargs
        ),
    )

    with pytest.raises(OSError):
        module.process_cdc_message(
            consumer,
            msg,
            source_system="future_source",
            ingestion_timestamp=(
                "2026-09-18T12:00:00Z"
            ),
        )

    assert quarantines == []
    assert consumer.commits == []


def test_raw_collision_never_quarantines_or_commits(
    monkeypatch,
):
    module = load_consumer()

    consumer = Consumer()

    value = json.dumps(
        {
            "payload": {
                "before": None,
                "after": {
                    "id": 1
                },
                "source": {
                    "schema": "public",
                    "table": "account",
                    "lsn": 100,
                    "ts_ms": 1760000000000,
                },
                "op": "c",
                "ts_ms": 1760000000123,
            }
        }
    ).encode()

    msg = Message(
        value=value
    )

    quarantines = []

    def collision(*args, **kwargs):
        raise module.CDCRawCollisionError(
            "collision"
        )

    monkeypatch.setattr(
        module,
        "store_cdc_event_to_s3",
        collision,
    )

    monkeypatch.setattr(
        module,
        "store_cdc_quarantine_metadata",
        lambda *args, **kwargs: quarantines.append(
            kwargs
        ),
    )

    with pytest.raises(
        module.CDCRawCollisionError
    ):
        module.process_cdc_message(
            consumer,
            msg,
            source_system="future_source",
            ingestion_timestamp=(
                "2026-09-18T12:00:00Z"
            ),
        )

    assert quarantines == []
    assert consumer.commits == []


def test_quarantine_object_contains_safe_metadata_only(
    monkeypatch,
):
    module = load_consumer()

    client = MissingObjectClient()

    monkeypatch.setattr(
        module,
        "get_minio_client",
        lambda: client,
    )

    metadata = module.cdc_failure_metadata(
        source_system="future_source",
        topic="cdc.future.public.account",
        partition=2,
        offset=41,
        failure_category="CDC_PERMANENT_RECORD_FAILURE",
        error=ValueError(
            "secret-value-must-not-leak"
        ),
        event_timestamp=(
            "2026-09-18T12:00:00Z"
        ),
    )

    key = module.store_cdc_quarantine_metadata(
        metadata,
        timestamp=module.datetime.fromisoformat(
            "2026-09-18T12:00:00+00:00"
        ),
    )

    assert key.startswith(
        "restricted/cdc-quarantine/v1/"
    )

    assert len(client.puts) == 1

    payload = client.puts[0]["Body"].decode(
        "utf-8"
    )

    assert "secret-value-must-not-leak" not in payload
    assert "record_key" not in payload
    assert "before_json" not in payload
    assert "after_json" not in payload
    assert "original_payload" not in payload


def test_quarantine_storage_failure_is_fail_stop(
    monkeypatch,
):
    module = load_consumer()

    monkeypatch.setattr(
        module,
        "get_minio_client",
        lambda: FailingObjectClient(),
    )

    metadata = module.cdc_failure_metadata(
        source_system="future_source",
        topic="cdc.future.public.account",
        partition=2,
        offset=41,
        failure_category="CDC_PERMANENT_RECORD_FAILURE",
        error=ValueError("bad event"),
        event_timestamp=(
            "2026-09-18T12:00:00Z"
        ),
    )

    with pytest.raises(
        module.CDCQuarantineWriteFailedError
    ):
        module.store_cdc_quarantine_metadata(
            metadata,
            timestamp=module.datetime.fromisoformat(
                "2026-09-18T12:00:00+00:00"
            ),
        )


def test_cdc_branch_still_does_not_use_generated_dlq():
    module = load_consumer()

    names = set(
        module.process_cdc_message.__code__.co_names
    )

    assert "failure_envelope" not in names
    assert "publish_failure" not in names
    assert "publish_dlq_then_commit" not in names
    assert "KAFKA_DLQ_TOPIC" not in names
