from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]

MODULE_PATH = (
    ROOT
    / "services"
    / "kafka-consumer-events"
    / "kafka_consumer_events.py"
)

spec = importlib.util.spec_from_file_location(
    "cdc_raw_consumer_under_test",
    MODULE_PATH,
)

module = importlib.util.module_from_spec(
    spec
)

assert spec.loader is not None
spec.loader.exec_module(module)


class FakeMessage:
    def __init__(
        self,
        *,
        topic="cdc.test.public.accounts",
        partition=2,
        offset=41,
        key=None,
        value=None,
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value


class FakeConsumer:
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


def debezium_value(op="c"):
    before = None
    after = {
        "id": 7,
        "status": "ACTIVE",
    }

    if op == "u":
        before = {
            "id": 7,
            "status": "PENDING",
        }

    if op == "d":
        before = {
            "id": 7,
            "status": "ACTIVE",
        }
        after = None

    return {
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "schema": "public",
                "table": "accounts",
                "lsn": 987654,
                "ts_ms": 1760000000000,
                "snapshot": "false",
            },
            "op": op,
            "ts_ms": 1760000000123,
            "transaction": {
                "id": "tx-9",
            },
        }
    }


def test_runtime_cdc_topics_remain_empty():
    assert module.activated_cdc_topic_map() == {}


def test_generated_subscription_remains_24_topics():
    assert len(module.Settings.KAFKA_TOPICS) == 24

    assert (
        module.consumer_topics()
        == module.Settings.KAFKA_TOPICS
    )


def test_generated_topic_classification_unchanged():
    topic = module.Settings.KAFKA_TOPICS[0]

    mode, source = (
        module.classify_ingestion_topic(
            topic,
            {},
        )
    )

    assert mode == "GENERATED_EVENT"
    assert source is None


def test_unknown_topic_fails_closed():
    with pytest.raises(
        module.PermanentProcessingError
    ):
        module.classify_ingestion_topic(
            "unknown.topic",
            {},
        )


def test_explicit_cdc_mapping_classifies_database_topic():
    mode, source = (
        module.classify_ingestion_topic(
            "cdc.test.public.accounts",
            {
                "cdc.test.public.accounts":
                "future_operational_source"
            },
        )
    )

    assert mode == "CDC_DATABASE"
    assert source == "future_operational_source"


def test_tombstone_is_processed_without_payment_deserialiser(
    monkeypatch,
):
    msg = FakeMessage(
        key=json.dumps(
            {"id": 7}
        ).encode(),
        value=None,
    )

    consumer = FakeConsumer()
    captured = {}

    def fake_store(
        record,
        **kwargs,
    ):
        captured["record"] = record
        return "raw/v2/cdc/tombstone.avro"

    monkeypatch.setattr(
        module,
        "store_cdc_event_to_s3",
        fake_store,
    )

    result = module.process_cdc_message(
        consumer,
        msg,
        source_system=(
            "future_operational_source"
        ),
        ingestion_timestamp=(
            "2026-09-18T12:00:00Z"
        ),
    )

    assert result == (
        "raw/v2/cdc/tombstone.avro"
    )

    record = captured["record"]

    assert record["record_type"] == (
        "CDC_TOMBSTONE"
    )
    assert record["tombstone"] is True

    assert consumer.commits == [
        (
            "cdc.test.public.accounts",
            2,
            41,
            False,
        )
    ]


def test_change_event_separates_database_and_kafka_positions(
    monkeypatch,
):
    msg = FakeMessage(
        partition=3,
        offset=55,
        key=json.dumps(
            {"id": 7}
        ).encode(),
        value=json.dumps(
            debezium_value("u")
        ).encode(),
    )

    consumer = FakeConsumer()
    captured = {}

    def fake_store(
        record,
        **kwargs,
    ):
        captured["record"] = record
        return "raw/v2/cdc/change.avro"

    monkeypatch.setattr(
        module,
        "store_cdc_event_to_s3",
        fake_store,
    )

    module.process_cdc_message(
        consumer,
        msg,
        source_system=(
            "future_operational_source"
        ),
        ingestion_timestamp=(
            "2026-09-18T12:00:00Z"
        ),
    )

    record = captured["record"]

    assert record["source_position"] == "987654"
    assert record["source_transaction"] == "tx-9"

    assert record["kafka_topic"] == (
        "cdc.test.public.accounts"
    )
    assert record["kafka_partition"] == 3
    assert record["kafka_offset"] == 55

    assert json.loads(
        record["before_json"]
    )["status"] == "PENDING"

    assert json.loads(
        record["after_json"]
    )["status"] == "ACTIVE"


def test_commit_occurs_only_after_durable_storage(
    monkeypatch,
):
    msg = FakeMessage(
        key=json.dumps(
            {"id": 7}
        ).encode(),
        value=json.dumps(
            debezium_value("c")
        ).encode(),
    )

    consumer = FakeConsumer()

    def fail_storage(*args, **kwargs):
        raise OSError(
            "simulated storage failure"
        )

    monkeypatch.setattr(
        module,
        "store_cdc_event_to_s3",
        fail_storage,
    )

    with pytest.raises(OSError):
        module.process_cdc_message(
            consumer,
            msg,
            source_system=(
                "future_operational_source"
            ),
            ingestion_timestamp=(
                "2026-09-18T12:00:00Z"
            ),
        )

    assert consumer.commits == []


def test_invalid_cdc_key_fails_closed():
    with pytest.raises(
        module.CDCEnvelopeError
    ):
        module.decode_cdc_key(
            b"not-json"
        )


def test_invalid_cdc_value_fails_closed():
    with pytest.raises(
        module.CDCEnvelopeError
    ):
        module.decode_cdc_value(
            b'["not","an","object"]'
        )


def test_cdc_avro_ocf_serialization():
    envelope = (
        module.normalize_debezium_event(
            source_system=(
                "future_operational_source"
            ),
            key={"id": 7},
            value=debezium_value("c"),
            ingestion_timestamp=(
                "2026-09-18T12:00:00Z"
            ),
        )
    )

    record = module.cdc_avro_record(
        envelope,
        topic="cdc.test.public.accounts",
        partition=1,
        offset=9,
    )

    payload = module.serialize_cdc_to_avro(
        record
    )

    assert payload[:4] == b"Obj\x01"


def test_cdc_processing_does_not_use_generated_failure_envelope():
    names = set(
        module.process_cdc_message.__code__.co_names
    )

    assert "failure_envelope" not in names
    assert "publish_failure" not in names
    assert "process_message" not in names
