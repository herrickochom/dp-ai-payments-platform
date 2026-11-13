"""Offline adapter for non-legacy, ordinary source envelopes.

No Kafka commits, Pub/Sub publish, GCS writes or cloud access.
The existing PaymentEvent pipeline is intentionally untouched.
"""
from __future__ import annotations
import base64
import hashlib
import importlib.util
import json
from pathlib import Path

SOURCE = Path(__file__).with_name("source_envelope_v1.py")
spec = importlib.util.spec_from_file_location("dp_source_envelope_v1", SOURCE)
contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract)

class AdapterError(ValueError):
    pass

def make_envelope(topic: str, partition: int, offset: int, timestamp_ms: int,
                  payload: bytes | None, schema_registry_id: int | None = None) -> dict:
    if not isinstance(topic, str) or any(type(x) is not int or x < 0 for x in (partition, offset, timestamp_ms)):
        raise AdapterError("invalid Kafka identity")
    if payload is not None and not isinstance(payload, bytes):
        raise AdapterError("payload must be bytes or None")
    raw = b"" if payload is None else payload
    envelope = {
        "message_type": contract.SOURCE_MESSAGE_TYPE,
        "kafka_topic": topic, "kafka_partition": partition,
        "kafka_offset": offset, "kafka_timestamp_ms": timestamp_ms,
        "payload_base64": base64.b64encode(raw).decode("ascii"),
        "payload_sha256": hashlib.sha256(raw).hexdigest(),
        "schema_registry_id": schema_registry_id,
    }
    if topic not in LEGACY_TOPICS:
        envelope["value_is_null"] = payload is None
    validated = contract.validate_envelope(envelope)
    if validated["payload_format"] == "tombstone" and payload is not None:
        raise AdapterError("CDC tombstone must be Kafka null, not empty bytes")
    return envelope


def _canonical_record(envelope: dict) -> dict:
    import base64
    import hashlib

    validated = contract.validate_envelope(envelope)
    topic = validated["topic"]

    if topic in LEGACY_TOPICS:
        raise AdapterError("legacy writer owns this topic")

    route = contract.route_for(topic)
    payload = base64.b64decode(envelope["payload_base64"], validate=True)

    is_tombstone = validated["payload_format"] == "tombstone"

    if is_tombstone and payload:
        raise AdapterError("CDC tombstone must have empty payload")

    return {
        "kafka_topic": topic,
        "kafka_partition": validated["partition"],
        "kafka_offset": validated["offset"],
        "kafka_timestamp_ms": validated["timestamp_ms"],
        "payload_bytes": payload,
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "schema_registry_id": envelope.get("schema_registry_id"),
        "payload_format": validated["payload_format"],
        "processing_mode": route["processing_mode"],
        "value_is_null": validated["value_is_null"],
        "is_tombstone": is_tombstone,
    }


AVRO_SCHEMA = {
    "type": "record",
    "name": "CanonicalSourceEnvelopeV1",
    "namespace": "dp.payments.raw",
    "fields": [
        {"name": "kafka_topic", "type": "string"},
        {"name": "kafka_partition", "type": "long"},
        {"name": "kafka_offset", "type": "long"},
        {"name": "kafka_timestamp_ms", "type": "long"},
        {"name": "payload_bytes", "type": "bytes"},
        {"name": "payload_sha256", "type": "string"},
        {"name": "schema_registry_id", "type": ["null", "long"], "default": None},
        {"name": "payload_format", "type": "string"},
        {"name": "processing_mode", "type": "string"},
        {"name": "value_is_null", "type": "boolean"},
        {"name": "is_tombstone", "type": "boolean"},
    ],
}


def immutable_key(envelope: dict) -> str:
    validated = contract.validate_envelope(envelope)
    topic = validated["topic"]

    if topic in LEGACY_TOPICS:
        raise AdapterError("legacy writer owns this topic")

    route = contract.route_for(topic)
    mode = route["processing_mode"]

    classification = {}
    for field in ("category", "source_group", "source_system"):
        value = route.get(field)
        if not isinstance(value, str) or not value:
            raise AdapterError(f"missing governed {field}")
        if not value.isascii() or not all(
            c.islower() or c.isdigit() or c == "_" for c in value
        ) or not value[0].islower():
            raise AdapterError(f"invalid governed {field}")
        classification[field] = value

    if not topic or not all(c.isalnum() or c in "._-" for c in topic):
        raise AdapterError("invalid topic path")

    from datetime import datetime, timezone

    dt = datetime.fromtimestamp(
        validated["timestamp_ms"] / 1000,
        timezone.utc,
    )

    return (
        f"raw/v1/{mode}/"
        f"category={classification['category']}/"
        f"source_group={classification['source_group']}/"
        f"source_system={classification['source_system']}/"
        f"year={dt:%Y}/month={dt:%m}/day={dt:%d}/"
        f"topic={topic}/"
        f"partition={validated['partition']}/"
        f"offset={validated['offset']}/record.avro"
    )


LEGACY_TOPICS = frozenset(
    topic for topic, route in contract.ROUTES.items()
    if route["status"] == "legacy_payment_event"
)


def prepare_object(envelope: dict) -> tuple[str, bytes]:
    """Produce deterministic Avro OCF for immutable GCS storage."""
    import io
    import hashlib
    from fastavro import writer, parse_schema

    record = _canonical_record(envelope)
    key = immutable_key(envelope)

    identity = (
        f"{record['kafka_topic']}|"
        f"{record['kafka_partition']}|"
        f"{record['kafka_offset']}|"
        f"{record['payload_sha256']}"
    ).encode("utf-8")

    marker = hashlib.sha256(identity).digest()[:16]

    output = io.BytesIO()

    writer(
        output,
        parse_schema(AVRO_SCHEMA),
        [record],
        codec="null",
        sync_marker=marker,
        strict=True,
    )

    body = output.getvalue()

    if not body.startswith(b"Obj\x01"):
        raise AdapterError("invalid Avro OCF")

    return key, body
