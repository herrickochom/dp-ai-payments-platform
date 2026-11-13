"""Offline 38-topic source routing and lossless envelope validation.

This module does not publish, acknowledge, or access cloud resources.
"""
from __future__ import annotations
import base64
import binascii
import hashlib
import json
from pathlib import Path

ROUTES = json.loads((Path(__file__).with_name("topic_routes_v1.json")).read_text(encoding="utf-8"))["routes"]

class RouteRejected(ValueError):
    pass

def route_for(topic: str, *, ordinary: bool = True) -> dict:
    route = ROUTES.get(topic)
    if route is None:
        raise RouteRejected("unknown topic")
    if ordinary and route["status"].startswith("blocked"):
        raise RouteRejected("topic blocked from ordinary ingestion")
    return dict(route)


SOURCE_MESSAGE_TYPE = "payments.source-event.v1"

def validate_envelope(envelope: dict) -> dict:
    if not isinstance(envelope, dict):
        raise RouteRejected("envelope must be a mapping")
    required = {"kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp_ms", "payload_base64", "payload_sha256"}
    if not required.issubset(envelope):
        raise RouteRejected("missing required source metadata")
    if envelope.get("message_type") != SOURCE_MESSAGE_TYPE:
        raise RouteRejected("invalid or missing source message type")
    topic = envelope["kafka_topic"]
    route = route_for(topic)
    if route["status"] != "legacy_payment_event":
        if type(envelope.get("value_is_null")) is not bool:
            raise RouteRejected("value_is_null must be an explicit boolean")
    part, offset, ts = (envelope[k] for k in ("kafka_partition", "kafka_offset", "kafka_timestamp_ms"))
    if any(type(v) is not int or v < 0 for v in (part, offset, ts)):
        raise RouteRejected("invalid Kafka coordinates")
    encoded = envelope["payload_base64"]
    if not isinstance(encoded, str):
        raise RouteRejected("payload_base64 must be a string")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise RouteRejected("invalid base64") from exc
    if hashlib.sha256(payload).hexdigest() != envelope["payload_sha256"]:
        raise RouteRejected("payload digest mismatch")
    fmt = route["format"]
    if fmt == "unverified":
        raise RouteRejected("payload encoding not verified")
    value_is_null = envelope.get("value_is_null", False)
    if value_is_null:
        if payload:
            raise RouteRejected("Kafka null cannot contain payload bytes")
        if fmt != "json" or topic != "cdc.pdm_mdm.mdm.sacco_master_sources":
            raise RouteRejected("unexpected Kafka tombstone")
        kind = "tombstone"
    elif not payload:
        raise RouteRejected("empty non-null Kafka payload")
    elif fmt == "confluent_avro":
        if len(payload) < 6 or payload[0] != 0:
            raise RouteRejected("invalid Confluent Avro framing")
        sid = int.from_bytes(payload[1:5], "big")
        if sid < 1 or envelope.get("schema_registry_id") != sid:
            raise RouteRejected("schema registry ID mismatch")
        kind = "confluent_avro"
    else:
        try:
            json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise RouteRejected("invalid JSON payload") from exc
        kind = "json"
    return {"topic": topic, "partition": part, "offset": offset,
            "timestamp_ms": ts, "processing_mode": route["processing_mode"],
            "payload_format": kind, "payload_sha256": envelope["payload_sha256"],
            "value_is_null": value_is_null,
            "schema_registry_id": envelope.get("schema_registry_id") if kind == "confluent_avro" else None}
