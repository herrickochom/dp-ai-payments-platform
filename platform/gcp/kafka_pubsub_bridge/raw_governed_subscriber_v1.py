"""Fail-closed governed Pub/Sub to immutable GCS Raw v1 adapter."""

import importlib.util
import io
import json
from pathlib import Path

from fastavro import reader


class SubscriberError(RuntimeError):
    pass


def load_encoder():
    path = Path(__file__).with_name("raw_governed_encoder_v1.py")
    spec = importlib.util.spec_from_file_location(
        "governed_encoder_for_subscriber", path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


encoder = load_encoder()


def process(data, ack_id, store, acker, approved_topics):
    """Store, verify, then acknowledge exactly one source envelope."""

    if not isinstance(data, bytes):
        raise SubscriberError("Pub/Sub data must be bytes")

    if not isinstance(approved_topics, (set, frozenset)):
        raise SubscriberError("explicit topic approval set required")

    try:
        envelope = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise SubscriberError("invalid Pub/Sub transport") from exc

    if not isinstance(envelope, dict):
        raise SubscriberError("invalid source envelope")

    topic = envelope.get("kafka_topic")

    if not isinstance(topic, str):
        raise SubscriberError("missing Kafka topic")

    # Check authorisation before any GCS operation.
    encoder.governance.require_live_authorisation(
        topic, approved_topics
    )

    name, body, expected = encoder.prepare(envelope)

    created = store.create_if_absent(name, body)

    if type(created) is not bool:
        raise SubscriberError("invalid storage result")

    stored = store.read(name)

    if not isinstance(stored, bytes):
        raise SubscriberError("GCS readback must return bytes")

    try:
        records = list(reader(io.BytesIO(stored)))
    except Exception as exc:
        raise SubscriberError("invalid stored Avro OCF") from exc

    if len(records) != 1 or records[0] != expected:
        raise SubscriberError(
            "immutable Kafka coordinate collision or readback mismatch"
        )

    # Never acknowledge before durable storage verification.
    acker.ack(ack_id)

    return {
        "name": name,
        "status": (
            "CREATED" if created
            else "ALREADY_PRESENT_IDENTICAL"
        ),
        "topic": topic,
        "partition": expected["kafka_partition"],
        "offset": expected["kafka_offset"],
    }
