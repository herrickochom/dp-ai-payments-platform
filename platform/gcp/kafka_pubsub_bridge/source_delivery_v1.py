"""Offline-tested callback-based delivery primitives for non-legacy ordinary topics.

Not connected to a live bridge or subscriber. Caller supplies publisher/storage
callbacks and owns consumer lifecycle, retries, leases and credentials.
"""
from __future__ import annotations
import hashlib
import importlib.util
from pathlib import Path

p=Path(__file__).with_name("source_runtime_adapter_v1.py")
spec=importlib.util.spec_from_file_location("dp_stage2_adapter",p)
adapter=importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)

class DeliveryError(RuntimeError): pass


APPROVED_SOURCE_TOPICS = frozenset({
    "mdm.sacco.golden",
    "mdm.agent.golden",
    "mdm.geography.golden",
})

def publish_then_commit(envelope, publish, commit):
    """Commit Kafka only after publisher confirms durable acceptance.

    publish(envelope) must block until Pub/Sub publish future resolves and
    return a nonempty message ID. No commit on failures.
    """
    adapter.contract.validate_envelope(envelope)
    if envelope["kafka_topic"] in adapter.LEGACY_TOPICS:
        raise DeliveryError("legacy bridge owns topic")
    topic = envelope["kafka_topic"]
    route = adapter.contract.route_for(topic)
    if (
        route["status"] != "contract_only"
        or topic not in APPROVED_SOURCE_TOPICS
    ):
        raise DeliveryError("source publishing not explicitly authorised")
    message_id=publish(envelope)
    if not isinstance(message_id,str) or not message_id:
        raise DeliveryError("publisher did not confirm message ID")
    commit()
    return message_id


def store_then_ack(envelope, create_if_absent, readback, ack):
    """Ack Pub/Sub only after immutable GCS create or byte-identical readback.

    create_if_absent(key, body) returns True if created, False if already exists.
    readback(key) returns exact stored bytes. Conflicting object fails closed.
    """
    key, body=adapter.prepare_object(envelope)
    created=create_if_absent(key,body)
    if type(created) is not bool:
        raise DeliveryError("storage callback must return bool")
    stored=readback(key)
    if not isinstance(stored,bytes) or not __import__("hmac").compare_digest(
        hashlib.sha256(stored).digest(),hashlib.sha256(body).digest()
    ) or stored!=body:
        raise DeliveryError("GCS readback mismatch or conflicting object")
    ack()
    return {"key":key,"created":created,"sha256":hashlib.sha256(body).hexdigest()}
