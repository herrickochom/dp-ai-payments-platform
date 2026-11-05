"""KP-001W offline-tested durable-write-before-ack core; no cloud execution entrypoint."""
import base64
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Protocol


class ConflictError(Exception):
    """Same Kafka identity, different immutable payload."""


class InvalidEnvelope(Exception):
    """Reject invalid or untrusted provenance."""


class ObjectStore(Protocol):
    def create_if_absent(self, name: str, data: bytes) -> bool: ...
    def read(self, name: str) -> bytes: ...


class Acknowledger(Protocol):
    def ack(self, ack_id: str) -> None: ...


@dataclass(frozen=True)
class Result:
    object_name: str
    state: str
    sha256: str


def validate(data: bytes) -> str:
    try:
        obj = json.loads(data)
    except (UnicodeError, ValueError) as exc:
        raise InvalidEnvelope('invalid JSON') from exc
    if not isinstance(obj, dict):
        raise InvalidEnvelope('JSON object required')
    topic = obj.get('kafka_topic')
    partition = obj.get('kafka_partition')
    offset = obj.get('kafka_offset')
    schema_id = obj.get('schema_registry_id')
    if not isinstance(topic, str) or not re.fullmatch(r'[A-Za-z0-9._-]{1,249}', topic):
        raise InvalidEnvelope('invalid topic')
    if type(partition) is not int or partition < 0 or type(offset) is not int or offset < 0:
        raise InvalidEnvelope('invalid Kafka identity')
    if type(schema_id) is not int or schema_id < 1:
        raise InvalidEnvelope('invalid schema ID')
    fields = [k for k in ('payload_base64','value_base64','kafka_value_base64','avro_payload_base64') if k in obj]
    if len(fields) != 1:
        raise InvalidEnvelope('exactly one Avro payload field required')
    try:
        wire = base64.b64decode(obj[fields[0]], validate=True)
    except (ValueError, TypeError) as exc:
        raise InvalidEnvelope('invalid base64') from exc
    if len(wire) <= 5 or wire[0] != 0 or int.from_bytes(wire[1:5], 'big') != schema_id:
        raise InvalidEnvelope('Confluent wire schema mismatch')
    if hashlib.sha256(wire).hexdigest() != obj.get('payload_sha256'):
        raise InvalidEnvelope('payload SHA-256 mismatch')
    return f'raw/native/v1/{topic}/partition={partition:02d}/offset={offset:020d}.json'


def process(data: bytes, ack_id: str, store: ObjectStore, acker: Acknowledger) -> Result:
    """Ack only after create-or-identical and exact readback. Errors propagate without ack."""
    name = validate(data)
    created = store.create_if_absent(name, data)
    stored = store.read(name)
    if stored != data:
        raise ConflictError('immutable Kafka identity has conflicting bytes or failed readback')
    digest = hashlib.sha256(stored).hexdigest()
    acker.ack(ack_id)
    return Result(name, 'CREATED' if created else 'ALREADY_PRESENT_IDENTICAL', digest)
