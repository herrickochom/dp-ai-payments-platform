
import hashlib
import io
import sys
from pathlib import Path

import pytest
from fastavro import writer

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/metadata"))

from raw_v1_metadata_contract import MetadataContract
from raw_v1_avro_decoder import RawAvroDecoder

contract = MetadataContract(ROOT)
decoder = RawAvroDecoder(contract, ROOT)

PAYLOAD = b"\x00\x00\x00\x00\x01example"

OBJECT = (
    "raw/v1/streaming/category=agent_network/"
    "source_group=agent/source_system=agent/"
    "year=1970/month=01/day=01/"
    "topic=agent.locations/partition=0/offset=0/record.avro"
)

NAMES = ["RawSourceEvent", "CanonicalSourceEnvelopeV1"]


def record(name):
    result = {
        "kafka_topic": "agent.locations",
        "kafka_partition": 0,
        "kafka_offset": 0,
        "kafka_timestamp_ms": 0,
        "schema_registry_id": 1,
        "payload_sha256": hashlib.sha256(PAYLOAD).hexdigest()
    }

    if name == "RawSourceEvent":
        result["payload_wire"] = PAYLOAD
    else:
        result.update(
            payload_bytes=PAYLOAD,
            payload_format="confluent_avro",
            processing_mode="streaming",
            value_is_null=False,
            is_tombstone=False
        )

    return result


def encode(name, records):
    stream = io.BytesIO()
    writer(stream, decoder.schemas[name], records)
    return stream.getvalue()


@pytest.mark.parametrize("name", NAMES)
def test_valid_envelope(name):
    raw = encode(name, [record(name)])
    metadata, decoded = decoder.derive_metadata(OBJECT, raw)

    assert decoded["payload_bytes"] == PAYLOAD
    assert decoded["envelope_name"] == name
    assert decoded["raw_object_sha256"] == hashlib.sha256(raw).hexdigest()
    assert len(metadata) == 11
    assert metadata["data-domain"] == "agent_management"
    assert metadata["metadata-contract-version"] == "1.0.0"


@pytest.mark.parametrize("name", NAMES)
def test_digest_mismatch(name):
    value = record(name)
    value["payload_sha256"] = "0" * 64

    with pytest.raises(ValueError):
        decoder.decode(encode(name, [value]))


@pytest.mark.parametrize("name", NAMES)
def test_schema_id_mismatch(name):
    value = record(name)
    value["schema_registry_id"] = 2

    with pytest.raises(ValueError):
        decoder.decode(encode(name, [value]))


@pytest.mark.parametrize("name", NAMES)
def test_multiple_records(name):
    with pytest.raises(ValueError):
        decoder.decode(encode(name, [record(name), record(name)]))


@pytest.mark.parametrize("name", NAMES)
def test_empty_container(name):
    with pytest.raises(ValueError):
        decoder.decode(encode(name, []))


@pytest.mark.parametrize("name", NAMES)
def test_truncated_container(name):
    raw = encode(name, [record(name)])

    with pytest.raises(ValueError):
        decoder.decode(raw[:-10])


@pytest.mark.parametrize("raw", [b"", b"invalid", b'{"json":true}'])
def test_invalid_container(raw):
    with pytest.raises(ValueError):
        decoder.decode(raw)


def test_unknown_schema():
    stream = io.BytesIO()

    writer(
        stream,
        {
            "type": "record",
            "name": "ProjectedPaymentEvent",
            "fields": [{"name": "id", "type": "string"}]
        },
        [{"id": "test"}]
    )

    with pytest.raises(ValueError):
        decoder.decode(stream.getvalue())


@pytest.mark.parametrize("name", NAMES)
@pytest.mark.parametrize("old,new", [
    ("offset=0", "offset=1"),
    ("partition=0", "partition=1"),
    ("day=01", "day=02"),
    ("topic=agent.locations", "topic=agent.profiles"),
    ("category=agent_network", "category=mdm"),
    ("source_system=agent", "source_system=mtn"),
    ("raw/v1/streaming", "raw/v1/batch")
])
def test_object_path_mismatch(name, old, new):
    raw = encode(name, [record(name)])

    with pytest.raises(ValueError):
        decoder.derive_metadata(OBJECT.replace(old, new), raw)


def test_processing_mode_mismatch():
    name = "CanonicalSourceEnvelopeV1"
    value = record(name)
    value["processing_mode"] = "batch"

    with pytest.raises(ValueError):
        decoder.decode(encode(name, [value]))


@pytest.mark.parametrize("topic", [
    "unknown.topic",
    "mdm.beneficiary.golden.restricted",
    "mdm.sacco.golden"
])
def test_source_admission(topic):
    name = "CanonicalSourceEnvelopeV1"
    value = record(name)
    value["kafka_topic"] = topic

    with pytest.raises(ValueError):
        decoder.decode(encode(name, [value]))


@pytest.mark.parametrize("field", ["value_is_null", "is_tombstone"])
def test_null_and_tombstone_rejection(field):
    name = "CanonicalSourceEnvelopeV1"
    value = record(name)
    value[field] = True

    with pytest.raises(ValueError):
        decoder.decode(encode(name, [value]))
