
import hashlib
import io
import json
import sys
from pathlib import Path

import pytest
from fastavro import writer
from google.api_core.exceptions import PreconditionFailed

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/metadata"))

from raw_v1_metadata_contract import MetadataContract
from raw_v1_avro_decoder import RawAvroDecoder
from raw_v1_conditional_creation import create_and_verify

contract = MetadataContract(ROOT)
decoder = RawAvroDecoder(contract, ROOT)


def fixture(topic):
    route = contract.routes[topic]

    payload = (
        b'{"test":true}'
        if route["format"] == "json"
        else b"\x00\x00\x00\x00\x01example"
    )

    record = {
        "kafka_topic": topic,
        "kafka_partition": 0,
        "kafka_offset": 0,
        "kafka_timestamp_ms": 0,
        "schema_registry_id": (
            None if route["format"] == "json" else 1
        ),
        "payload_bytes": payload,
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "payload_format": route["format"],
        "processing_mode": route["processing_mode"],
        "value_is_null": False,
        "is_tombstone": False
    }

    stream = io.BytesIO()

    writer(
        stream,
        decoder.schemas["CanonicalSourceEnvelopeV1"],
        [record]
    )

    name = (
        f"raw/v1/{route['processing_mode']}/"
        f"category={route['category']}/"
        f"source_group={route['source_group']}/"
        f"source_system={route['source_system']}/"
        f"year=1970/month=01/day=01/topic={topic}/"
        "partition=0/offset=0/record.avro"
    )

    return name, stream.getvalue()


@pytest.mark.parametrize("topic", sorted(contract.routes))
def test_admission_policy(topic):
    route = contract.routes[topic]

    expected = (
        route["status"] == "legacy_payment_event"
        or (
            route["status"] == "contract_only"
            and topic in contract.approved_source_topics
        )
    )

    assert contract.source_admitted(topic) == expected


@pytest.mark.parametrize(
    "topic", sorted(contract.approved_source_topics)
)
def test_approved_mdm(topic):
    name, body = fixture(topic)
    metadata, decoded = decoder.derive_metadata(name, body)

    assert len(metadata) == 11
    assert metadata["data-domain"] == "master_data_management"
    assert metadata["source-topic"] == topic
    assert decoded["payload_bytes"] == b'{"test":true}'


def test_admission_totals():
    assert sum(
        contract.source_admitted(t)
        for t in contract.routes
    ) == 27

    assert not contract.source_admitted("unknown.topic")


class Bucket:

    def __init__(self):
        self.state = None
        self.body = None
        self.uploads = 0
        self.failure = None

    def blob(self, name):
        return Blob(self)


class Blob:

    def __init__(self, bucket):
        self.bucket = bucket

    def upload_from_string(self, body, **kwargs):
        b = self.bucket

        assert kwargs == {
            "content_type": "application/avro",
            "if_generation_match": 0,
            "checksum": "crc32c"
        }

        assert len(self.metadata) == 11
        assert self.cache_control == "no-store"

        if b.state is not None:
            raise PreconditionFailed("Already exists")

        if b.failure == "upload":
            raise RuntimeError("Upload failed")

        b.uploads += 1
        b.body = body

        b.state = dict(
            generation=1,
            metageneration=1,
            crc32c="fixture-crc",
            md5_hash="fixture-md5",
            size=len(body),
            content_type="application/avro",
            cache_control=self.cache_control,
            content_encoding=None,
            content_disposition=None,
            content_language=None,
            custom_time=None,
            metadata=dict(self.metadata)
        )

        self.generation = b.state["generation"]

        if b.failure == "missing_generation":
            self.generation = None

        if b.failure == "generation_mismatch":
            b.state["generation"] += 1

        if b.failure == "metadata":
            b.state["metadata"].pop("data-domain")

    def reload(self):
        self.__dict__.update(self.bucket.state)
        self.metadata = dict(self.metadata)

    def download_as_bytes(self, **kwargs):
        b = self.bucket

        assert kwargs["if_generation_match"] == b.state["generation"]
        assert kwargs["if_metageneration_match"] == b.state["metageneration"]
        assert kwargs["raw_download"] is True
        assert kwargs["checksum"] == "crc32c"

        return b.body


@pytest.mark.parametrize(
    "topic", ["agent.locations"] + sorted(contract.approved_source_topics)
)
def test_create_and_retry(topic):
    bucket = Bucket()
    name, body = fixture(topic)

    result = create_and_verify(bucket, name, body, decoder)

    assert result["status"] == "CREATED_VERIFIED"
    assert result["metadata_verified"]
    assert result["payload_verified"]

    retry = create_and_verify(bucket, name, body, decoder)

    assert retry["status"] == "ALREADY_PRESENT_IDENTICAL"
    assert bucket.uploads == 1


def test_conflicting_payload():
    bucket = Bucket()
    name, body = fixture("agent.locations")

    create_and_verify(bucket, name, body, decoder)

    bucket.body = bytes([body[0] ^ 1]) + body[1:]

    with pytest.raises(RuntimeError, match="Conflicting"):
        create_and_verify(bucket, name, body, decoder)

    assert bucket.uploads == 1


def test_existing_missing_metadata():
    bucket = Bucket()
    name, body = fixture("agent.locations")

    create_and_verify(bucket, name, body, decoder)

    bucket.state["metadata"].pop("data-domain")

    with pytest.raises(RuntimeError, match="remediation"):
        create_and_verify(bucket, name, body, decoder)

    assert bucket.uploads == 1


def test_existing_conflicting_metadata():
    bucket = Bucket()
    name, body = fixture("agent.locations")

    create_and_verify(bucket, name, body, decoder)

    bucket.state["metadata"]["data-domain"] = "payments"

    with pytest.raises(ValueError, match="conflict"):
        create_and_verify(bucket, name, body, decoder)

    assert bucket.uploads == 1


@pytest.mark.parametrize("failure", ["upload", "metadata"])
def test_creation_failure(failure):
    bucket = Bucket()
    bucket.failure = failure

    name, body = fixture("agent.locations")

    with pytest.raises(RuntimeError):
        create_and_verify(bucket, name, body, decoder)


def test_invalid_object_before_upload():
    bucket = Bucket()
    name, _ = fixture("agent.locations")

    with pytest.raises(ValueError):
        create_and_verify(bucket, name, b"invalid", decoder)

    assert bucket.uploads == 0
