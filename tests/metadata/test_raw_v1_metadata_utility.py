
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
from raw_v1_metadata import execute

decoder = RawAvroDecoder(MetadataContract(ROOT), ROOT)

OBJECT = (
    "raw/v1/streaming/category=agent_network/"
    "source_group=agent/source_system=agent/"
    "year=1970/month=01/day=01/"
    "topic=agent.locations/partition=0/offset=0/record.avro"
)

wire = b"\x00\x00\x00\x00\x01example"

stream = io.BytesIO()

writer(stream, decoder.schemas["RawSourceEvent"], [{
    "kafka_topic": "agent.locations",
    "kafka_partition": 0,
    "kafka_offset": 0,
    "kafka_timestamp_ms": 0,
    "schema_registry_id": 1,
    "payload_sha256": hashlib.sha256(wire).hexdigest(),
    "payload_wire": wire
}])

RAW = stream.getvalue()


class Bucket:

    def __init__(self):
        self.payload = RAW
        self.patches = 0
        self.reads = 0
        self.failure = None

        self.state = dict(
            generation=1,
            metageneration=1,
            crc32c="fixture-crc",
            md5_hash="fixture-md5",
            size=len(RAW),
            content_type="application/avro",
            cache_control=None,
            content_encoding=None,
            content_disposition=None,
            content_language=None,
            custom_time=None,
            metadata={}
        )

    def blob(self, name):
        assert name == OBJECT
        return Blob(self)


class Blob:

    def __init__(self, bucket):
        self.bucket = bucket

    def reload(self):
        self.__dict__.update(self.bucket.state)
        self.metadata = dict(self.metadata)

    def download_as_bytes(self, **kwargs):
        b = self.bucket

        assert kwargs["raw_download"] is True
        assert kwargs["checksum"] == "crc32c"

        assert kwargs["if_generation_match"] == \
            b.state["generation"]

        assert kwargs["if_metageneration_match"] == \
            b.state["metageneration"]

        b.reads += 1

        if b.failure == "payload" and b.reads == 2:
            return bytes([b.payload[0] ^ 1]) + b.payload[1:]

        return b.payload

    def patch(self, **kwargs):
        b = self.bucket

        if b.failure == "generation":
            b.state["generation"] += 1

        if b.failure == "metageneration":
            b.state["metageneration"] += 1

        if kwargs["if_generation_match"] != \
                b.state["generation"]:
            raise RuntimeError("Stale generation")

        if kwargs["if_metageneration_match"] != \
                b.state["metageneration"]:
            raise RuntimeError("Stale metageneration")

        b.patches += 1
        b.state["metadata"] = dict(self.metadata)
        b.state["cache_control"] = self.cache_control
        b.state["metageneration"] += 1

        if b.failure == "verification":
            b.state["metadata"]["source-offset"] = "99"


def test_dry_run():
    b = Bucket()
    report = execute(b, OBJECT, decoder)

    assert report["status"] == "DRY_RUN_PASS"
    assert report["update_required"]
    assert len(report["proposed_custom_metadata"]) == 11
    assert b.patches == 0
    assert b.reads == 2
    assert all(report["checks"].values())


def test_apply():
    b = Bucket()
    report = execute(b, OBJECT, decoder, apply=True)

    assert report["status"] == "PASS"
    assert b.patches == 1
    assert b.reads == 2
    assert b.state["generation"] == 1
    assert b.state["metageneration"] == 2
    assert all(report["checks"].values())


def test_idempotency():
    b = Bucket()

    execute(b, OBJECT, decoder, apply=True)
    report = execute(b, OBJECT, decoder, apply=True)

    assert report["status"] == "NO_CHANGE_REQUIRED"
    assert b.patches == 1
    assert b.reads == 4
    assert all(report["checks"].values())


def test_conflict():
    b = Bucket()
    b.state["metadata"] = {"data-domain": "payments"}

    with pytest.raises(ValueError, match="Metadata conflict"):
        execute(b, OBJECT, decoder, apply=True)

    assert b.patches == 0


@pytest.mark.parametrize("failure", [
    "generation", "metageneration"
])
def test_concurrent_update(failure):
    b = Bucket()
    b.failure = failure

    with pytest.raises(RuntimeError, match="Stale"):
        execute(b, OBJECT, decoder, apply=True)

    assert b.patches == 0


@pytest.mark.parametrize("failure", [
    "payload", "verification"
])
def test_post_update_failure(failure):
    b = Bucket()
    b.failure = failure

    with pytest.raises(RuntimeError):
        execute(b, OBJECT, decoder, apply=True)

    assert b.patches == 1


def test_noop_payload_failure():
    b = Bucket()

    execute(b, OBJECT, decoder, apply=True)

    b.reads = 0
    b.failure = "payload"

    with pytest.raises(RuntimeError):
        execute(b, OBJECT, decoder, apply=True)

    assert b.patches == 1


@pytest.mark.parametrize("key,value", [
    ("content_type", "application/json"),
    ("content_encoding", "gzip"),
    ("content_disposition", "attachment"),
    ("content_language", "en"),
    ("custom_time", "unexpected"),
    ("crc32c", None)
])
def test_native_metadata_rejection(key, value):
    b = Bucket()
    b.state[key] = value

    with pytest.raises(ValueError):
        execute(b, OBJECT, decoder, apply=True)

    assert b.patches == 0


def test_preserve_existing_metadata():
    b = Bucket()
    b.state["metadata"] = {"existing-tag": "preserve"}

    execute(b, OBJECT, decoder, apply=True)

    assert b.state["metadata"]["existing-tag"] == "preserve"
    assert len(b.state["metadata"]) == 12


def test_invalid_avro():
    b = Bucket()
    b.payload = b"invalid"
    b.state["size"] = len(b.payload)

    with pytest.raises(ValueError):
        execute(b, OBJECT, decoder, apply=True)

    assert b.patches == 0
