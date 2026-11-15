import importlib.util
import pathlib
import unittest
from unittest.mock import patch

ROOT=pathlib.Path(__file__).resolve().parents[2]
P=ROOT/'platform/gcp/pubsub_raw_ingestion/raw_v1_cloud.py'
spec=importlib.util.spec_from_file_location('raw_v1_cloud',P)
cloud=importlib.util.module_from_spec(spec); spec.loader.exec_module(cloud)

class FakeBlob:
    def __init__(self): self.data=None; self.kwargs=None
    def upload_from_string(self,data,**kwargs):
        self.kwargs=kwargs
        if self.data is not None:
            raise FakePreconditionFailed('already exists')
        self.data=data
    def download_as_bytes(self): return self.data
class FakePreconditionFailed(Exception): pass
class FakeBucket:
    def __init__(self): self.objects={}
    def blob(self,name): return self.objects.setdefault(name,FakeBlob())
class FakeSubscriber:
    def __init__(self): self.calls=[]
    def acknowledge(self,request): self.calls.append(request)

class Tests(unittest.TestCase):
    def test_gcs_precondition_and_replay(self):
        import sys,types
        api=types.ModuleType('google.api_core.exceptions'); api.PreconditionFailed=FakePreconditionFailed
        core=types.ModuleType('google.api_core'); core.exceptions=api
        google=types.ModuleType('google'); google.api_core=core
        with patch.dict(sys.modules,{'google':google,'google.api_core':core,'google.api_core.exceptions':api}):
            bucket=FakeBucket(); store=cloud.GCSConditionalStore(bucket)
            self.assertTrue(store.create_if_absent('a',b'one'))
            self.assertEqual(bucket.blob('a').kwargs['if_generation_match'],0)
            self.assertFalse(store.create_if_absent('a',b'two'))
            self.assertEqual(store.read('a'),b'one')
    def test_ack_request(self):
        sub=FakeSubscriber(); cloud.PubSubAcknowledger(sub,'subscription').ack('id')
        self.assertEqual(sub.calls,[{'subscription':'subscription','ack_ids':['id']}])
    def test_failure_never_acks(self):
        sub=FakeSubscriber(); acker=cloud.PubSubAcknowledger(sub,'subscription')
        with self.assertRaises(cloud.raw.ContractError):
            cloud.process_message(b'not-json','id',FakeBucket(),acker,lambda w,t:{})
        self.assertEqual(sub.calls,[])

if __name__=='__main__': unittest.main()



def test_registry_decoder_rejects_trailing_bytes():
    import importlib.util
    import io
    import json
    from pathlib import Path
    from fastavro import parse_schema, schemaless_writer
    from confluent_kafka.schema_registry import Schema

    module_path = (
        Path(__file__).resolve().parents[2]
        / "platform/gcp/pubsub_raw_ingestion/raw_v1_cloud.py"
    )
    spec = importlib.util.spec_from_file_location(
        "raw_v1_strict_decoder_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    schema = {
        "type": "record",
        "name": "AcceptanceEvent",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "amount", "type": "double"},
        ],
    }
    record = {"event_id": "acceptance-001", "amount": 125.5}

    output = io.BytesIO()
    schemaless_writer(output, parse_schema(schema), record)
    wire = b"\x00" + (9).to_bytes(4, "big") + output.getvalue()

    class OfflineRegistry:
        def get_schema(self, schema_id, *args, **kwargs):
            if schema_id != 9:
                raise ValueError("unknown schema ID")
            return Schema(json.dumps(schema), "AVRO")

    decoder = module.RegistryDecoder(OfflineRegistry())

    assert decoder(wire, "pdmis.repayments") == record

    import pytest

    with pytest.raises(Exception):
        decoder(wire[:-1], "pdmis.repayments")

    with pytest.raises(ValueError, match="trailing Avro bytes"):
        decoder(wire + b"\x00\x01\x02", "pdmis.repayments")

def test_integrated_raw_v1_delivery_replay_and_collision(monkeypatch):
    import base64
    import hashlib
    import io
    import json

    import pytest
    from fastavro import parse_schema, schemaless_writer
    from confluent_kafka.schema_registry import Schema

    from google.api_core.exceptions import PreconditionFailed

    schema = {
        "type": "record",
        "name": "AcceptanceEvent",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "amount", "type": "double"},
        ],
    }

    event = {
        "event_id": "subscriber-acceptance-001",
        "amount": 125.5,
    }

    output = io.BytesIO()
    schemaless_writer(output, parse_schema(schema), event)

    wire = bytes([0]) + (9).to_bytes(4, "big") + output.getvalue()

    envelope = {
        "kafka_topic": "pdmis.repayments",
        "kafka_partition": 0,
        "kafka_offset": 17,
        "kafka_timestamp_ms": 1760000000000,
        "schema_registry_id": 9,
        "payload_base64": base64.b64encode(wire).decode("ascii"),
        "payload_sha256": hashlib.sha256(wire).hexdigest(),
    }

    data = json.dumps(envelope).encode("utf-8")

    operations = []
    objects = {}
    upload_options = []

    class OfflineRegistry:
        def get_schema(self, schema_id, *args, **kwargs):
            assert schema_id == 9
            operations.append("schema_lookup")
            return Schema(json.dumps(schema), "AVRO")

    class Blob:
        def __init__(self, name):
            self.name = name

        def upload_from_string(self, payload, **kwargs):
            operations.append("gcs_create_attempt")
            upload_options.append(kwargs)

            assert kwargs["if_generation_match"] == 0
            assert kwargs["content_type"] == "application/avro"

            if self.name in objects:
                raise PreconditionFailed("object already exists")

            objects[self.name] = bytes(payload)
            operations.append("gcs_created")

        def download_as_bytes(self):
            operations.append("gcs_readback")
            return objects[self.name]

    class Bucket:
        def blob(self, name):
            return Blob(name)

    class Subscriber:
        def __init__(self):
            self.requests = []

        def acknowledge(self, request):
            operations.append("pubsub_ack")
            self.requests.append(request)

    bucket = Bucket()
    subscriber = Subscriber()

    store = cloud.GCSConditionalStore(bucket)
    acker = cloud.PubSubAcknowledger(
        subscriber,
        "projects/offline/subscriptions/raw-v1",
    )
    decoder = cloud.RegistryDecoder(OfflineRegistry())

    name, state = cloud.process_message(
        data,
        "ack-first",
        store,
        acker,
        decoder,
    )

    assert state == "CREATED"

    assert name.startswith(
        "raw/v1/streaming/category=pdmis/"
        "source_group=pdmis/source_system=pdmis/"
    )

    assert name.endswith(
        "/topic=pdmis.repayments/"
        "partition=0/offset=17/record.avro"
    )

    assert len(objects) == 1
    assert objects[name][:4] == bytes([79, 98, 106, 1])

    stored_record = cloud.raw.ocf_read(objects[name])

    assert stored_record["payload_wire"] == wire
    assert stored_record["payload_sha256"] == hashlib.sha256(wire).hexdigest()
    assert stored_record["schema_registry_id"] == 9
    assert stored_record["kafka_topic"] == "pdmis.repayments"
    assert stored_record["kafka_partition"] == 0
    assert stored_record["kafka_offset"] == 17

    assert operations.index("gcs_created") < operations.index(
        "gcs_readback"
    )
    assert operations.index("gcs_readback") < operations.index(
        "pubsub_ack"
    )

    assert subscriber.requests == [{
        "subscription": "projects/offline/subscriptions/raw-v1",
        "ack_ids": ["ack-first"],
    }]

    original = objects[name]

    replay_name, replay_state = cloud.process_message(
        data,
        "ack-replay",
        store,
        acker,
        decoder,
    )

    assert replay_name == name
    assert replay_state == "ALREADY_PRESENT_IDENTICAL"
    assert objects[name] == original
    assert len(objects) == 1

    assert subscriber.requests[-1]["ack_ids"] == ["ack-replay"]

    ack_count = len(subscriber.requests)

    objects[name] = b"invalid-existing-object"

    with pytest.raises(cloud.raw.CollisionError):
        cloud.process_message(
            data,
            "ack-collision",
            store,
            acker,
            decoder,
        )

    assert len(subscriber.requests) == ack_count
    assert objects[name] == b"invalid-existing-object"

    assert all(
        options["if_generation_match"] == 0
        for options in upload_options
    )
