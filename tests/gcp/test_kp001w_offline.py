import base64
import hashlib
import importlib.util
import json
import pathlib
import unittest

MODULE = pathlib.Path(__file__).resolve().parents[2] / 'platform/gcp/pubsub_raw_ingestion/worker.py'
spec = importlib.util.spec_from_file_location('kp001w_worker', MODULE)
worker = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = worker
spec.loader.exec_module(worker)


def envelope(offset=108):
    wire = b'\x00' + (6).to_bytes(4, 'big') + b'synthetic-avro-test'
    return json.dumps({'kafka_topic':'icmn.pmn.pain001','kafka_partition':5,'kafka_offset':offset,
      'schema_registry_id':6,'payload_base64':base64.b64encode(wire).decode(),
      'payload_sha256':hashlib.sha256(wire).hexdigest()},sort_keys=True).encode()


class Store:
    def __init__(self): self.objects = {}; self.fail = False
    def create_if_absent(self, name, data):
        if self.fail: raise OSError('storage unavailable')
        if name in self.objects: return False
        self.objects[name] = data
        return True
    def read(self, name):
        if self.fail: raise OSError('readback unavailable')
        return self.objects[name]


class Acker:
    def __init__(self): self.calls = []
    def ack(self, ack_id): self.calls.append(ack_id)


class WorkerTests(unittest.TestCase):
    def setUp(self): self.store = Store(); self.acker = Acker(); self.data = envelope()
    def run_one(self, data=None): return worker.process(data or self.data,'ack1',self.store,self.acker)
    def test_create_then_ack(self):
        r=self.run_one(); self.assertEqual(r.state,'CREATED'); self.assertEqual(self.acker.calls,['ack1'])
    def test_duplicate_identical_acks(self):
        self.run_one(); r=self.run_one(); self.assertEqual(r.state,'ALREADY_PRESENT_IDENTICAL'); self.assertEqual(len(self.acker.calls),2)
    def test_conflicting_object_no_ack(self):
        name=worker.validate(self.data); self.store.objects[name]=b'conflict'
        with self.assertRaises(worker.ConflictError): self.run_one()
        self.assertEqual(self.acker.calls,[])
    def test_write_failure_no_ack(self):
        self.store.fail=True
        with self.assertRaises(OSError): self.run_one()
        self.assertEqual(self.acker.calls,[])
    def test_readback_failure_no_ack(self):
        name=worker.validate(self.data); self.store.objects[name]=self.data; self.store.fail=True
        with self.assertRaises(OSError): self.run_one()
        self.assertEqual(self.acker.calls,[])
    def test_invalid_hash_no_ack(self):
        obj=json.loads(self.data); obj['payload_sha256']='0'*64
        with self.assertRaises(worker.InvalidEnvelope): self.run_one(json.dumps(obj).encode())
        self.assertEqual(self.acker.calls,[])
    def test_invalid_identity_no_ack(self):
        obj=json.loads(self.data); obj['kafka_offset']=-1
        with self.assertRaises(worker.InvalidEnvelope): self.run_one(json.dumps(obj).encode())
        self.assertEqual(self.acker.calls,[])
    def test_invalid_wire_schema_no_ack(self):
        obj=json.loads(self.data); obj['schema_registry_id']=7
        with self.assertRaises(worker.InvalidEnvelope): self.run_one(json.dumps(obj).encode())
        self.assertEqual(self.acker.calls,[])
    def test_topic_traversal_rejected(self):
        obj=json.loads(self.data); obj['kafka_topic']='../secret'
        with self.assertRaises(worker.InvalidEnvelope): self.run_one(json.dumps(obj).encode())
        self.assertEqual(self.acker.calls,[])
    def test_distinct_offsets_distinct_objects(self):
        first=self.run_one(); second=self.run_one(envelope(109))
        self.assertNotEqual(first.object_name,second.object_name)
