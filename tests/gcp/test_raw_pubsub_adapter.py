import base64
import hashlib
import json
import unittest
from pathlib import Path
import sys
# Import the existing kp001w namespace without shadowing Python stdlib platform.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "platform" / "gcp"))
from pubsub_raw_ingestion.pubsub_adapter import handle_message
from pubsub_raw_ingestion.worker import ConflictError, InvalidEnvelope


def envelope(offset=108):
    wire = b'\x00' + (6).to_bytes(4, 'big') + b'synthetic-avro-test'
    return json.dumps({
        'kafka_topic':'icmn.pmn.pain001', 'kafka_partition':5,
        'kafka_offset':offset, 'schema_registry_id':6,
        'payload_base64':base64.b64encode(wire).decode(),
        'payload_sha256':hashlib.sha256(wire).hexdigest(),
    }, sort_keys=True).encode()


class Message:
    def __init__(self, data): self.data=data; self.acks=0
    def ack(self): self.acks += 1


class Store:
    def __init__(self): self.objects={}; self.fail_write=False; self.fail_read=False
    def create_if_absent(self, name, data):
        if self.fail_write: raise OSError('write failure')
        if name in self.objects: return False
        self.objects[name]=data
        return True
    def read(self, name):
        if self.fail_read: raise OSError('read failure')
        return self.objects[name]


class PubSubAdapterTests(unittest.TestCase):
    def setUp(self): self.store=Store(); self.msg=Message(envelope())
    def test_created_ack_once(self):
        self.assertEqual(handle_message(self.msg,self.store).state,'CREATED')
        self.assertEqual(self.msg.acks,1)
    def test_identical_duplicate_ack_once(self):
        handle_message(self.msg,self.store)
        second=Message(self.msg.data)
        self.assertEqual(handle_message(second,self.store).state,'ALREADY_PRESENT_IDENTICAL')
        self.assertEqual(second.acks,1)
    def test_conflict_no_ack(self):
        self.store.objects['raw/native/v1/icmn.pmn.pain001/partition=05/offset=00000000000000000108.json']=b'conflict'
        with self.assertRaises(ConflictError): handle_message(self.msg,self.store)
        self.assertEqual(self.msg.acks,0)
    def test_write_failure_no_ack(self):
        self.store.fail_write=True
        with self.assertRaises(OSError): handle_message(self.msg,self.store)
        self.assertEqual(self.msg.acks,0)
    def test_read_failure_no_ack(self):
        self.store.fail_read=True
        with self.assertRaises(OSError): handle_message(self.msg,self.store)
        self.assertEqual(self.msg.acks,0)
    def test_invalid_envelope_no_ack(self):
        self.msg.data=b'not json'
        with self.assertRaises(InvalidEnvelope): handle_message(self.msg,self.store)
        self.assertEqual(self.msg.acks,0)
    def test_nonbytes_no_ack(self):
        self.msg.data='not bytes'
        with self.assertRaises(TypeError): handle_message(self.msg,self.store)
        self.assertEqual(self.msg.acks,0)
    def test_ack_failure_propagates(self):
        def bad_ack(): raise OSError('ack transport failure')
        self.msg.ack=bad_ack
        with self.assertRaises(OSError): handle_message(self.msg,self.store)
