import base64
import hashlib
import importlib.util
import json
import pathlib
import unittest
from datetime import datetime,timezone

ROOT=pathlib.Path(__file__).resolve().parents[2]
MODULE=ROOT/'platform/gcp/pubsub_raw_ingestion/raw_v2.py'
spec=importlib.util.spec_from_file_location('kp001_raw_v2',MODULE)
raw=importlib.util.module_from_spec(spec); spec.loader.exec_module(raw)

class Store:
    def __init__(self): self.objects={}
    def create_if_absent(self,name,data):
        if name in self.objects: return False
        self.objects[name]=data; return True
    def read(self,name): return self.objects[name]
class Ack:
    def __init__(self): self.calls=[]
    def ack(self,ack_id): self.calls.append(ack_id)

class Tests(unittest.TestCase):
    def envelope(self,offset=7):
        wire=bytes([0,0,0,0,2,1,2,3])
        return json.dumps({'kafka_topic':'icmn.pmn.pain001','kafka_partition':2,
          'kafka_offset':offset,'kafka_timestamp_ms':1789257600000,'schema_registry_id':2,
          'payload_base64':base64.b64encode(wire).decode(),
          'payload_sha256':hashlib.sha256(wire).hexdigest()}).encode()
    def decode(self,wire,topic):
        self.assertEqual(topic,'icmn.pmn.pain001')
        return {'event_id':'e1','instructed_amount':12.5,'currency':'UGX'}
    def test_ocf_and_replay(self):
        store=Store(); ack=Ack(); data=self.envelope()
        name,state=raw.process_raw_v2(data,'a',store,ack,self.decode)
        self.assertTrue(name.startswith('raw/v2/category=icmn/source_group=icmn/source_system=pmn/'))
        self.assertTrue(name.endswith('/topic=icmn.pmn.pain001/partition=2/offset=7/record.avro'))
        self.assertEqual(state,'CREATED')
        self.assertEqual(store.read(name)[:4],bytes([79,98,106,1]))
        self.assertEqual(raw.ocf_read(store.read(name))['payload']['amount'],12.5)
        self.assertEqual(raw.process_raw_v2(data,'b',store,ack,self.decode)[1],'ALREADY_PRESENT_IDENTICAL')
        self.assertEqual(ack.calls,['a','b'])
    def test_collision_never_acks(self):
        store=Store(); ack=Ack(); data=self.envelope()
        name,_=raw.process_raw_v2(data,'a',store,ack,self.decode)
        store.objects[name]=b'bad'
        with self.assertRaises(raw.CollisionError): raw.process_raw_v2(data,'b',store,ack,self.decode)
        self.assertEqual(ack.calls,['a'])
    def test_bad_digest_never_acks(self):
        store=Store(); ack=Ack(); obj=json.loads(self.envelope()); obj['payload_sha256']='0'*64
        with self.assertRaises(raw.ContractError): raw.process_raw_v2(json.dumps(obj).encode(),'a',store,ack,self.decode)
        self.assertEqual(ack.calls,[])
    def test_decoder_failure_never_acks(self):
        def fail(wire,topic): raise RuntimeError('schema registry failed')
        ack=Ack()
        with self.assertRaises(RuntimeError): raw.process_raw_v2(self.envelope(),'a',Store(),ack,fail)
        self.assertEqual(ack.calls,[])
    def test_same_coordinate_different_record_fails(self):
        store=Store(); ack=Ack(); data=self.envelope()
        raw.process_raw_v2(data,'a',store,ack,self.decode)
        with self.assertRaises(raw.CollisionError):
            raw.process_raw_v2(data,'b',store,ack,lambda w,t:{'event_id':'other'})
        self.assertEqual(ack.calls,['a'])

if __name__=='__main__': unittest.main()
