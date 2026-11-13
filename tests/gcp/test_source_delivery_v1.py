from unittest.mock import patch
import importlib.util,unittest
from pathlib import Path
p=Path(__file__).resolve().parents[2]/"platform/gcp/kafka_pubsub_bridge/source_delivery_v1.py"
spec=importlib.util.spec_from_file_location("source_delivery_v1",p)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class DeliveryTests(unittest.TestCase):
 def setUp(self):
  self.e=m.adapter.make_envelope("payment-events.retry",0,9,1770000000000,b'{}')
 def test_publish_commit_after_confirmation(self):
   with patch.object(m, 'APPROVED_SOURCE_TOPICS', frozenset({'payment-events.retry'})):
     steps=[]
     self.assertEqual(m.publish_then_commit(self.e,lambda e:(steps.append('publish') or 'id-1'),lambda:steps.append('commit')),'id-1')
     self.assertEqual(steps,['publish','commit'])
 def test_no_commit_on_publish_error(self):
  steps=[]
  with self.assertRaises(RuntimeError):
   m.publish_then_commit(self.e,lambda e:(_ for _ in ()).throw(RuntimeError('offline failure')),lambda:steps.append('commit'))
  self.assertEqual(steps,[])
 def test_no_commit_without_confirmation(self):
  steps=[]
  with self.assertRaises(m.DeliveryError):m.publish_then_commit(self.e,lambda e:None,lambda:steps.append('commit'))
  self.assertEqual(steps,[])
 def test_create_readback_ack(self):
  storage={};steps=[]
  def create(k,b):storage[k]=b;steps.append('create');return True
  def read(k):steps.append('read');return storage[k]
  out=m.store_then_ack(self.e,create,read,lambda:steps.append('ack'))
  self.assertTrue(out['created']);self.assertEqual(steps,['create','read','ack'])
 def test_duplicate_identical(self):
  storage={};key,body=m.adapter.prepare_object(self.e);storage[key]=body;steps=[]
  out=m.store_then_ack(self.e,lambda k,b:False,lambda k:storage[k],lambda:steps.append('ack'))
  self.assertFalse(out['created']);self.assertEqual(steps,['ack'])
 def test_conflict_no_ack(self):
  steps=[]
  with self.assertRaises(m.DeliveryError):m.store_then_ack(self.e,lambda k,b:False,lambda k:b'conflict',lambda:steps.append('ack'))
  self.assertEqual(steps,[])
 def test_storage_failure_no_ack(self):
  steps=[]
  with self.assertRaises(RuntimeError):m.store_then_ack(self.e,lambda k,b:(_ for _ in ()).throw(RuntimeError('storage error')),lambda k:b'',lambda:steps.append('ack'))
  self.assertEqual(steps,[])
 def test_restricted_no_publish(self):
  e=dict(self.e,kafka_topic='mdm.crosswalk.restricted');steps=[]
  with self.assertRaises(m.adapter.contract.RouteRejected):m.publish_then_commit(e,lambda e:steps.append('publish'),lambda:steps.append('commit'))
  self.assertEqual(steps,[])
 def test_legacy_writer_untouched(self):
  e=m.adapter.make_envelope('icmn.vpm.pain001',0,1,1770000000000,b'\x00\x00\x00\x00\x01x',1)
  with self.assertRaises(m.DeliveryError):m.publish_then_commit(e,lambda e:'id',lambda:None)
  with self.assertRaises(m.adapter.AdapterError):m.store_then_ack(e,lambda k,b:True,lambda k:b'',lambda:None)
if __name__=='__main__':unittest.main()
