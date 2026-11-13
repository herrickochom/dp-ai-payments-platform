from unittest.mock import patch
import importlib.util,unittest
from pathlib import Path
p=Path(__file__).resolve().parents[2]/'platform/gcp/kafka_pubsub_bridge/source_bounded_entrypoint_v1.py'
spec=importlib.util.spec_from_file_location('source_bounded_entrypoint_v1',p)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Message:
 def __init__(self,topic='payment-events.retry',value=b'{}',error=None):self._topic=topic;self._value=value;self._error=error
 def error(self):return self._error
 def timestamp(self):return (1,1770000000000)
 def topic(self):return self._topic
 def partition(self):return 0
 def offset(self):return 9
 def value(self):return self._value
class Consumer:
 def __init__(self,fail=False,result_override=None):
  self.commits=0
  self.fail=fail
  self.result_override=result_override
 def commit(self,**kw):
  assert kw['asynchronous'] is False
  assert isinstance(kw['message'],Message)
  self.commits+=1
  if self.fail:return None
  if self.result_override is not None:return self.result_override
  msg=kw['message']
  return [type('TP',(),{
   'topic':msg.topic(),
   'partition':msg.partition(),
   'offset':msg.offset()+1,
   'err':None
  })()]
class Future:
 def __init__(self,fail=False):self.fail=fail
 def result(self):
  if self.fail:raise RuntimeError('publish failure')
  return 'message-id'
class Publisher:
 def __init__(self,fail=False):self.calls=0;self.fail=fail
 def publish(self,topic,data):self.calls+=1;return Future(self.fail)
class Tests(unittest.TestCase):
 def test_commit_after_publish(self):
   with patch.object(m.callbacks.delivery, 'APPROVED_SOURCE_TOPICS', frozenset({'payment-events.retry'})):
     c=Consumer();p=Publisher()
     self.assertEqual(m.publish_one(Message(),c,p,'projects/example/topics/ordinary'),'message-id')
     self.assertEqual((p.calls,c.commits),(1,1))
 def test_failure_no_commit(self):
  c=Consumer();p=Publisher(True)
  with self.assertRaises(RuntimeError):m.publish_one(Message(),c,p,'topic')
  self.assertEqual(c.commits,0)
 def test_restricted_no_publish(self):
  c=Consumer();p=Publisher()
  with self.assertRaises(m.callbacks.delivery.adapter.contract.RouteRejected):m.publish_one(Message('mdm.crosswalk.restricted'),c,p,'topic')
  self.assertEqual((p.calls,c.commits),(0,0))
 def test_legacy_not_rerouted(self):
  c=Consumer();p=Publisher()
  with self.assertRaises(Exception):m.publish_one(Message('icmn.vpm.pain001',b'\x00\x00\x00\x00\x01x'),c,p,'topic')
  self.assertEqual((p.calls,c.commits),(0,0))
 def test_kafka_error(self):
  c=Consumer();p=Publisher()
  with self.assertRaises(m.EntrypointError):m.publish_one(Message(error=RuntimeError('bad')),c,p,'topic')
  self.assertEqual((p.calls,c.commits),(0,0))
 def test_bound(self):
   with patch.object(m.callbacks.delivery, 'APPROVED_SOURCE_TOPICS', frozenset({'payment-events.retry'})):
     c=Consumer();p=Publisher()
     self.assertEqual(m.bounded_publish([Message() for _ in range(4)],c,p,'topic',2),2)
     self.assertEqual((p.calls,c.commits),(2,2))
 def test_invalid_bound(self):
  with self.assertRaises(m.EntrypointError):m.bounded_publish([],Consumer(),Publisher(),'topic',11)
 def test_missing_commit_confirmation(self):
   with patch.object(m.callbacks.delivery, 'APPROVED_SOURCE_TOPICS', frozenset({'payment-events.retry'})):
     with self.assertRaises(m.EntrypointError):m.publish_one(Message(),Consumer(True),Publisher(),'topic')

 def test_commit_coordinates_rejected(self):
  with patch.object(m.callbacks.delivery,'APPROVED_SOURCE_TOPICS',
                    frozenset({'payment-events.retry'})):
   cases=[
    ('wrong topic','other.topic',0,10,None),
    ('wrong partition','payment-events.retry',1,10,None),
    ('wrong offset','payment-events.retry',0,9,None),
    ('commit error','payment-events.retry',0,10,'broker error'),
   ]
   for label,topic,partition,offset,error in cases:
    with self.subTest(label=label):
     result=[type('TP',(),{
      'topic':topic,'partition':partition,
      'offset':offset,'err':error
     })()]
     c=Consumer(result_override=result)
     p=Publisher()
     with self.assertRaises(m.EntrypointError):
      m.publish_one(Message(),c,p,'topic')
     self.assertEqual((p.calls,c.commits),(1,1))

 def test_ambiguous_commit_confirmation_rejected(self):
  with patch.object(m.callbacks.delivery,'APPROVED_SOURCE_TOPICS',
                    frozenset({'payment-events.retry'})):
   valid=type('TP',(),{
    'topic':'payment-events.retry',
    'partition':0,'offset':10,'err':None
   })()
   for result in ([],[valid,valid]):
    with self.subTest(result_count=len(result)):
     with self.assertRaises(m.EntrypointError):
      m.publish_one(
       Message(),
       Consumer(result_override=result),
       Publisher(),
       'topic'
      )

if __name__=='__main__':unittest.main()


class DeferredAcknowledgementAcceptance(unittest.TestCase):

    def test_verified_storage_does_not_ack(self):
        class Message:
            data = b"test"

            def __init__(self):
                self.acks = 0

            def ack(self):
                self.acks += 1

        message = Message()
        bucket = object()
        calls = []

        def fake_store(received, supplied_bucket, ack=None):
            calls.append((received, supplied_bucket))
            self.assertIsNotNone(ack)
            ack()
            return {"verified": True}

        with patch.object(
            m.callbacks,
            "store_pubsub_message",
            side_effect=fake_store,
        ):
            result = m.store_one_deferred(message, bucket)

        self.assertEqual(result, {"verified": True})
        self.assertEqual(calls, [(message, bucket)])
        self.assertEqual(message.acks, 0)

    def test_storage_failure_does_not_ack(self):
        class Message:
            data = b"test"

            def __init__(self):
                self.acks = 0

            def ack(self):
                self.acks += 1

        message = Message()

        with patch.object(
            m.callbacks,
            "store_pubsub_message",
            side_effect=RuntimeError("simulated storage failure"),
        ):
            with self.assertRaises(RuntimeError):
                m.store_one_deferred(message, object())

        self.assertEqual(message.acks, 0)

