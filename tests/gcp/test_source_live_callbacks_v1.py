from unittest.mock import patch
from google.api_core.exceptions import PreconditionFailed
import importlib.util,json,unittest
from pathlib import Path
p=Path(__file__).resolve().parents[2]/'platform/gcp/kafka_pubsub_bridge/source_live_callbacks_v1.py'
spec=importlib.util.spec_from_file_location('source_live_callbacks_v1',p)
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class FakeFuture:
 def __init__(self,value='msg-1',error=None):self.value=value;self.error=error
 def result(self):
  if self.error:raise self.error
  return self.value
class FakePublisher:
 def __init__(self,future):self.future=future;self.calls=[]
 def publish(self,topic,payload):self.calls.append((topic,payload));return self.future
class FakeBlob:
 def __init__(self,existing=None,fail=None):self.data=existing;self.fail=fail
 def upload_from_string(self,body,**kw):
  assert kw['if_generation_match']==0
  if self.fail:raise self.fail
  self.data=body
 def download_as_bytes(self, **kwargs):return self.data
class FakeBucket:
 def __init__(self,blob):self.b=blob
 def blob(self,key):return self.b
class FakeMessage:
 def __init__(self,e):self.data=json.dumps(e).encode();self.acks=0
 def ack(self):self.acks+=1
class WiringTests(unittest.TestCase):
 def setUp(self):
  self.record=dict(topic='payment-events.retry',partition=0,offset=2,timestamp_ms=1770000000000,value=b'{}')
  self.e=m.delivery.adapter.make_envelope(**dict(topic=self.record['topic'],partition=0,offset=2,timestamp_ms=1770000000000,payload=b'{}'))
 def test_publish_commit_after_future(self):
   with patch.object(m.delivery, 'APPROVED_SOURCE_TOPICS', frozenset({'payment-events.retry'})):
     steps=[];p=FakePublisher(FakeFuture())
     self.assertEqual(m.publish_kafka_record(self.record,p,'projects/test/topics/ordinary',lambda:steps.append('commit')),'msg-1')
     self.assertEqual(steps,['commit']);self.assertEqual(len(p.calls),1)
 def test_future_failure_no_commit(self):
  steps=[]
  with self.assertRaises(RuntimeError):m.publish_kafka_record(self.record,FakePublisher(FakeFuture(error=RuntimeError('failed'))),'topic',lambda:steps.append('commit'))
  self.assertEqual(steps,[])
 def test_restricted_no_publish(self):
  p=FakePublisher(FakeFuture());r=dict(self.record,topic='mdm.crosswalk.restricted')
  with self.assertRaises(m.delivery.adapter.contract.RouteRejected):m.publish_kafka_record(r,p,'topic',lambda:None)
  self.assertFalse(p.calls)
 def test_create_and_ack(self):
  msg=FakeMessage(self.e);b=FakeBlob()
  m.store_pubsub_message(msg,FakeBucket(b))
  self.assertEqual(msg.acks,1);self.assertIsNotNone(b.data)
 def test_conflict_no_ack(self):
  msg=FakeMessage(self.e);b=FakeBlob(existing=b'conflict',fail=PreconditionFailed('object already exists'))
  with self.assertRaises(m.delivery.DeliveryError):m.store_pubsub_message(msg,FakeBucket(b))
  self.assertEqual(msg.acks,0)
 def test_duplicate_identical_ack(self):
  key,body=m.delivery.adapter.prepare_object(self.e)
  msg=FakeMessage(self.e);b=FakeBlob(existing=body,fail=PreconditionFailed('object already exists'))
  m.store_pubsub_message(msg,FakeBucket(b));self.assertEqual(msg.acks,1)
 def test_upload_failure_no_ack(self):
  msg=FakeMessage(self.e)
  with self.assertRaises(RuntimeError):m.store_pubsub_message(msg,FakeBucket(FakeBlob(fail=RuntimeError('GCS failed'))))
  self.assertEqual(msg.acks,0)

 def test_unrelated_412_error_no_ack(self):
  class UnrelatedError(Exception):
   code=412

  msg=FakeMessage(self.e)
  blob=FakeBlob(fail=UnrelatedError('unrelated failure'))

  with self.assertRaises(UnrelatedError):
   m.store_pubsub_message(msg,FakeBucket(blob))

  self.assertEqual(msg.acks,0)


class GcsTimeoutAcceptance(unittest.TestCase):
    def setUp(self):
        self.envelope = m.delivery.adapter.make_envelope(
            topic='payment-events.retry',
            partition=0,
            offset=2,
            timestamp_ms=1770000000000,
            payload=b'{}',
        )

    def test_upload_and_readback_receive_timeout_policy(self):
        class RecordingBlob(FakeBlob):
            def __init__(self):
                super().__init__()
                self.upload_kwargs = None
                self.read_kwargs = None

            def upload_from_string(self, body, **kwargs):
                self.upload_kwargs = kwargs
                return super().upload_from_string(body, **kwargs)

            def download_as_bytes(self, **kwargs):
                self.read_kwargs = kwargs
                return super().download_as_bytes(**kwargs)

        blob = RecordingBlob()
        message = FakeMessage(self.envelope)

        m.store_pubsub_message(message, FakeBucket(blob))

        self.assertEqual(blob.upload_kwargs['timeout'], 20)
        self.assertIsNone(blob.upload_kwargs['retry'])
        self.assertEqual(blob.upload_kwargs['if_generation_match'], 0)
        self.assertEqual(
            blob.upload_kwargs['content_type'],
            'application/avro',
        )
        self.assertEqual(blob.read_kwargs['timeout'], 20)
        self.assertIsNone(blob.read_kwargs['retry'])
        self.assertEqual(message.acks, 1)

    def test_upload_timeout_never_acknowledges(self):
        class TimeoutBlob(FakeBlob):
            def upload_from_string(self, body, **kwargs):
                raise TimeoutError('simulated upload timeout')

        message = FakeMessage(self.envelope)

        with self.assertRaises(TimeoutError):
            m.store_pubsub_message(message, FakeBucket(TimeoutBlob()))

        self.assertEqual(message.acks, 0)

    def test_readback_timeout_never_acknowledges(self):
        class TimeoutBlob(FakeBlob):
            def download_as_bytes(self, **kwargs):
                raise TimeoutError('simulated readback timeout')

        message = FakeMessage(self.envelope)

        with self.assertRaises(TimeoutError):
            m.store_pubsub_message(message, FakeBucket(TimeoutBlob()))

        self.assertEqual(message.acks, 0)

    def test_duplicate_requires_exact_readback(self):
        _, body = m.delivery.adapter.prepare_object(self.envelope)

        class DuplicateBlob(FakeBlob):
            def __init__(self, existing):
                super().__init__(
                    existing=existing,
                    fail=PreconditionFailed('object already exists'),
                )
                self.read_kwargs = None

            def download_as_bytes(self, **kwargs):
                self.read_kwargs = kwargs
                return super().download_as_bytes(**kwargs)

        blob = DuplicateBlob(body)
        message = FakeMessage(self.envelope)

        result = m.store_pubsub_message(message, FakeBucket(blob))

        self.assertFalse(result['created'])
        self.assertEqual(message.acks, 1)
        self.assertEqual(blob.read_kwargs['timeout'], 20)
        self.assertIsNone(blob.read_kwargs['retry'])

        conflicting = DuplicateBlob(b'conflicting data')
        rejected = FakeMessage(self.envelope)

        with self.assertRaises(m.delivery.DeliveryError):
            m.store_pubsub_message(rejected, FakeBucket(conflicting))

        self.assertEqual(rejected.acks, 0)


if __name__=='__main__':unittest.main()
