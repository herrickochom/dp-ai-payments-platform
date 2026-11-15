import importlib.util,json,unittest
from pathlib import Path
p=Path(__file__).resolve().parents[2]/'platform/gcp/kafka_pubsub_bridge/source_operational_runtime_v1.py'
s=importlib.util.spec_from_file_location('source_operational_runtime_v1',p)
m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Acker:
 def __init__(self):self.ids=[]
 def ack(self,x):self.ids.append(x)
class Tests(unittest.TestCase):
 def test_classification(self):
  self.assertFalse(m.is_source_envelope(b'{"payload_base64":"AA=="}'))
  self.assertFalse(m.is_source_envelope(b'{"event_id":"legacy"}'))
 def test_malformed_source_does_not_fall_back(self):
  a=Acker();legacy=[]
  with self.assertRaises(Exception):
   m.dispatch(b'{"payload_base64":"not-base64"}','id',object(),a,lambda *x:legacy.append(1),None,None)
  self.assertEqual(legacy,[]);self.assertEqual(a.ids,[])
 def make_legacy_transport(self):
  import base64
  import hashlib

  schema_id = 1
  wire = bytes([0]) + schema_id.to_bytes(4, "big") + b"test-payload"

  envelope = {
   "kafka_topic": "icmn.vpm.pain001",
   "kafka_partition": 0,
   "kafka_offset": 1,
   "kafka_timestamp_ms": 1770000000000,
   "schema_registry_id": schema_id,
   "payload_base64": base64.b64encode(wire).decode("ascii"),
   "payload_sha256": hashlib.sha256(wire).hexdigest(),
  }

  return json.dumps(envelope).encode("utf-8")

 def test_legacy_dispatch(self):
  a = Acker()
  calls = []
  data = self.make_legacy_transport()

  result = m.dispatch(
   data, "id", object(), a,
   lambda *args: calls.append(args) or ("k", "CREATED"),
   "store", "decoder"
  )

  self.assertEqual(result, ("LEGACY", ("k", "CREATED")))
  self.assertEqual(len(calls), 1)
  self.assertEqual(calls[0][0], data)
  self.assertEqual(a.ids, [])

 def test_legacy_error_no_ack(self):
  a = Acker()
  data = self.make_legacy_transport()
  calls = []

  def failing_processor(*args):
   calls.append(args)
   raise RuntimeError("simulated downstream failure")

  with self.assertRaisesRegex(
   RuntimeError, "simulated downstream failure"
  ):
   m.dispatch(
    data, "id", object(), a,
    failing_processor, None, None
   )

  self.assertEqual(len(calls), 1)
  self.assertEqual(a.ids, [])

 def test_live_gate(self):
  from types import SimpleNamespace
  a=SimpleNamespace(live=False,max_messages=1,project='p',pubsub_topic='pdm-dev-payments-source-events-v1',subscription='s',bucket='dp-pdm-ai-platform-dev')
  with self.assertRaises(m.OperationalError):m.check_live(a)
 def test_bound_gate(self):
  from types import SimpleNamespace
  import os
  old=os.environ.get('DP_STAGE8_LIVE_APPROVAL');os.environ['DP_STAGE8_LIVE_APPROVAL']='APPROVED-BOUNDED-INGESTION'
  try:
   a=SimpleNamespace(live=True,max_messages=11,project='p',pubsub_topic='pdm-dev-payments-source-events-v1',subscription='s',bucket='dp-pdm-ai-platform-dev')
   with self.assertRaises(m.OperationalError):m.check_live(a)
  finally:
   if old is None:os.environ.pop('DP_STAGE8_LIVE_APPROVAL',None)
   else:os.environ['DP_STAGE8_LIVE_APPROVAL']=old


class SourceDispatchAcceptance(unittest.TestCase):

 def make_source(self, topic="payment-events.retry", payload=b'{}'):
  import base64
  import hashlib

  value = b"" if payload is None else payload

  envelope = {
   "message_type": "payments.source-event.v1",
   "kafka_topic": topic,
   "kafka_partition": 0,
   "kafka_offset": 17,
   "kafka_timestamp_ms": 1770000000000,
   "payload_base64": base64.b64encode(value).decode("ascii"),
   "payload_sha256": hashlib.sha256(value).hexdigest(),
   "schema_registry_id": None,
   "value_is_null": payload is None,
  }

  return json.dumps(envelope).encode("utf-8")

 def dispatch_source(self, data, acker, legacy):
  return m.dispatch(
   data, "ack-17", object(), acker,
   legacy, None, None
  )

 def test_valid_source_dispatch_once(self):
  from unittest.mock import patch

  acker = Acker()
  legacy = []

  with patch.object(m.ENTRY, "store_one", return_value={"created": True}) as store:
   result = self.dispatch_source(
    self.make_source(), acker,
    lambda *args: legacy.append(args)
   )

  self.assertEqual(result, ("SOURCE", {"created": True}))
  store.assert_called_once()
  self.assertEqual(legacy, [])
  self.assertEqual(acker.ids, [])

 def test_cdc_tombstone_dispatch(self):
  from unittest.mock import patch

  acker = Acker()
  legacy = []

  data = self.make_source(
   "cdc.pdm_mdm.mdm.sacco_master_sources",
   None
  )

  with patch.object(m.ENTRY, "store_one", return_value="stored") as store:
   result = self.dispatch_source(
    data, acker,
    lambda *args: legacy.append(args)
   )

  self.assertEqual(result, ("SOURCE", "stored"))
  store.assert_called_once()
  self.assertEqual(legacy, [])
  self.assertEqual(acker.ids, [])

 def test_restricted_topic_rejected(self):
  from unittest.mock import patch

  acker = Acker()
  legacy = []

  data = self.make_source("mdm.crosswalk.restricted")

  with patch.object(m.ENTRY, "store_one") as store:
   with self.assertRaises(m._stage12_contract.RouteRejected):
    self.dispatch_source(
     data, acker,
     lambda *args: legacy.append(args)
    )

  store.assert_not_called()
  self.assertEqual(legacy, [])
  self.assertEqual(acker.ids, [])

 def test_invalid_source_discriminator_no_fallback(self):
  from unittest.mock import patch

  acker = Acker()
  legacy = []

  data = json.loads(self.make_source())
  data["message_type"] = "unexpected.source.v1"

  with patch.object(m.ENTRY, "store_one") as store:
   with self.assertRaises(m.OperationalError):
    self.dispatch_source(
     json.dumps(data).encode(), acker,
     lambda *args: legacy.append(args)
    )

  store.assert_not_called()
  self.assertEqual(legacy, [])
  self.assertEqual(acker.ids, [])

 def test_source_storage_failure_no_ack(self):
  from unittest.mock import patch

  acker = Acker()
  legacy = []

  with patch.object(
   m.ENTRY, "store_one",
   side_effect=RuntimeError("simulated storage failure")
  ) as store:
   with self.assertRaises(RuntimeError):
    self.dispatch_source(
     self.make_source(), acker,
     lambda *args: legacy.append(args)
    )

  store.assert_called_once()
  self.assertEqual(legacy, [])
  self.assertEqual(acker.ids, [])


class SourceStorageIntegrationAcceptance(unittest.TestCase):

 def setUp(self):
  self.acker = Acker()
  self.legacy_calls = []

 def envelope_bytes(self):
  import base64
  import hashlib

  payload = b'{"reason":"integration-test"}'

  envelope = {
   "message_type": "payments.source-event.v1",
   "kafka_topic": "payment-events.retry",
   "kafka_partition": 0,
   "kafka_offset": 17,
   "kafka_timestamp_ms": 1770000000000,
   "payload_base64": base64.b64encode(payload).decode("ascii"),
   "payload_sha256": hashlib.sha256(payload).hexdigest(),
   "schema_registry_id": None,
   "value_is_null": False,
  }

  return json.dumps(envelope).encode("utf-8")

 def dispatch(self, bucket):
  return m.dispatch(
   self.envelope_bytes(),
   "ack-17",
   bucket,
   self.acker,
   lambda *args: self.legacy_calls.append(args),
   None,
   None,
  )

 def test_new_object_ack_once(self):
  class Blob:
   def __init__(self):
    self.data = None
    self.uploads = 0

   def upload_from_string(self, body, **kwargs):
    assert kwargs["if_generation_match"] == 0
    assert kwargs["content_type"] == "application/avro"
    self.uploads += 1
    self.data = body

   def download_as_bytes(self, **kwargs):
    return self.data

  class Bucket:
   def __init__(self):
    self.obj = Blob()
    self.keys = []

   def blob(self, key):
    self.keys.append(key)
    return self.obj

  bucket = Bucket()
  result = self.dispatch(bucket)

  self.assertEqual(result[0], "SOURCE")
  self.assertEqual(result[1]["created"], True)
  self.assertEqual(bucket.obj.uploads, 1)
  self.assertEqual(self.acker.ids, ["ack-17"])
  self.assertEqual(self.legacy_calls, [])
  self.assertTrue(bucket.keys[0].startswith("raw/v1/"))
  self.assertTrue(bucket.keys[0].endswith("/record.avro"))

 def test_duplicate_identical_ack_once(self):
  from google.api_core.exceptions import PreconditionFailed

  class Blob:
   def __init__(self):
    self.data = None

   def upload_from_string(self, body, **kwargs):
    if self.data is None:
     self.data = body
    else:
     raise PreconditionFailed("already exists")

   def download_as_bytes(self, **kwargs):
    return self.data

  class Bucket:
   def __init__(self):
    self.obj = Blob()

   def blob(self, key):
    return self.obj

  bucket = Bucket()

  first = self.dispatch(bucket)
  self.acker.ids.clear()
  second = self.dispatch(bucket)

  self.assertEqual(first[1]["created"], True)
  self.assertEqual(second[1]["created"], False)
  self.assertEqual(self.acker.ids, ["ack-17"])
  self.assertEqual(self.legacy_calls, [])

 def test_conflicting_object_never_ack(self):
  from google.api_core.exceptions import PreconditionFailed

  class Blob:
   def upload_from_string(self, body, **kwargs):
    raise PreconditionFailed("already exists")

   def download_as_bytes(self, **kwargs):
    return b"conflicting-object"

  class Bucket:
   def blob(self, key):
    return Blob()

  with self.assertRaises(m.ENTRY.callbacks.delivery.DeliveryError):
   self.dispatch(Bucket())

  self.assertEqual(self.acker.ids, [])
  self.assertEqual(self.legacy_calls, [])

 def test_storage_failure_never_ack(self):
  class Blob:
   def upload_from_string(self, body, **kwargs):
    raise RuntimeError("simulated GCS failure")

  class Bucket:
   def blob(self, key):
    return Blob()

  with self.assertRaises(RuntimeError):
   self.dispatch(Bucket())

  self.assertEqual(self.acker.ids, [])
  self.assertEqual(self.legacy_calls, [])


from unittest.mock import patch


class SourceLeaseIntegrationAcceptance(unittest.TestCase):

    class Message:
        def __init__(self):
            self.data = b"source-test"
            self.acks = 0

        def ack(self):
            self.acks += 1

    @staticmethod
    def verified(created=True):
        return {
            "key": "raw/v1/test/record.avro",
            "sha256": "a" * 64,
            "created": created,
        }

    def test_success_ack_once(self):
        message = self.Message()
        calls = []

        def store(received, bucket):
            calls.append("stored")
            return self.verified()

        with patch.object(
            m.ENTRY, "store_one_deferred", side_effect=store
        ):
            result = m.process_source_with_lease(
                message,
                object(),
                lambda _: calls.append("renewed"),
            )

        self.assertTrue(result.acknowledged)
        self.assertEqual(result.reason, "acknowledged_after_verification")
        self.assertEqual(message.acks, 1)
        self.assertEqual(calls, ["renewed", "stored"])

    def test_initial_lease_failure_prevents_storage(self):
        message = self.Message()

        def fail_lease(_):
            raise RuntimeError("initial lease unavailable")

        with patch.object(m.ENTRY, "store_one_deferred") as store:
            result = m.process_source_with_lease(
                message, object(), fail_lease
            )

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "initial_lease_failed")
        store.assert_not_called()
        self.assertEqual(message.acks, 0)

    def test_renewal_failure_prevents_ack(self):
        from threading import Event

        message = self.Message()
        renewal_failed = Event()
        attempts = [0]

        def extend(_):
            attempts[0] += 1
            if attempts[0] == 2:
                renewal_failed.set()
                raise RuntimeError("renewal failed")

        def store(received, bucket):
            self.assertTrue(renewal_failed.wait(2))
            return self.verified()

        with patch.object(
            m.ENTRY, "store_one_deferred", side_effect=store
        ):
            result = m.process_source_with_lease(
                message,
                object(),
                extend,
                renewal_interval_seconds=0.005,
            )

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "lease_renewal_failed")
        self.assertEqual(message.acks, 0)

    def test_storage_failure_prevents_ack(self):
        message = self.Message()

        with patch.object(
            m.ENTRY,
            "store_one_deferred",
            side_effect=RuntimeError("GCS unavailable"),
        ):
            result = m.process_source_with_lease(
                message, object(), lambda _: None
            )

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "durable_processing_failed")
        self.assertEqual(message.acks, 0)

    def test_duplicate_verified_ack_once(self):
        message = self.Message()

        with patch.object(
            m.ENTRY,
            "store_one_deferred",
            return_value=self.verified(created=False),
        ):
            result = m.process_source_with_lease(
                message, object(), lambda _: None
            )

        self.assertTrue(result.acknowledged)
        self.assertEqual(message.acks, 1)


class SourceLeaseRealStorageAcceptance(
    SourceStorageIntegrationAcceptance
):
    """Real source callbacks with in-memory GCS and lease renewal."""

    class Message:
        def __init__(self, data, events):
            self.data = data
            self.events = events
            self.acks = 0

        def ack(self):
            self.events.append("ack")
            self.acks += 1

    class Blob:
        def __init__(self, events):
            self.events = events
            self.data = None
            self.uploads = 0
            self.reads = 0
            self.fail_upload = False

        def upload_from_string(self, body, **kwargs):
            from google.api_core.exceptions import PreconditionFailed

            assert kwargs["if_generation_match"] == 0
            assert kwargs["content_type"] == "application/avro"

            self.events.append("upload")
            self.uploads += 1

            if self.fail_upload:
                raise RuntimeError("simulated GCS failure")

            if self.data is not None:
                raise PreconditionFailed("already exists")

            self.data = body

        def download_as_bytes(self, **kwargs):
            self.events.append("readback")
            self.reads += 1
            return self.data

    class Bucket:
        def __init__(self, events):
            self.events = events
            self.obj = SourceLeaseRealStorageAcceptance.Blob(events)
            self.keys = []

        def blob(self, key):
            self.keys.append(key)
            return self.obj

    def prepare(self):
        events = []
        message = self.Message(self.envelope_bytes(), events)
        bucket = self.Bucket(events)
        return message, bucket, events

    def process(self, message, bucket, events):
        return m.process_source_with_lease(
            message,
            bucket,
            lambda _: events.append("renew"),
        )

    def test_lease_new_object_real_readback(self):
        import hashlib

        message, bucket, events = self.prepare()
        result = self.process(message, bucket, events)

        self.assertTrue(result.acknowledged)
        self.assertEqual(message.acks, 1)
        self.assertEqual(bucket.obj.uploads, 1)
        self.assertEqual(bucket.obj.reads, 1)
        self.assertEqual(
            events,
            ["renew", "upload", "readback", "ack"],
        )
        self.assertEqual(len(set(bucket.keys)), 1)
        self.assertTrue(bucket.keys[0].startswith("raw/v1/"))
        self.assertTrue(bucket.keys[0].endswith("/record.avro"))
        self.assertEqual(
            hashlib.sha256(bucket.obj.data).hexdigest(),
            m.ENTRY.callbacks.delivery.adapter.prepare_object(
                __import__("json").loads(message.data)
            )[1] and hashlib.sha256(
                m.ENTRY.callbacks.delivery.adapter.prepare_object(
                    __import__("json").loads(message.data)
                )[1]
            ).hexdigest(),
        )

    def test_lease_duplicate_real_readback(self):
        message, bucket, events = self.prepare()

        first = self.process(message, bucket, events)
        self.assertTrue(first.acknowledged)

        events.clear()
        duplicate = self.Message(message.data, events)
        second = self.process(duplicate, bucket, events)

        self.assertTrue(second.acknowledged)
        self.assertEqual(duplicate.acks, 1)
        self.assertEqual(
            events,
            ["renew", "upload", "readback", "ack"],
        )
        self.assertEqual(bucket.obj.reads, 2)

    def test_lease_conflict_never_ack(self):
        message, bucket, events = self.prepare()
        bucket.obj.data = b"conflicting-object"

        result = self.process(message, bucket, events)

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "durable_processing_failed")
        self.assertEqual(message.acks, 0)
        self.assertIn("readback", events)
        self.assertNotIn("ack", events)

    def test_lease_storage_failure_never_ack(self):
        message, bucket, events = self.prepare()
        bucket.obj.fail_upload = True

        result = self.process(message, bucket, events)

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "durable_processing_failed")
        self.assertEqual(message.acks, 0)
        self.assertNotIn("ack", events)

    def test_lease_renewal_failure_during_real_storage(self):
        from threading import Event

        message, bucket, events = self.prepare()
        renewal_failed = Event()
        attempts = [0]

        original_upload = bucket.obj.upload_from_string

        def delayed_upload(body, **kwargs):
            self.assertTrue(renewal_failed.wait(2))
            return original_upload(body, **kwargs)

        bucket.obj.upload_from_string = delayed_upload

        def extend(_):
            attempts[0] += 1
            if attempts[0] == 2:
                renewal_failed.set()
                raise RuntimeError("lease renewal failed")

        result = m.process_source_with_lease(
            message,
            bucket,
            extend,
            renewal_interval_seconds=0.005,
        )

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "lease_renewal_failed")
        self.assertEqual(message.acks, 0)
        self.assertEqual(bucket.obj.reads, 1)
        self.assertNotIn("ack", events)

