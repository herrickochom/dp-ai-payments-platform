"""Opt-in bounded runtime for governed Kafka source records and Pub/Sub Raw v1 ingestion.
No network calls on import. Existing payment writers remain the legacy path.
"""
from __future__ import annotations
import argparse,importlib.util,json,os
import sys
from threading import Lock
from pathlib import Path

class OperationalError(RuntimeError):pass

def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path)
 mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
 return mod

BRIDGE=Path(__file__).resolve().parent
ROOT=BRIDGE.parents[2]
ENTRY=load(BRIDGE/'source_bounded_entrypoint_v1.py','dp_stage5_entry')


_stage12_contract_path = Path(__file__).with_name(
    "source_envelope_v1.py"
)
_stage12_spec = importlib.util.spec_from_file_location(
    "stage12_source_contract",
    _stage12_contract_path,
)
_stage12_contract = importlib.util.module_from_spec(
    _stage12_spec
)
_stage12_spec.loader.exec_module(_stage12_contract)

def is_source_envelope(data):
    """Recognise explicitly versioned source envelopes."""
    try:
        candidate = json.loads(data.decode("utf-8"))
    except (UnicodeError, ValueError, AttributeError):
        return False

    return (
        isinstance(candidate, dict)
        and candidate.get("message_type")
            == "payments.source-event.v1"
    )

class Received:
 def __init__(self,data,ack):self.data=data;self._ack=ack
 def ack(self):return self._ack()

def dispatch(
    data,
    ack_id,
    bucket,
    acker,
    legacy_process,
    legacy_store,
    legacy_decoder,
    *,
    source_store=None,
):
    """Classify and validate source and legacy transport before side effects."""
    try:
        candidate = json.loads(data.decode("utf-8"))
    except (UnicodeError, ValueError, AttributeError) as exc:
        raise OperationalError("invalid Pub/Sub transport JSON") from exc

    if not isinstance(candidate, dict):
        raise OperationalError("Pub/Sub transport must be an object")

    legacy_fields = {
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp_ms",
        "schema_registry_id",
        "payload_base64",
        "payload_sha256",
    }

    if "message_type" in candidate:
        if not is_source_envelope(data):
            raise OperationalError("invalid source discriminator")

        _stage12_contract.validate_envelope(candidate)

        message = Received(data, lambda: acker.ack(ack_id))
        store = ENTRY.store_one if source_store is None else source_store
        return "SOURCE", store(message, bucket)

    if set(candidate) != legacy_fields:
        raise OperationalError("invalid or ambiguous legacy envelope")

    # Validate the existing legacy transport contract before side effects.
    legacy_contract = load(
        ROOT / "platform/gcp/pubsub_raw_ingestion/raw_v1.py",
        "dp_legacy_transport_contract",
    )
    try:
        legacy_contract.transport(data)
    except legacy_contract.ContractError as exc:
        raise OperationalError("invalid legacy transport") from exc

    return "LEGACY", legacy_process(
        data,
        ack_id,
        legacy_store,
        acker,
        legacy_decoder,
    )

_source_guard_init_lock = Lock()


def get_source_lease_guard():
    """Return one fully initialised lease guard per runtime process."""
    guard_name = "dp_source_lease_guard"
    guard_path = ROOT / "platform/gcp/pubsub_raw_ingestion/lease_guard.py"

    with _source_guard_init_lock:
        guard = sys.modules.get(guard_name)

        if guard is not None:
            if not callable(
                getattr(guard, "process_with_renewal", None)
            ):
                raise OperationalError(
                    "cached lease guard is not fully initialised"
                )
            if Path(getattr(guard, "__file__", "")).resolve() != guard_path.resolve():
                raise OperationalError("cached lease guard path mismatch")
            return guard

        spec = importlib.util.spec_from_file_location(
            guard_name, guard_path
        )
        if spec is None or spec.loader is None:
            raise OperationalError("lease guard module unavailable")

        guard = importlib.util.module_from_spec(spec)

        # Dataclasses require the module to be registered during execution.
        # The initialisation lock prevents concurrent callers of this loader
        # from observing the partially initialised module.
        sys.modules[guard_name] = guard

        try:
            spec.loader.exec_module(guard)
        except BaseException:
            if sys.modules.get(guard_name) is guard:
                sys.modules.pop(guard_name, None)
            raise

        return guard


def process_source_with_lease(
    message,
    bucket,
    extend_lease,
    *,
    renewal_interval_seconds=5.0,
    max_processing_seconds=None,
):
    """Verify source storage under an existing Pub/Sub renewal guard.

    The lease guard exclusively owns acknowledgement.
    The caller supplies the message, bucket and deadline-extension callback.
    """
    guard = get_source_lease_guard()

    def durable_verify(received):
        result = ENTRY.store_one_deferred(received, bucket)
        return (
            isinstance(result, dict)
            and isinstance(result.get("key"), str)
            and bool(result["key"])
            and isinstance(result.get("sha256"), str)
            and len(result["sha256"]) == 64
            and type(result.get("created")) is bool
        )

    return guard.process_with_renewal(
        message,
        durable_verify,
        extend_lease,
        renewal_interval_seconds=renewal_interval_seconds,
        max_processing_seconds=max_processing_seconds,
    )


def check_live(args):
 if not args.live:raise OperationalError('live operation requires --live')
 if os.environ.get('DP_STAGE8_LIVE_APPROVAL')!='APPROVED-BOUNDED-INGESTION':
  raise OperationalError('live operation requires DP_STAGE8_LIVE_APPROVAL=APPROVED-BOUNDED-INGESTION')
 if type(args.max_messages) is not int or not 1<=args.max_messages<=10:
  raise OperationalError('max-messages must be 1..10')
 if not all((args.project,args.pubsub_topic,args.subscription,args.bucket)):
  raise OperationalError('explicit cloud project/topic/subscription/bucket required')
 if args.pubsub_topic!='pdm-dev-payments-source-events-v1':
  raise OperationalError('unexpected Pub/Sub topic')
 if args.bucket!='dp-pdm-ai-platform-dev':raise OperationalError('unexpected GCS bucket')

def run_bridge(args):
 check_live(args)
 if not all((args.kafka_bootstrap,args.kafka_topic,args.kafka_group)):
  raise OperationalError('Kafka connection, topic and dedicated group required')
 # Reject unauthorised topics before opening network clients.
 delivery = ENTRY.callbacks.delivery
 route = delivery.adapter.contract.route_for(args.kafka_topic)

 if args.kafka_topic in delivery.adapter.LEGACY_TOPICS:
  raise OperationalError(
   'legacy topic belongs to existing ingestion path'
  )

 if (
  route['status'] != 'contract_only'
  or args.kafka_topic not in delivery.APPROVED_SOURCE_TOPICS
 ):
  raise OperationalError(
   'source publishing not explicitly authorised'
  )

 from confluent_kafka import Consumer
 from google.cloud import pubsub_v1
 from confluent_kafka import TopicPartition
 publisher=pubsub_v1.PublisherClient()
 topic_path=publisher.topic_path(args.project,args.pubsub_topic)
 publisher.get_topic(request={'topic':topic_path})
 consumer=Consumer({'bootstrap.servers':args.kafka_bootstrap,'group.id':args.kafka_group,
                    'enable.auto.commit':False,'enable.auto.offset.store':False,
                    'auto.offset.reset':'latest'})
 consumer.subscribe([args.kafka_topic]);done=0
 try:
  for _ in range(args.max_messages):
   msg=consumer.poll(5.0)
   if msg is None:break
   if msg.error():raise OperationalError('Kafka poll error; offset uncommitted')
   if msg.topic()!=args.kafka_topic:raise OperationalError('unexpected Kafka topic')
   # Source contract blocks legacy, restricted, and unverified topics before publishing.
   ENTRY.publish_one(msg,consumer,publisher,topic_path)
   done+=1
 finally:consumer.close()
 return done

_subscriber_routing_init_lock = Lock()


def _load_subscriber_routing():
    """Load the routing contract with dataclass-safe registration."""
    name = "dp_source_subscriber_routing_v1"
    path = ROOT / (
        "platform/gcp/kafka_pubsub_bridge/"
        "source_subscriber_routing_v1.py"
    )

    with _subscriber_routing_init_lock:
        module = sys.modules.get(name)

        if module is not None:
            if Path(getattr(module, "__file__", "")).resolve() != path.resolve():
                raise OperationalError("subscriber routing module path mismatch")
            if not callable(getattr(module, "route_received", None)):
                raise OperationalError("subscriber routing module incomplete")
            return module

        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise OperationalError("subscriber routing module unavailable")

        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module

        try:
            spec.loader.exec_module(module)
        except BaseException:
            if sys.modules.get(name) is module:
                sys.modules.pop(name, None)
            raise

        return module


def process_legacy_with_lease(
    message,
    data,
    ack_id,
    store,
    decoder,
    legacy_process,
    extend_lease,
    *,
    renewal_interval_seconds=5.0,
    max_processing_seconds=None,
):
    """Run existing legacy durable processing; defer actual Pub/Sub ack."""
    guard = get_source_lease_guard()

    class DeferredAcknowledger:
        def __init__(self):
            self.requests = 0

        def ack(self, supplied_ack_id):
            if supplied_ack_id != ack_id:
                raise OperationalError("legacy acknowledgement ID mismatch")
            self.requests += 1
            if self.requests != 1:
                raise OperationalError("multiple legacy acknowledgement requests")

    deferred = DeferredAcknowledger()

    def durable_verify(received):
        result = legacy_process(
            data,
            ack_id,
            store,
            deferred,
            decoder,
        )
        return (
            deferred.requests == 1
            and isinstance(result, tuple)
            and len(result) == 2
            and isinstance(result[0], str)
            and bool(result[0])
            and result[1] in (
                "CREATED",
                "ALREADY_PRESENT_IDENTICAL",
            )
        )

    return guard.process_with_renewal(
        message,
        durable_verify,
        extend_lease,
        renewal_interval_seconds=renewal_interval_seconds,
        max_processing_seconds=max_processing_seconds,
    )


def route_pulled_message(
    *,
    received,
    subscriber,
    subscription,
    bucket,
    legacy_acker,
    legacy_process,
    legacy_store,
    legacy_decoder,
    renewal_interval_seconds=5.0,
    max_processing_seconds=60.0,
):
    """Route pulled SOURCE and LEGACY messages with renewable leases."""
    if (
        isinstance(max_processing_seconds, bool)
        or not isinstance(max_processing_seconds, (int, float))
        or not 0 < max_processing_seconds < float("inf")
    ):
        raise OperationalError("finite positive processing budget required")

    routing = _load_subscriber_routing()

    acknowledger_module = load(
        ROOT / "platform/gcp/pubsub_raw_ingestion/"
        "source_acknowledger_v1.py",
        "dp_source_acknowledger_v1",
    )
    extender_module = load(
        ROOT / "platform/gcp/pubsub_raw_ingestion/"
        "source_lease_extension_v1.py",
        "dp_source_lease_extension_v1",
    )

    source_acker = acknowledger_module.SourceAcknowledger(
        subscriber, subscription, rpc_timeout_seconds=5.0,
    )
    extender = extender_module.SourceLeaseExtender(
        subscriber,
        subscription,
        received.ack_id,
        extension_seconds=30,
        rpc_timeout_seconds=5.0,
    )

    return routing.route_received(
        data=received.message.data,
        ack_id=received.ack_id,
        bucket=bucket,
        acker=source_acker,
        legacy_process=legacy_process,
        legacy_store=legacy_store,
        legacy_decoder=legacy_decoder,
        dispatch=dispatch,
        process_source_with_lease=process_source_with_lease,
        process_legacy_with_lease=process_legacy_with_lease,
        extend_lease=extender.extend,
        renewal_interval_seconds=renewal_interval_seconds,
        max_processing_seconds=max_processing_seconds,
    )


def run_subscriber(args):
 check_live(args)
 if not args.schema_registry_url:raise OperationalError('legacy schema registry URL required')
 from google.cloud import pubsub_v1,storage
 from confluent_kafka.schema_registry import SchemaRegistryClient
 cloud=load(ROOT/'platform/gcp/pubsub_raw_ingestion/raw_v1_cloud.py','dp_legacy_cloud')
 subscriber=pubsub_v1.SubscriberClient()
 path=subscriber.subscription_path(args.project,args.subscription)
 bucket=storage.Client(project=args.project).lookup_bucket(args.bucket)
 if bucket is None or bucket.name!=args.bucket:raise OperationalError('approved GCS bucket unavailable')
 legacy_store=cloud.GCSConditionalStore(bucket)
 acker=cloud.PubSubAcknowledger(subscriber,path)
 decoder=cloud.RegistryDecoder(SchemaRegistryClient({'url':args.schema_registry_url}))
 done=0
 try:
  for _ in range(args.max_messages):
   response=subscriber.pull(request={'subscription':path,'max_messages':1},timeout=20)
   if not response.received_messages:break
   received=response.received_messages[0]
   result = route_pulled_message(
       received=received,
       subscriber=subscriber,
       subscription=path,
       bucket=bucket,
       legacy_acker=acker,
       legacy_process=cloud.process_message,
       legacy_store=legacy_store,
       legacy_decoder=decoder,
   )
   if type(getattr(result, 'acknowledged', None)) is not bool:
     raise OperationalError('invalid subscriber acknowledgement result')
   if not result.acknowledged:
     raise OperationalError(
       f'subscriber delivery unacknowledged: '
       f'route={result.route!r}, reason={result.reason!r}'
     )
   done+=1
 finally:subscriber.close()
 return done

def main():
 p=argparse.ArgumentParser()
 p.add_argument('--mode',choices=['bridge','subscriber'],required=True)
 p.add_argument('--live',action='store_true')
 p.add_argument('--max-messages',type=int,default=1)
 p.add_argument('--project');p.add_argument('--pubsub-topic');p.add_argument('--subscription')
 p.add_argument('--bucket');p.add_argument('--schema-registry-url')
 p.add_argument('--kafka-bootstrap');p.add_argument('--kafka-topic');p.add_argument('--kafka-group')
 args=p.parse_args()
 check_live(args)
 print('LIVE_COMPLETE processed=',run_bridge(args) if args.mode=='bridge' else run_subscriber(args))
if __name__=='__main__':main()
