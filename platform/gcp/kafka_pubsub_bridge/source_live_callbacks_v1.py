"""Opt-in client adapters for Stage 3 delivery primitives.

No network access on import. Caller owns client creation, subscriptions,
leases, bounded processing and explicit live authorization. Existing payment
pipeline is not modified. These functions are NOT a deployed subscriber.
"""
from __future__ import annotations
import importlib.util
import json
from pathlib import Path

p=Path(__file__).with_name('source_delivery_v1.py')
spec=importlib.util.spec_from_file_location('dp_source_delivery_v1',p)
delivery=importlib.util.module_from_spec(spec)
spec.loader.exec_module(delivery)

class WiringError(RuntimeError):pass


def publish_kafka_record(record, publisher, pubsub_topic_path, commit):
    """record: mapping with topic/partition/offset/timestamp_ms/value.

    Publisher.publish(topic_path, data) must return a Future whose result()
    confirms the Pub/Sub message ID. Kafka commit is a supplied callback and
    MUST commit only this acknowledged record, synchronously.
    """
    if not isinstance(pubsub_topic_path,str) or not pubsub_topic_path:
        raise WiringError('explicit Pub/Sub topic path required')
    a=delivery.adapter
    envelope=a.make_envelope(record['topic'],record['partition'],record['offset'],
                             record['timestamp_ms'],record['value'],record.get('schema_registry_id'))
    def publish(e):
        payload=json.dumps(e,sort_keys=True,separators=(',',':')).encode('utf-8')
        future=publisher.publish(pubsub_topic_path,payload)
        return future.result()
    return delivery.publish_then_commit(envelope,publish,commit)


def store_pubsub_message(message, bucket, ack=None):
    """Write immutable GCS object and ack only after exact readback.

    GCS blob upload uses generation precondition 0. Precondition failures
    mean the object exists and require exact readback verification. All other
    exceptions propagate without ack. Supply a real Pub/Sub message with
    message.data and message.ack(). No credentials created here.
    """
    import json
    from google.api_core.exceptions import PreconditionFailed
    envelope=json.loads(message.data.decode('utf-8'))
    # Validation occurs inside Stage 3 before any storage operation.
    delivery.adapter.contract.validate_envelope(envelope)
    key,body=delivery.adapter.prepare_object(envelope)
    blob=bucket.blob(key)
    def create(k,b):
        try:
            blob.upload_from_string(b,content_type='application/avro',if_generation_match=0,timeout=20,retry=None)
            return True
        except PreconditionFailed:
            # Existing object still requires byte-identical readback.
            return False
    def readback(k):
        return blob.download_as_bytes(timeout=20,retry=None)
    return delivery.store_then_ack(envelope,create,readback,ack or message.ack)
