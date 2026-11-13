"""Bounded, opt-in orchestration for Stage 4 client callbacks.

This module does NOT replace existing bridge/subscriber entrypoints, create
clients, manage leases, or start live ingestion. An operator must provide
actual Kafka, Pub/Sub and GCS clients and explicitly bounded records/messages.
Legacy payment topics are intentionally delegated to their existing writer.
"""
from __future__ import annotations
import importlib.util
from pathlib import Path

p=Path(__file__).with_name('source_live_callbacks_v1.py')
spec=importlib.util.spec_from_file_location('dp_stage4_callbacks',p)
callbacks=importlib.util.module_from_spec(spec)
spec.loader.exec_module(callbacks)

class EntrypointError(RuntimeError):pass

def _record_from_kafka(message):
    """Adapt confluent_kafka.Message without logging payload or credentials."""
    error=message.error()
    if error is not None:raise EntrypointError('Kafka message has error')
    ts_type,ts_ms=message.timestamp()
    if type(ts_ms) is not int or ts_ms<0:
        raise EntrypointError('Kafka timestamp unavailable')
    topic=message.topic()
    partition=message.partition()
    offset=message.offset()
    value=message.value()
    if not isinstance(topic,str) or type(partition) is not int or type(offset) is not int:
        raise EntrypointError('Invalid Kafka coordinates')
    if value is not None and not isinstance(value,bytes):
        raise EntrypointError('Kafka value must be bytes or null')
    schema_id=int.from_bytes(value[1:5],'big') if value is not None and len(value)>=5 and value[0]==0 else None
    return dict(topic=topic,partition=partition,offset=offset,
                timestamp_ms=ts_ms,value=value,schema_registry_id=schema_id)

def publish_one(message,consumer,publisher,topic_path):
    """Publish a single nonlegacy eligible Kafka record; sync commit on ack.

    The caller MUST use a dedicated Kafka consumer group, disable auto commit,
    and ensure no other processing of this partition is in flight.
    """
    record=_record_from_kafka(message)
    def commit():
        # Explicit message commit, never a broad asynchronous offset commit.
        result=consumer.commit(message=message,asynchronous=False)
        if not isinstance(result, (list, tuple)) or len(result) != 1:
            raise EntrypointError('Kafka commit confirmation missing or ambiguous')
        tp=result[0]
        if getattr(tp,'err',None) is not None:
            raise EntrypointError('Kafka commit error')
        expected=(record['topic'],record['partition'],record['offset']+1)
        actual=(getattr(tp,'topic',None),
                getattr(tp,'partition',None),
                getattr(tp,'offset',None))
        if actual != expected:
            raise EntrypointError('Kafka commit coordinates mismatch')
    return callbacks.publish_kafka_record(record,publisher,topic_path,commit)

def store_one(pubsub_message,bucket):
    """Store and read back before Pub/Sub ack. Errors propagate unacked."""
    return callbacks.store_pubsub_message(pubsub_message,bucket)

def store_one_deferred(pubsub_message, bucket):
    """Verify immutable storage without acknowledging Pub/Sub.

    The caller must own final acknowledgement, normally through
    the existing lease guard after its final renewal check.
    """
    return callbacks.store_pubsub_message(
        pubsub_message,
        bucket,
        ack=lambda: None,
    )


def bounded_publish(messages,consumer,publisher,topic_path,max_messages=1):
    if type(max_messages) is not int or not 1<=max_messages<=10:
        raise EntrypointError('Bound must be between 1 and 10')
    count=0
    for message in messages:
        if count>=max_messages:break
        publish_one(message,consumer,publisher,topic_path)
        count+=1
    return count
