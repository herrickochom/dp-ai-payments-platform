"""Kafka -> Pub/Sub proof-of-concept, strict six-field contract.

Run explicitly with --live after authorisation and source-topic mapping review.
No consumer offsets are committed before a successful Pub/Sub publish future.
"""
import argparse
import json
import os
from dataclasses import dataclass
from typing import Any, Mapping

FIELDS = ('event_id', 'payment_id', 'event_type', 'amount_minor', 'currency', 'event_timestamp')

class ContractError(ValueError):
    pass


def validate_event(event: Mapping[str, Any]) -> dict:
    """No inferred values; reject extra fields and incompatible source records."""
    if not isinstance(event, dict):
        raise ContractError('event must be a JSON object')
    if set(event) != set(FIELDS):
        raise ContractError('source event fields do not match approved Pub/Sub contract')
    for field in FIELDS:
        if field == 'amount_minor':
            if type(event[field]) is not int or not -(2**63) <= event[field] < 2**63:
                raise ContractError('amount_minor must be a signed 64-bit integer')
        elif not isinstance(event[field], str) or not event[field].strip():
            raise ContractError(f'{field} must be a nonblank string')
    return {field: event[field] for field in FIELDS}


def encode_event(event: Mapping[str, Any]) -> bytes:
    return json.dumps(validate_event(event), separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def provenance(message: Any, source_system: str) -> dict:
    if not source_system or not source_system.strip():
        raise ContractError('governed source_system is required')
    return {
        'source_system': source_system,
        'kafka_topic': message.topic(),
        'kafka_partition': str(message.partition()),
        'kafka_offset': str(message.offset()),
    }


def publish_then_commit(consumer: Any, publisher: Any, topic_path: str, message: Any,
                        event: Mapping[str, Any], source_system: str) -> str:
    """Acknowledge Pub/Sub publish before synchronously committing Kafka offset."""
    payload = encode_event(event)
    attrs = provenance(message, source_system)
    future = publisher.publish(topic_path, payload, **attrs)
    pubsub_message_id = future.result(timeout=30)
    consumer.commit(message=message, asynchronous=False)
    return pubsub_message_id


def run_live(args: Any) -> None:
    # Imports delayed: offline tests never need cloud or Kafka packages.
    from confluent_kafka import Consumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import SerializationContext, MessageField
    from google.cloud import pubsub_v1

    if not args.schema_registry_url or not args.kafka_bootstrap or not args.source_topic:
        raise SystemExit('FAIL: Kafka bootstrap, topic and Schema Registry URL required')
    if not args.source_system or not args.source_system.strip():
        raise SystemExit('FAIL: governed source-system name required')
    if not args.project or not args.topic:
        raise SystemExit('FAIL: Pub/Sub project and topic required')
    if args.topic != 'pdm-payments-poc-v1':
        raise SystemExit('FAIL: POC restricted to isolated Pub/Sub topic')
    registry = SchemaRegistryClient({'url': args.schema_registry_url})
    deserializer = AvroDeserializer(registry)
    consumer = Consumer({
        'bootstrap.servers': args.kafka_bootstrap,
        'group.id': args.group,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest',
    })
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)
    consumer.subscribe([args.source_topic])
    completed = 0
    try:
        while completed < args.max_messages:
            message = consumer.poll(5.0)
            if message is None:
                print('No more messages in current poll; exiting without changes to offsets')
                break
            if message.error():
                raise RuntimeError('Kafka consumer error; no offset committed')
            event = deserializer(message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            # Unsupported schemas are deliberately fatal: do not skip or commit.
            validate_event(event)
            publish_then_commit(consumer, publisher, topic_path, message, event, args.source_system)
            completed += 1
            print(f'Published and committed {completed} event(s)')
    finally:
        consumer.close()


def main() -> None:
    p = argparse.ArgumentParser(description='Guarded Kafka to Pub/Sub POC')
    p.add_argument('--live', action='store_true', help='Explicitly authorise live consumption and publishing')
    p.add_argument('--kafka-bootstrap', default=os.getenv('KP_KAFKA_BOOTSTRAP', ''))
    p.add_argument('--schema-registry-url', default=os.getenv('KP_SCHEMA_REGISTRY_URL', ''))
    p.add_argument('--source-topic', default=os.getenv('KP_SOURCE_TOPIC', ''))
    p.add_argument('--source-system', default=os.getenv('KP_SOURCE_SYSTEM', ''))
    p.add_argument('--project', default='project-e48f194e-f44f-458d-b4f')
    p.add_argument('--topic', default='pdm-payments-poc-v1')
    p.add_argument('--group', default='gcp-kp001d-poc-v1')
    p.add_argument('--max-messages', type=int, default=1)
    args = p.parse_args()
    if not args.live:
        print('SAFE: bridge installed; no Kafka consumption or cloud publishing. Use offline tests first.')
        return
    if not 1 <= args.max_messages <= 10:
        raise SystemExit('FAIL: bounded POC requires 1..10 messages')
    run_live(args)

if __name__ == '__main__':
    main()


def encode_wire_envelope(message):
    """Raw v2 transport preserves original Confluent wire bytes and Kafka provenance."""
    import base64
    import hashlib
    wire=message.value()
    if type(wire) is not bytes or len(wire)<=5 or wire[0]!=0:
        raise ContractError('Confluent Avro wire bytes required')
    stamp=message.timestamp()
    if not stamp or type(stamp[1]) is not int or stamp[1]<0:
        raise ContractError('Kafka timestamp required')
    schema_id=int.from_bytes(wire[1:5],'big')
    if schema_id<1 or type(message.partition()) is not int or message.partition()<0 or type(message.offset()) is not int or message.offset()<0:
        raise ContractError('invalid Kafka provenance')
    obj={'kafka_topic':message.topic(),'kafka_partition':message.partition(),
         'kafka_offset':message.offset(),'kafka_timestamp_ms':stamp[1],
         'schema_registry_id':schema_id,'payload_base64':base64.b64encode(wire).decode('ascii'),
         'payload_sha256':hashlib.sha256(wire).hexdigest()}
    return json.dumps(obj,sort_keys=True,separators=(',',':')).encode('utf-8')

def publish_wire_then_commit(consumer,publisher,topic_path,message):
    """Commit Kafka only after successful Pub/Sub publish future."""
    data=encode_wire_envelope(message)
    message_id=publisher.publish(topic_path,data).result(timeout=30)
    consumer.commit(message=message,asynchronous=False)
    return message_id
