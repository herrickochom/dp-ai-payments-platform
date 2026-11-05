"""GCP-KP-001I native-event transport; no business-field inference.

At-least-once: Pub/Sub publish acknowledgement precedes Kafka synchronous commit.
On failure, do not commit. Restart with a stable consumer group to replay.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import struct
from dataclasses import dataclass
from typing import Callable

MAGIC = 0
ENVELOPE_VERSION = '1'


class BridgeError(RuntimeError):
    pass


def decode_header(payload: bytes) -> int:
    if not isinstance(payload, bytes) or len(payload) < 6 or payload[0] != MAGIC:
        raise BridgeError('invalid Confluent Avro wire header')
    schema_id = struct.unpack('>I', payload[1:5])[0]
    if schema_id < 1:
        raise BridgeError('invalid Schema Registry schema ID')
    return schema_id


def validate_avro(payload: bytes, schema_text: str) -> None:
    from fastavro import parse_schema, schemaless_reader
    try:
        schema = parse_schema(json.loads(schema_text))
        stream = io.BytesIO(payload[5:])
        record = schemaless_reader(stream, schema)
        if stream.read(1) or not isinstance(record, dict):
            raise BridgeError('Avro trailing bytes or invalid record')
    except BridgeError:
        raise
    except Exception as exc:
        raise BridgeError('Avro payload failed schema validation') from exc


def envelope_bytes(*, payload: bytes, topic: str, partition: int, offset: int,
                   schema_id: int, schema_subject: str) -> bytes:
    if not topic or not schema_subject or partition < 0 or offset < 0:
        raise BridgeError('invalid Kafka provenance')
    if decode_header(payload) != schema_id:
        raise BridgeError('schema ID mismatch')
    obj = {
        'envelope_version': ENVELOPE_VERSION,
        'encoding': 'confluent-avro-base64',
        'kafka_topic': topic,
        'kafka_partition': partition,
        'kafka_offset': offset,
        'schema_registry_id': schema_id,
        'schema_registry_subject': schema_subject,
        'payload_sha256': hashlib.sha256(payload).hexdigest(),
        'payload_base64': base64.b64encode(payload).decode('ascii'),
    }
    return json.dumps(obj, sort_keys=True, separators=(',', ':')).encode('utf-8')


def process_one(*, message, get_schema: Callable[[int], str], publish: Callable[[bytes, dict], object],
                commit: Callable[[object], None], subject: str, expected_schema_id: int | None = None) -> None:
    payload = message.value()
    schema_id = decode_header(payload)
    if expected_schema_id is not None and schema_id != expected_schema_id:
        raise BridgeError('unexpected source schema ID')
    schema_text = get_schema(schema_id)
    validate_avro(payload, schema_text)
    topic, partition, offset = message.topic(), message.partition(), message.offset()
    body = envelope_bytes(payload=payload, topic=topic, partition=partition, offset=offset,
                          schema_id=schema_id, schema_subject=subject)
    attrs = {
        'envelope_version': ENVELOPE_VERSION,
        'source_topic': topic,
        'source_partition': str(partition),
        'source_offset': str(offset),
        'source_schema_id': str(schema_id),
        'source_schema_subject': subject,
    }
    publish(body, attrs)  # Must block until Pub/Sub acknowledges.
    commit(message)       # Synchronous commit only after acknowledgement.


def run_live(args):
    try:
        from confluent_kafka import Consumer, TopicPartition
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from google.cloud import pubsub_v1
    except ImportError as exc:
        raise BridgeError('install confluent-kafka, google-cloud-pubsub, fastavro in venv') from exc
    if os.getenv('GCP_KP001I_APPROVAL') != 'LIVE-BOUNDED-PILOT':
        raise BridgeError('live pilot requires GCP_KP001I_APPROVAL=LIVE-BOUNDED-PILOT')
    if not args.schema_registry_url or not args.bootstrap or not args.project or not args.topic_id:
        raise BridgeError('missing live connection settings')
    if args.max_messages < 1 or args.max_messages > 100:
        raise BridgeError('bounded pilot must use 1..100 messages')
    if args.topic_id == 'pdm-payments-poc-v1':
        raise BridgeError('cannot publish native envelopes to six-field schema topic')
    registry = SchemaRegistryClient({'url': args.schema_registry_url})
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic_id)
    # Fail closed if topic missing; never create cloud resources automatically.
    publisher.get_topic(request={'topic': topic_path})
    consumer = Consumer({
        'bootstrap.servers': args.bootstrap,
        'group.id': args.group,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest',
        'enable.auto.offset.store': False,
    })
    seen = 0
    idle = 0
    consumer.subscribe([args.source_topic])
    try:
        while seen < args.max_messages and idle < 10:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            idle = 0
            if msg.error():
                raise BridgeError('Kafka consumer error')
            if msg.topic() != args.source_topic:
                raise BridgeError('unexpected Kafka source topic')
            def schema_lookup(schema_id):
                schema = registry.get_schema(schema_id)
                return schema.schema_str
            def publish(body, attrs):
                # result() blocks for server acknowledgement or raises; no commit on error.
                return publisher.publish(topic_path, body, **attrs).result(timeout=30)
            def commit(message):
                # Explicit next offset, synchronous broker acknowledgement.
                consumer.commit(offsets=[TopicPartition(message.topic(), message.partition(), message.offset() + 1)], asynchronous=False)
            process_one(message=msg, get_schema=schema_lookup, publish=publish,
                        commit=commit, subject=args.subject, expected_schema_id=args.schema_id)
            seen += 1
            print(f'ACK_AND_COMMIT topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}')
    finally:
        consumer.close()
    print(f'PILOT_COMPLETE acknowledged_and_committed={seen}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--bootstrap', default=os.getenv('KAFKA_BOOTSTRAP_SERVERS', ''))
    parser.add_argument('--schema-registry-url', default=os.getenv('SCHEMA_REGISTRY_URL', ''))
    parser.add_argument('--project', default='project-e48f194e-f44f-458d-b4f')
    parser.add_argument('--topic-id', default='pdm-native-events-poc-v1')
    parser.add_argument('--source-topic', required=True)
    parser.add_argument('--subject', required=True)
    parser.add_argument('--schema-id', type=int, required=True)
    parser.add_argument('--group', default='gcp-kp001i-native-bridge-pilot-v1')
    parser.add_argument('--max-messages', type=int, default=10)
    args = parser.parse_args()
    if not args.live:
        raise SystemExit('No live action requested. Use --live and explicit approval for bounded pilot.')
    run_live(args)


if __name__ == '__main__':
    main()
