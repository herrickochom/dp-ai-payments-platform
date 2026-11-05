"""KP-010C: bounded native-wire Kafka -> existing Pub/Sub topic; no source replay.

This is an additive bridge only. Historical source publishing remains blocked until
source identity and provenance are authoritatively reconciled.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BRIDGE = ROOT / 'platform/gcp/kafka_pubsub_bridge/bridge.py'
EXPECTED_TOPIC = 'pdm-native-events-poc-v1'

def load_bridge():
    spec = importlib.util.spec_from_file_location('kp010_existing_bridge', BRIDGE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def validate_topics(path):
    data = json.loads(Path(path).read_text())
    if not isinstance(data, list) or len(data) != 24 or any(not isinstance(x, str) or not x.strip() for x in data) or len(set(data)) != 24:
        raise ValueError('approved inventory must be a JSON array of exactly 24 distinct topic names')
    return data

def run(args):
    topics = validate_topics(args.approved_topics)
    if not args.live:
        print('SAFE: validated 24-topic inventory; no Kafka or cloud calls')
        return 0
    if not args.approve_pubsub_cost or not args.bootstrap or not args.registry or not args.project:
        raise SystemExit('STOP: explicit live cost approval, Kafka, registry and project required')
    if args.pubsub_topic != EXPECTED_TOPIC or not 1 <= args.max_messages <= 100:
        raise SystemExit('STOP: destination or bound not approved')
    from confluent_kafka import Consumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from google.cloud import pubsub_v1
    registry = SchemaRegistryClient({'url': args.registry})
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.pubsub_topic)
    consumer = Consumer({'bootstrap.servers': args.bootstrap, 'group.id': args.group,
                         'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    bridge = load_bridge()
    processed = 0
    seen = set()
    try:
        consumer.subscribe(topics)
        while processed < args.max_messages:
            msg = consumer.poll(5)
            if msg is None:
                break
            if msg.error():
                raise RuntimeError(f'Kafka poll failed: {msg.error()}')
            if msg.topic() not in topics:
                raise RuntimeError('unapproved source topic')
            wire = msg.value()
            if not isinstance(wire, bytes) or len(wire) <= 5 or wire[0] != 0:
                raise RuntimeError('not a Confluent Avro wire message')
            registry.get_schema(int.from_bytes(wire[1:5], 'big'))
            identity = (msg.topic(), msg.partition(), msg.offset())
            if identity in seen:
                raise RuntimeError('duplicate coordinates within execution')
            seen.add(identity)
            pubsub_id = bridge.publish_wire_then_commit(consumer, publisher, topic_path, msg)
            print(json.dumps({'topic': msg.topic(), 'partition': msg.partition(),
                              'offset': msg.offset(), 'pubsub_message_id': pubsub_id}))
            processed += 1
    finally:
        consumer.close()
    print(json.dumps({'published': processed, 'requested_max': args.max_messages,
                      'topics_configured': len(topics), 'end_to_end_verified': False}))
    if processed == 0:
        raise SystemExit('INCOMPLETE: zero records available; no delivery acceptance')
    return processed

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--approved-topics', required=True, help='Previously verified 24-topic JSON array')
    p.add_argument('--live', action='store_true')
    p.add_argument('--approve-pubsub-cost', action='store_true')
    p.add_argument('--bootstrap', default=os.getenv('KP_KAFKA_BOOTSTRAP', ''))
    p.add_argument('--registry', default=os.getenv('KP_SCHEMA_REGISTRY_URL', ''))
    p.add_argument('--project', default=os.getenv('KP_GCP_PROJECT', ''))
    p.add_argument('--pubsub-topic', default=EXPECTED_TOPIC)
    p.add_argument('--group', default='gcp-kp010c-native-bridge')
    p.add_argument('--max-messages', type=int, default=10)
    run(p.parse_args())

if __name__ == '__main__':
    main()
