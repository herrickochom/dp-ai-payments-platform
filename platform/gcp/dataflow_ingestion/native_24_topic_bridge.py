"""kafka-pubsub-native-bridge: bounded native-wire Kafka -> existing Pub/Sub topic; no source replay.

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
EXPECTED_TOPIC = 'pdm-dev-payments-source-events-v1'

def load_bridge():
    spec = importlib.util.spec_from_file_location('native_existing_bridge', BRIDGE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def validate_topics(path):
    data = json.loads(Path(path).read_text())
    if not isinstance(data, list) or not data:
        raise ValueError('approved inventory must be a non-empty JSON array')
    if any(not isinstance(x, str) or not x.strip() for x in data):
        raise ValueError('approved inventory must contain only non-empty strings')
    if len(set(data)) != len(data):
        raise ValueError('approved inventory must not contain duplicate topics')
    _catalog = _load_canonical_catalog()
    _unknown = sorted(set(data) - set(_catalog))
    if _unknown:
        raise ValueError('approved inventory contains topics not in canonical catalog: '
                         + ', '.join(_unknown))
    return data


def _load_canonical_catalog():
    import os
    import yaml as _yaml
    _here = os.path.dirname(os.path.abspath(__file__))
    _path = os.path.normpath(
        os.path.join(_here, '..', '..', '..', 'platform', 'kafka', 'topics.yaml'))
    if not os.path.isfile(_path):
        raise ValueError('canonical topic catalog not found at ' + _path)
    with open(_path) as _f:
        _doc = _yaml.safe_load(_f)
    if isinstance(_doc, dict):
        _topics = _doc.get('topics') or _doc.get('kafka_topics') or []
    elif isinstance(_doc, list):
        _topics = _doc
    else:
        raise ValueError('canonical catalog must be a list or mapping')
    _names = []
    for _t in _topics:
        if isinstance(_t, str):
            _names.append(_t.strip())
        elif isinstance(_t, dict):
            _n = _t.get('name') or _t.get('topic') or _t.get('id')
            if not isinstance(_n, str) or not _n.strip():
                raise ValueError('catalog entry missing name: ' + repr(_t))
            _names.append(_n.strip())
        else:
            raise ValueError('catalog entries must be strings or dicts')
    if len(_names) != len(set(_names)):
        raise ValueError('catalog has duplicate names')
    return _names



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
    p.add_argument('--bootstrap', default=os.getenv('PDM_KAFKA_BOOTSTRAP', ''))
    p.add_argument('--registry', default=os.getenv('PDM_SCHEMA_REGISTRY_URL', ''))
    p.add_argument('--project', default=os.getenv('PDM_GCP_PROJECT', ''))
    p.add_argument('--pubsub-topic', default=EXPECTED_TOPIC)
    p.add_argument('--group', default='gcp-native-source-bridge')
    p.add_argument('--max-messages', type=int, default=10)
    run(p.parse_args())

if __name__ == '__main__':
    main()
