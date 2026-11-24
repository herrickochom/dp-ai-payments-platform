#!/usr/bin/env python3
"""Read-only Kafka -> GCS Raw Avro record/volume reconciliation."""
import hashlib
import io
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import fastavro
import yaml
from confluent_kafka import Consumer, TopicPartition
from google.cloud import storage

ROOT = Path.cwd()
PROJECT = 'project-e48f194e-f44f-458d-b4f'
BUCKET = 'dp-pdm-ai-platform-dev'
SOURCES = [
 'data/mdm/geography_master_sources.json',
 'data/mdm/beneficiary_master_sources.json',
 'data/mdm/golden_agents.json',
 'data/mdm/golden_beneficiaries_restricted.json',
 'data/mdm/golden_geography.json',
 'data/mdm/golden_saccos.json',
 'data/mdm/sacco_master_sources.json',
 'data/mdm/source_crosswalk.json',
 'data/mobile_networks',
 'data/mdm/beneficiary_identity_alerts_restricted.json',
 'data/mdm/agent_master_sources.json',
]
OUT = Path('/tmp/source-target-record-audit.json')
PATTERN = re.compile(r'^raw/v1/([^/]+)/.*?/topic=([^/]+)/partition=(\d+)/offset=(\d+)/record\.avro$')

def main():
    config_path = ROOT / 'platform/kafka/topics.yaml'
    if not config_path.is_file():
        raise RuntimeError(f'Missing {config_path}')
    config = yaml.safe_load(config_path.read_text())
    items = config.get('topics') if isinstance(config, dict) else config
    if not isinstance(items, list):
        raise RuntimeError('Unexpected topics.yaml structure; no guessing')
    topics = {}
    for item in items:
        if not isinstance(item, dict):
            raise RuntimeError('Unexpected topic item')
        name = item.get('name') or item.get('topic')
        mode = item.get('processing_mode')
        if not isinstance(name, str) or mode not in {'batch','streaming','real-time'} or name in topics:
            raise RuntimeError(f'Invalid/duplicate topic contract: {name}')
        topics[name] = mode
    if len(topics) != 38:
        raise RuntimeError(f'Expected 38 topics; found {len(topics)}')

    report = {'generated_utc':datetime.now(timezone.utc).isoformat(), 'project':PROJECT,
              'bucket':BUCKET, 'source_inventory':[], 'topics':{}, 'unknown_raw_objects':[],
              'notes':['Source file-to-topic lineage is not inferred from matching counts.',
                       'Avro structural decoding is not equivalent to payload-hash verification.',
                       'Kafka is a point-in-time retained-record snapshot; compaction may remove historical records.',
                       'Pub/Sub historical delivery totals are not reconstructable from a current subscription.']}
    for relative in SOURCES:
        path = ROOT / relative
        entry = {'path':relative, 'exists':path.exists()}
        if path.is_file():
            entry['bytes'] = path.stat().st_size
            if path.suffix == '.json':
                data = json.loads(path.read_text())
                entry['record_count'] = len(data) if isinstance(data,list) else None
                entry['json_root_type'] = type(data).__name__
                if isinstance(data,dict):
                    entry['list_lengths'] = {k:len(v) for k,v in data.items() if isinstance(v,list)}
        elif path.is_dir():
            files = [p for p in path.rglob('*') if p.is_file()]
            entry['files'] = len(files)
            entry['bytes'] = sum(p.stat().st_size for p in files)
        report['source_inventory'].append(entry)

    consumer = Consumer({'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP','localhost:9094'),
        'group.id':'read-only-record-reconciliation', 'enable.auto.commit':False,
        'enable.auto.offset.store':False, 'allow.auto.create.topics':False,
        'auto.offset.reset':'error'})
    try:
        # Scan each partition independently to avoid poll-order assumptions.
        for topic, mode in topics.items():
            metadata = consumer.list_topics(topic, timeout=15)
            info = metadata.topics.get(topic)
            if info is None or info.error:
                raise RuntimeError(f'Kafka topic unavailable: {topic}')
            observed = {}
            offsets = {}
            for partition in sorted(info.partitions):
                tp = TopicPartition(topic, partition)
                low, high = consumer.get_watermark_offsets(tp, timeout=15, cached=False)
                offsets[str(partition)] = {'low':low, 'high':high}
                if low == high:
                    continue
                consumer.assign([TopicPartition(topic,partition,low)])
                deadline = time.monotonic() + 120
                while True:
                    pos = consumer.position([tp])[0].offset
                    if pos >= high:
                        break
                    if time.monotonic() > deadline:
                        raise RuntimeError(f'Kafka poll timeout: {topic}:{partition} at {pos}/{high}')
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        raise RuntimeError(str(msg.error()))
                    if msg.topic() != topic or msg.partition() != partition:
                        raise RuntimeError('Unexpected Kafka partition')
                    if msg.offset() >= high:
                        raise RuntimeError('Kafka snapshot advanced during scan')
                    identity = (partition,msg.offset())
                    if identity in observed:
                        raise RuntimeError(f'Duplicate Kafka identity: {topic}:{identity}')
                    payload = msg.value()
                    observed[identity] = {'payload_bytes':len(payload) if payload is not None else 0,
                         'payload_sha256':hashlib.sha256(payload).hexdigest() if payload is not None else None,
                         'tombstone':payload is None}
                consumer.unassign()
            report['topics'][topic] = {'processing_mode':mode, 'kafka_partitions':offsets,
                'kafka_records':len(observed),
                'kafka_payload_bytes':sum(x['payload_bytes'] for x in observed.values()),
                'kafka_tombstones':sum(x['tombstone'] for x in observed.values()),
                'raw_objects':0,'raw_stored_bytes':0,'raw_decoded_records':0,
                'raw_avro_errors':[],'missing_in_raw':[], 'extra_in_raw':[],
                'duplicate_raw_identities':[], 'mode_mismatches':[],
                '_kafka_ids':observed, '_raw_ids':set()}
            print(f'KAFKA {topic}: records={len(observed)} bytes={report["topics"][topic]["kafka_payload_bytes"]}',flush=True)
    finally:
        consumer.close()

    client = storage.Client(project=PROJECT)
    if client.lookup_bucket(BUCKET) is None:
        raise RuntimeError(f'Bucket unavailable: {BUCKET}')
    for blob in client.list_blobs(BUCKET,prefix='raw/v1/'):
        if blob.name.endswith('/'):
            continue
        match = PATTERN.fullmatch(blob.name)
        if not match:
            report['unknown_raw_objects'].append(blob.name)
            continue
        mode,topic,p,o = match.groups()
        if topic not in report['topics']:
            report['unknown_raw_objects'].append(blob.name)
            continue
        identity = (int(p),int(o))
        row = report['topics'][topic]
        if mode != row['processing_mode']:
            row['mode_mismatches'].append(blob.name)
        if identity in row['_raw_ids']:
            row['duplicate_raw_identities'].append(blob.name)
        row['_raw_ids'].add(identity)
        row['raw_objects'] += 1
        row['raw_stored_bytes'] += blob.size or 0
        try:
            data = blob.download_as_bytes(timeout=60)
            if not data.startswith(b'Obj\x01'):
                raise ValueError('Missing Avro OCF magic')
            with io.BytesIO(data) as stream:
                count = sum(1 for _ in fastavro.reader(stream))
            row['raw_decoded_records'] += count
            if count != 1:
                row['raw_avro_errors'].append({'path':blob.name,'reason':f'Expected one record per object; decoded {count}'})
        except Exception as exc:
            row['raw_avro_errors'].append({'path':blob.name,'reason':str(exc)})

    failures = 0
    for topic,row in report['topics'].items():
        kafka_ids = set(row.pop('_kafka_ids'))
        raw_ids = row.pop('_raw_ids')
        row['missing_in_raw'] = [f'{p}:{o}' for p,o in sorted(kafka_ids - raw_ids)]
        row['extra_in_raw'] = [f'{p}:{o}' for p,o in sorted(raw_ids - kafka_ids)]
        row['position_count_matches'] = not row['missing_in_raw'] and not row['extra_in_raw']
        row['avro_valid'] = not row['raw_avro_errors'] and row['raw_decoded_records'] == row['raw_objects']
        row['status'] = 'PASS_POSITION_AND_AVRO' if (row['position_count_matches'] and row['avro_valid'] and not row['duplicate_raw_identities'] and not row['mode_mismatches']) else 'FAIL'
        failures += row['status']=='FAIL'
        print(f'{row["status"]} {topic}: Kafka={row["kafka_records"]} Raw={row["raw_objects"]} AvroRows={row["raw_decoded_records"]} KafkaBytes={row["kafka_payload_bytes"]} GCSBytes={row["raw_stored_bytes"]}',flush=True)
    report['summary'] = {'topics':len(topics),'populated_topics':sum(r['raw_objects']>0 for r in report['topics'].values()),
        'kafka_records':sum(r['kafka_records'] for r in report['topics'].values()),
        'raw_objects':sum(r['raw_objects'] for r in report['topics'].values()),
        'decoded_avro_records':sum(r['raw_decoded_records'] for r in report['topics'].values()),
        'kafka_payload_bytes':sum(r['kafka_payload_bytes'] for r in report['topics'].values()),
        'gcs_raw_bytes':sum(r['raw_stored_bytes'] for r in report['topics'].values()),
        'failed_topics':failures,'unmatched_raw_paths':len(report['unknown_raw_objects']),
        'payload_hash_reconciled':False}
    OUT.write_text(json.dumps(report,indent=2,sort_keys=True))
    print('SUMMARY',json.dumps(report['summary'],sort_keys=True))
    print('REPORT',OUT)
    if failures or report['unknown_raw_objects']:
        sys.exit(2)
    print('PASS: Kafka identities and Avro structure reconciled; payload hashes NOT YET reconciled')

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(f'STOP: {type(exc).__name__}: {exc}',file=sys.stderr)
        sys.exit(1)
