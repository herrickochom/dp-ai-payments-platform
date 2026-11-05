#!/usr/bin/env python3
"""KP-014 offline Bronze validation and Parquet staging; NEVER commits an Iceberg table.

Input: local JSONL export of the verified 24-column BigQuery Bronze schema.
No GCP credentials, API calls, or remote filesystem writes are used.
"""
import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys

FIELDS = [
    ('event_id','string',True), ('message_id','string',False),
    ('event_timestamp','string',True), ('source_system','string',True),
    ('message_type','string',True), ('event_family','string',False),
    ('payload_amount','double',False), ('payload_currency','string',False),
    ('payload_debtor','string',False), ('payload_creditor','string',False),
    ('payload_status','string',False), ('payload_x_attributes','string',False),
    ('event_data','string',False), ('parsed_event_data','string',False),
    ('_kafka_topic','string',True), ('_kafka_partition','long',True),
    ('_kafka_offset','long',True), ('_kafka_timestamp','timestamptz',True),
    ('_kafka_category','string',False), ('_kafka_source_group','string',False),
    ('_kafka_source_system','string',False), ('_raw_object_path','string',True),
    ('_bronze_pipeline_version','string',True), ('_bronze_loaded_at','timestamptz',True),
    ('parsed_event_timestamp','timestamptz',True)
]
SOURCE_NAMES = {name for name, _, _ in FIELDS if name != 'parsed_event_timestamp'}
INTEGER_MIN = -(2**63)
INTEGER_MAX = 2**63 - 1

def timestamp(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('timestamp must be nonempty ISO-8601 string')
    s = value.strip()
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    try:
        parsed = dt.datetime.fromisoformat(s)
    except ValueError as exc:
        raise ValueError('invalid ISO-8601 timestamp') from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('timezone offset required')
    return parsed.astimezone(dt.timezone.utc)

def canonical_json(value):
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError('payload_x_attributes is not valid JSON') from exc
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)

def convert(record):
    if not isinstance(record, dict):
        raise ValueError('record must be a JSON object')
    unknown = set(record) - SOURCE_NAMES
    if unknown:
        raise ValueError('unexpected source fields: ' + ','.join(sorted(unknown)))
    result = {}
    for name, kind, required in FIELDS:
        if name == 'parsed_event_timestamp':
            continue
        value = record.get(name)
        if value is None:
            if required:
                raise ValueError(f'missing required field {name}')
            result[name] = None
            continue
        if name == 'payload_x_attributes':
            result[name] = canonical_json(value)
        elif kind == 'string':
            if not isinstance(value, str):
                raise ValueError(f'{name} must be string')
            if required and not value.strip():
                raise ValueError(f'{name} cannot be empty')
            result[name] = value
        elif kind == 'long':
            if type(value) is not int or not INTEGER_MIN <= value <= INTEGER_MAX:
                raise ValueError(f'{name} must be signed int64')
            if name in ('_kafka_partition','_kafka_offset') and value < 0:
                raise ValueError(f'{name} cannot be negative')
            result[name] = value
        elif kind == 'double':
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f'{name} must be finite numeric')
            result[name] = float(value)
        elif kind == 'timestamptz':
            result[name] = timestamp(value)
        else:
            raise AssertionError(kind)
    result['parsed_event_timestamp'] = timestamp(record['event_timestamp'])
    return result

def json_safe(record):
    return {k: v.isoformat() if isinstance(v, dt.datetime) else v for k, v in record.items()}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True, type=Path, help='local JSONL input; no cloud URI')
    parser.add_argument('--output', required=True, type=Path, help='new local directory')
    parser.add_argument('--parquet', action='store_true', help='write Parquet via installed pyarrow (no Iceberg commit)')
    args = parser.parse_args()
    src = args.input.expanduser().resolve()
    out = args.output.expanduser().resolve()
    if not src.is_file() or src.is_symlink():
        parser.error('input must be existing regular local file, not a symlink')
    if out.exists():
        parser.error('output must not exist; overwrite prohibited')
    if out == src or out in src.parents or src in out.parents:
        parser.error('input and output must be separate')
    if args.parquet:
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            parser.error('pyarrow not installed; no package installation attempted')
    out.mkdir(parents=True, exist_ok=False)
    accepted = []
    rejected = []
    seen = set()
    digest = hashlib.sha256()
    total = 0
    with src.open('rb') as handle:
        for line in handle:
            total += 1
            digest.update(line)
            try:
                record = json.loads(line)
                converted = convert(record)
                key = (converted['_kafka_topic'], converted['_kafka_partition'], converted['_kafka_offset'])
                if key in seen:
                    raise ValueError('duplicate Kafka topic/partition/offset within input')
                seen.add(key)
                accepted.append(converted)
            except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                rejected.append({'line': total, 'reason': str(exc)[:300], 'source_sha256': hashlib.sha256(line).hexdigest()})
    with (out/'quarantine.jsonl').open('w', encoding='utf-8') as handle:
        for row in rejected:
            handle.write(json.dumps(row, sort_keys=True)+'\n')
    if args.parquet:
        type_map = {'string': pa.string(), 'long': pa.int64(), 'double': pa.float64(), 'timestamptz': pa.timestamp('us', tz='UTC')}
        schema = pa.schema([pa.field(n, type_map[t], nullable=not required) for n,t,required in FIELDS])
        table = pa.Table.from_pylist(accepted, schema=schema)
        pq.write_table(table, out/'validated_staging.parquet', compression='zstd')
    else:
        with (out/'validated_staging.jsonl').open('w', encoding='utf-8') as handle:
            for row in accepted:
                handle.write(json.dumps(json_safe(row), sort_keys=True, ensure_ascii=False)+'\n')
    summary = {
        'status': 'OFFLINE_STAGING_ONLY_NOT_ICEBERG_COMMIT',
        'source_sha256': digest.hexdigest(), 'input_lines': total,
        'accepted': len(accepted), 'quarantined': len(rejected),
        'unique_kafka_identities_in_accepted': len(seen),
        'format': 'parquet' if args.parquet else 'jsonl',
        'schema_fields': len(FIELDS), 'cloud_calls': 0,
        'iceberg_catalog_commits': 0,
        'cross_run_deduplication': 'NOT_IMPLEMENTED',
        'source_snapshot_consistency': 'INPUT_EXPORT_RESPONSIBILITY',
        'quarantine_contains': 'line, reason, source_sha256 (not source payload)'
    }
    (out/'manifest.json').write_text(json.dumps(summary, indent=2, sort_keys=True)+'\n', encoding='utf-8')
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not rejected else 3

if __name__ == '__main__':
    sys.exit(main())
