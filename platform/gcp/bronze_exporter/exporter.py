#!/usr/bin/env python3
"""KP-014B BigQuery Bronze -> local JSONL. Offline default; --live is explicit and billable."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sys

PROJECT = 'project-e48f194e-f44f-458d-b4f'
TABLE = f'{PROJECT}.bronze.payments_raw_v2'
FIELDS = ('event_id message_id event_timestamp source_system message_type event_family '
          'payload_amount payload_currency payload_debtor payload_creditor payload_status '
          'payload_x_attributes event_data parsed_event_data _kafka_topic _kafka_partition '
          '_kafka_offset _kafka_timestamp _kafka_category _kafka_source_group '
          '_kafka_source_system _raw_object_path _bronze_pipeline_version _bronze_loaded_at').split()
TIMESTAMPS = {'_kafka_timestamp', '_bronze_loaded_at'}


def sql(limit):
    columns = ',\n  '.join(('TO_JSON_STRING(payload_x_attributes) AS payload_x_attributes'
                            if f == 'payload_x_attributes' else f'`{f}`') for f in FIELDS)
    # Stable total order for unique Kafka identities. A live mutable source is not a frozen snapshot.
    return (f'SELECT\n  {columns}\nFROM `{TABLE}`\n'
            'ORDER BY `_kafka_topic`, `_kafka_partition`, `_kafka_offset`\n'
            + (f'LIMIT {limit}\n' if limit is not None else ''))


def normalize(row):
    result = {}
    for field in FIELDS:
        val = row[field]
        if field in TIMESTAMPS and val is not None:
            if not isinstance(val, dt.datetime) or val.tzinfo is None:
                raise ValueError(f'{field}: expected timezone-aware datetime')
            val = val.astimezone(dt.timezone.utc).isoformat()
        if field == 'payload_x_attributes' and val is not None:
            # BigQuery TO_JSON_STRING yields JSON text. Do not double-encode it.
            if not isinstance(val, str):
                raise ValueError('payload_x_attributes: expected JSON string')
            json.loads(val)
        result[field] = val
    return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--live', action='store_true', help='explicitly authorise BigQuery query execution')
    p.add_argument('--output', type=Path, help='new local directory; never overwrite')
    p.add_argument('--limit', type=int, help='sample size; omit only after sample accepted')
    p.add_argument('--max-bytes-billed', type=int, default=100_000_000,
                   help='hard BigQuery maximum_bytes_billed, default 100 MB; not a monetary guarantee')
    p.add_argument('--expected-rows', type=int, help='require exact count; recommended 20403 for full export')
    args = p.parse_args(argv)
    if args.limit is not None and not 1 <= args.limit <= 100000:
        p.error('--limit must be between 1 and 100000')
    if args.max_bytes_billed < 1:
        p.error('--max-bytes-billed must be positive')
    if args.expected_rows is not None and args.expected_rows < 0:
        p.error('--expected-rows must be nonnegative')
    if args.expected_rows is not None and args.limit is not None and args.expected_rows > args.limit:
        p.error('--expected-rows cannot exceed --limit')
    statement = sql(args.limit)
    if not args.live:
        print('OFFLINE ONLY: no credentials, no BigQuery calls, no writes')
        print(statement)
        return 0
    if args.output is None:
        p.error('--live requires --output')
    dest = args.output.expanduser().resolve()
    if dest.exists() or dest.is_symlink():
        p.error('output directory exists; overwrite prohibited')
    if str(dest).startswith(('gs://', 's3://')):
        p.error('remote output prohibited')
    if os.environ.get('KP014B_ALLOW_BQ_READ') != 'YES':
        p.error('live mode also requires KP014B_ALLOW_BQ_READ=YES')
    from google.cloud import bigquery
    client = bigquery.Client(project=PROJECT)
    dry_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_job = client.query(statement, job_config=dry_config, location='europe-west2')
    estimated = int(dry_job.total_bytes_processed or 0)
    print(f'DRY RUN: estimated bytes processed={estimated}; cap={args.max_bytes_billed}', flush=True)
    if estimated > args.max_bytes_billed:
        raise SystemExit('STOP: dry-run estimate exceeds configured maximum')
    config = bigquery.QueryJobConfig(maximum_bytes_billed=args.max_bytes_billed,
                                    use_query_cache=False, priority=bigquery.QueryPriority.BATCH)
    # Create the output only after the dry-run gate; failures leave auditable partial output.
    dest.mkdir(parents=True, exist_ok=False)
    digest = hashlib.sha256()
    count = 0
    event_ids = set()
    kafka_ids = set()
    try:
        job = client.query(statement, job_config=config, location='europe-west2')
        with (dest / 'bronze.jsonl').open('wb') as f:
            for row in job.result(page_size=1000):
                obj = normalize(row)
                kafka_key = (obj['_kafka_topic'], obj['_kafka_partition'], obj['_kafka_offset'])
                if kafka_key in kafka_ids:
                    raise ValueError('duplicate Kafka identity in export')
                if obj['event_id'] in event_ids:
                    raise ValueError('duplicate event_id in export')
                kafka_ids.add(kafka_key)
                event_ids.add(obj['event_id'])
                data = (json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                                   allow_nan=False) + '\n').encode('utf-8')
                f.write(data)
                digest.update(data)
                count += 1
        if args.expected_rows is not None and count != args.expected_rows:
            raise ValueError(f'row-count mismatch: got {count}, expected {args.expected_rows}')
        manifest = {'status': 'COMPLETE_LOCAL_EXPORT_NOT_ICEBERG', 'table': TABLE,
                    'query_job_id': job.job_id, 'location': 'europe-west2',
                    'rows': count, 'unique_event_ids': len(event_ids),
                    'unique_kafka_identities': len(kafka_ids), 'fields': len(FIELDS),
                    'sha256': digest.hexdigest(), 'dry_run_bytes': estimated,
                    'bytes_billed': job.total_bytes_billed,
                    'max_bytes_billed': args.max_bytes_billed,
                    'limit': args.limit, 'snapshot_consistency': 'single BigQuery query',
                    'iceberg_commits': 0, 'gcs_writes': 0}
        (dest / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True)+'\n')
        print(json.dumps(manifest, indent=2, sort_keys=True))
    except Exception as exc:
        (dest / 'FAILED.txt').write_text(f'{type(exc).__name__}: {exc}\n')
        raise
    return 0


if __name__ == '__main__':
    sys.exit(main())
