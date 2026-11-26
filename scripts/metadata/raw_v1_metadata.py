#!/usr/bin/env python3
"""Single-object Raw v1 metadata pilot. Dry-run unless --apply is supplied."""
import argparse
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from google.cloud import storage

PATTERN = re.compile(
    r'raw/v1/(?P<mode>real-time|streaming|batch)/category=(?P<category>[^/]+)/'
    r'source_group=(?P<group>[^/]+)/source_system=(?P<system>[^/]+)/'
    r'year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/'
    r'topic=(?P<topic>[^/]+)/partition=(?P<partition>\d+)/offset=(?P<offset>\d+)/record\.avro'
)

def derive(name):
    match = PATTERN.fullmatch(name)
    if not match:
        raise ValueError('Object does not match canonical Raw v1 path')
    p = match.groupdict()
    date(int(p['year']), int(p['month']), int(p['day']))
    return {'data-domain': 'payments', 'data-layer': 'raw',
            'storage-contract-version': 'v1', 'processing-mode': p['mode'],
            'category': p['category'], 'source-group': p['group'],
            'source-system': p['system'], 'source-topic': p['topic'],
            'source-partition': p['partition'], 'source-offset': p['offset']}

def snapshot(blob):
    return {k: getattr(blob, k) for k in ('generation', 'metageneration', 'crc32c',
            'md5_hash', 'size', 'content_type', 'cache_control', 'content_encoding',
            'content_disposition', 'content_language', 'custom_time', 'metadata')}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', required=True)
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--object', required=True, help='Exact object name, not gs:// URI')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence-dir', default='metadata-evidence')
    args = parser.parse_args()
    proposed = derive(args.object)
    directory = Path(args.evidence_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    evidence = directory / f'raw-metadata-{stamp}.json'
    report = {'uri': f'gs://{args.bucket}/{args.object}', 'apply': args.apply,
              'proposed_custom_metadata': proposed, 'status': 'STARTED'}
    def save():
        evidence.write_text(json.dumps(report, indent=2, default=str) + '\n')
    try:
        client = storage.Client(project=args.project)
        bucket = client.bucket(args.bucket)
        blob = bucket.blob(args.object)
        blob.reload()
        before = snapshot(blob)
        report['before'] = before
        if before['content_type'] != 'application/avro' or before['content_encoding']:
            raise ValueError('Unexpected content type or encoding; refusing update')
        if not before['crc32c'] or before['generation'] is None or before['metageneration'] is None:
            raise ValueError('Missing integrity or concurrency evidence')
        existing = dict(before['metadata'] or {})
        conflicts = {k: existing[k] for k,v in proposed.items() if k in existing and existing[k] != v}
        if conflicts:
            report['conflicts'] = conflicts
            raise ValueError('Existing custom metadata conflicts; manual review required')
        merged = {**existing, **proposed}
        report['proposed_metadata'] = merged
        report['proposed_cache_control'] = 'no-store'
        changed = merged != existing or before['cache_control'] != 'no-store'
        if not args.apply:
            report['status'] = 'DRY_RUN_PASS'
        elif not changed:
            report['status'] = 'NO_CHANGE_REQUIRED'
        else:
            report['status'] = 'UPDATE_PENDING'
            save()
            blob.metadata = merged
            blob.cache_control = 'no-store'
            blob.patch(if_generation_match=before['generation'],
                       if_metageneration_match=before['metageneration'])
            report['status'] = 'UPDATE_SENT_VERIFICATION_PENDING'
            save()
            fresh = bucket.blob(args.object)
            fresh.reload()
            after = snapshot(fresh)
            report['after'] = after
            unchanged = ('generation','crc32c','md5_hash','size','content_type',
                         'content_encoding','content_disposition','content_language','custom_time')
            checks = {k: before[k] == after[k] for k in unchanged}
            checks['metadata'] = after['metadata'] == merged
            checks['cache_control'] = after['cache_control'] == 'no-store'
            checks['metageneration_advanced'] = int(after['metageneration']) > int(before['metageneration'])
            report['checks'] = checks
            if not all(checks.values()):
                raise RuntimeError('Post-update verification failed; inspect evidence before retrying')
            report['status'] = 'PASS'
    except Exception as exc:
        report['previous_status'] = report['status']
        report['status'] = 'FAIL'
        report['error'] = str(exc)
        raise
    finally:
        save()
        print(json.dumps(report, indent=2, default=str))
        print(f'Evidence: {evidence}')

if __name__ == '__main__':
    main()
