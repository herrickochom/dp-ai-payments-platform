#!/usr/bin/env python3
"""Apply a small, verified subset of the GCP legacy retirement migration.

Offline only. No backups, cloud calls, Terraform actions or bulk replacements.
Uses uploaded SHA256 evidence to reject drift; no partial writes on preflight errors.
"""
import argparse
import hashlib
import json
import sys
from pathlib import Path

DEFAULT_ROOT = Path('/home/hochom/projects/dp-ai-payments-platform-gcp')
EVIDENCE = {
    'ops/cleanup_docker_and_data.sh': ('    "backend/trino/logs"\n', ''),
    'tests/gcp/test_cloud_service_adapters.py': (
        "read('raw/v2/a.avro',1)",
        "read('raw/retired/a.avro',1)",
    ),
}

def sha(data):
    return hashlib.sha256(data).hexdigest()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--context', type=Path, required=True, help='Uploaded source-aware context JSON')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    root = args.root.resolve()
    if not (root / '.git').exists():
        sys.exit('STOP: repository .git directory not found')
    contexts = {x['path']: x for x in json.loads(args.context.read_text())['files']}
    planned = []
    errors = []
    for rel, (old, new) in EVIDENCE.items():
        path = (root / rel).resolve()
        if not path.is_relative_to(root) or path.is_symlink():
            errors.append(f'{rel}: unsafe path'); continue
        if not path.is_file() or rel not in contexts:
            errors.append(f'{rel}: missing file or evidence'); continue
        raw = path.read_bytes()
        if sha(raw) != contexts[rel]['sha256']:
            errors.append(f'{rel}: SHA256 drift; source changed since report'); continue
        try:
            data = raw.decode('utf-8')
        except UnicodeDecodeError:
            errors.append(f'{rel}: not UTF-8'); continue
        if data.count(old) != 1:
            errors.append(f'{rel}: expected exact text once, found {data.count(old)}'); continue
        revised = data.replace(old, new, 1)
        planned.append((path, rel, revised.encode('utf-8')))
    if errors:
        print('\n'.join('STOP: '+e for e in errors), file=sys.stderr)
        sys.exit(2)
    for _, rel, _ in planned:
        print(('APPLY' if args.apply else 'PREVIEW') + ': ' + rel)
    if not args.apply:
        print('No files changed. Pass --apply to perform these exact edits.')
        return
    for path, rel, content in planned:
        path.write_bytes(content)
    print(f'CHANGED: {len(planned)} files; no backup files created')
    print('NEXT: git diff --check && python -m pytest -q tests/gcp/test_cloud_service_adapters.py')
    print('NOTE: This is only phase 1; full retirement is NOT complete.')

if __name__ == '__main__':
    main()
