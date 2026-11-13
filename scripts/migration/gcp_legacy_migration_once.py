#!/usr/bin/env python3
"""Offline, one-off, source-aware legacy migration. No guessing or backups.

First run: generate source-context inventory and a guarded edit manifest in /tmp.
Edit the manifest's changes array after inspecting the captured contexts.
Apply: exact SHA-256 preconditions, transactional preflight, no partial writes on
preflight failures; no cloud calls, no git staging, no backup creation.
"""
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path.cwd().resolve()
DEFAULT_AUDIT = Path('/tmp/gcp-repository-naming-audit-final.json')
DEFAULT_PLAN = Path('/tmp/gcp-source-aware-migration.json')
DEFAULT_CONTEXT = Path('/tmp/gcp-source-aware-context.json')
CONFIRM = 'APPLY-VERIFIED-SOURCE-EDITS'
PATTERNS = {
    'nessie': r'nessie', 'minio': r'minio', 'trino': r'trino',
    'raw_v2': r'raw[/_-]?v2|raw v2',
    'legacy_s3_endpoint': r's3[._-]?endpoint|s3a://|s3://|AWS_ENDPOINT_URL',
}
REGEX = {k: re.compile(v, re.I) for k, v in PATTERNS.items()}
PROTECTED = ('evidence/', 'docs/evidence/', 'programme/evidence/',
             'runtime/repository-cleanup/')

def digest(data):
    return hashlib.sha256(data).hexdigest()

def safe_path(rel):
    if not isinstance(rel, str) or not rel or '\\' in rel:
        raise ValueError('invalid relative path')
    p = Path(rel)
    if p.is_absolute() or '..' in p.parts or '.git' in p.parts:
        raise ValueError('unsafe path: ' + rel)
    result = ROOT / p
    if not result.resolve().is_relative_to(ROOT):
        raise ValueError('path escapes repository: ' + rel)
    if result.is_symlink():
        raise ValueError('symlink blocked: ' + rel)
    return result

def check_root():
    r = subprocess.run(['git', 'rev-parse', '--show-toplevel'], capture_output=True, text=True)
    if r.returncode or Path(r.stdout.strip()).resolve() != ROOT:
        raise SystemExit('BLOCKED: run at the Git repository root')

def read_audit(path):
    obj = json.loads(path.read_text())
    issues = obj.get('issues', {})
    entries = issues.get('legacy_references')
    if not isinstance(entries, list):
        raise SystemExit('BLOCKED: audit missing issues.legacy_references')
    if obj.get('repository') and Path(obj['repository']).resolve() != ROOT:
        raise SystemExit('BLOCKED: audit belongs to another repository')
    return entries

def inventory(entries, context):
    by_path = {}
    for e in entries:
        if not isinstance(e, dict) or 'path' not in e:
            raise SystemExit('BLOCKED: malformed audit entry')
        by_path.setdefault(e['path'], []).append(e)
    files = []
    for rel, findings in sorted(by_path.items()):
        p = safe_path(rel)
        if not p.is_file():
            files.append({'path': rel, 'status': 'missing', 'findings': findings})
            continue
        raw = p.read_bytes()
        try:
            lines = raw.decode('utf-8').splitlines()
        except UnicodeDecodeError:
            files.append({'path': rel, 'status': 'binary-or-non-utf8', 'findings': findings})
            continue
        flagged = sorted({int(n) for f in findings for n in f.get('lines', []) if isinstance(n, int)})
        snippets = []
        for n in flagged:
            if 1 <= n <= len(lines):
                snippets.append({'line': n, 'context': [
                    {'number': j + 1, 'text': lines[j]}
                    for j in range(max(0, n - 4), min(len(lines), n + 3))]})
        files.append({'path': rel, 'status': 'present', 'sha256': digest(raw),
                      'findings': findings, 'contexts': snippets})
    context.write_text(json.dumps({'repository': str(ROOT), 'files': files}, indent=2) + '\n')
    return files

def generate_plan(files, plan):
    # No speculative substitutions. Every replacement must be exact and reviewed.
    obj = {'repository': str(ROOT), 'purpose': 'source-aware legacy retirement',
           'instructions': ('Add changes as {path, expected_sha256, action, old, new}. '
                            'action=replace_exact requires old and new and exactly one old match; '
                            'action=delete_file requires old/new omitted. '
                            'Review source contexts and callers before proposing changes. '
                            'Protected evidence and Terraform state files cannot be modified.'),
           'candidates': [{'path': f['path'], 'expected_sha256': f['sha256'],
                           'categories': sorted({x.get('category', '') for x in f['findings']})}
                          for f in files if f['status'] == 'present'],
           'changes': []}
    plan.write_text(json.dumps(obj, indent=2) + '\n')
    return obj

def apply_plan(path, confirm):
    if confirm != CONFIRM:
        raise SystemExit('BLOCKED: explicit --confirm ' + CONFIRM + ' required')
    obj = json.loads(path.read_text())
    if obj.get('repository') != str(ROOT):
        raise SystemExit('BLOCKED: plan repository mismatch')
    changes = obj.get('changes')
    if not isinstance(changes, list) or not changes:
        raise SystemExit('BLOCKED: no reviewed changes in plan; nothing applied')
    prepared = {}
    for c in changes:
        rel = c['path']
        if rel.startswith(PROTECTED) or rel.endswith(('.tfstate', '.tfstate.backup', '.tfplan')):
            raise SystemExit('BLOCKED: protected evidence or Terraform state: ' + rel)
        p = safe_path(rel)
        if not p.is_file():
            raise SystemExit('BLOCKED: missing file: ' + rel)
        original = prepared.get(rel, p.read_bytes())
        # Check against original disk state, not intermediate modifications.
        if digest(p.read_bytes()) != c.get('expected_sha256'):
            raise SystemExit('BLOCKED: file changed since review: ' + rel)
        action = c.get('action')
        if action == 'delete_file':
            if rel in prepared:
                raise SystemExit('BLOCKED: duplicate action for deleted file: ' + rel)
            prepared[rel] = None
        elif action == 'replace_exact':
            old, new = c.get('old'), c.get('new')
            if not isinstance(old, str) or not old or not isinstance(new, str) or old == new:
                raise SystemExit('BLOCKED: invalid exact replacement: ' + rel)
            try:
                content = original.decode('utf-8')
            except UnicodeDecodeError:
                raise SystemExit('BLOCKED: non-UTF8 source: ' + rel)
            if content.count(old) != 1:
                raise SystemExit('BLOCKED: expected exactly one occurrence: ' + rel)
            prepared[rel] = content.replace(old, new, 1).encode('utf-8')
        else:
            raise SystemExit('BLOCKED: unknown action: ' + str(action))
    print('PREFLIGHT PASS:', len(prepared), 'files; exact hashes and replacements verified')
    # No writes until all source preconditions pass. This is not a full rollback
    # transaction if the OS fails during writes; git remains the recovery mechanism.
    for rel, data in prepared.items():
        p = safe_path(rel)
        if data is None:
            p.unlink()
        else:
            mode = p.stat().st_mode
            with p.open('wb') as handle:
                handle.write(data)
            os.chmod(p, mode)
        print('APPLIED:', rel)
    for args in (['git', 'diff', '--check'], ['git', 'diff', '--cached', '--check']):
        result = subprocess.run(args)
        if result.returncode:
            raise SystemExit('WARNING: git whitespace check failed; inspect changes')
    print('Git whitespace checks passed. Run regression tests and regenerate audit.')

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--audit', type=Path, default=DEFAULT_AUDIT)
    ap.add_argument('--plan', type=Path, default=DEFAULT_PLAN)
    ap.add_argument('--context', type=Path, default=DEFAULT_CONTEXT)
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--confirm', default='')
    args = ap.parse_args()
    check_root()
    if args.apply:
        apply_plan(args.plan, args.confirm)
    else:
        if not args.audit.is_file():
            raise SystemExit('BLOCKED: missing audit: ' + str(args.audit))
        files = inventory(read_audit(args.audit), args.context)
        generate_plan(files, args.plan)
        print('MODE: READ ONLY')
        print('AUDIT FILE/CATEGORY FINDINGS:', sum(len(f['findings']) for f in files))
        print('DISTINCT FILES:', len(files))
        print('SOURCE CONTEXT:', args.context)
        print('REVIEW MANIFEST:', args.plan)
        print('CHANGES: 0 (no invented replacements)')
        print('Nothing modified. Review source contexts before adding exact edits.')

if __name__ == '__main__':
    main()
