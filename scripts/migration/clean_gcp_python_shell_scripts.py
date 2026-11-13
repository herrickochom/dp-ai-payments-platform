#!/usr/bin/env python3
"""One-off, local-only cleanup of Python and shell scripts in the GCP repository.

Preview by default; --apply changes exact, safe legacy local-project references.
No cloud calls, Docker execution, backup creation or Git operations that mutate state.
"""
from __future__ import annotations
import argparse
import ast
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path('/home/hochom/projects/dp-ai-payments-platform-gcp')
REPO_NAME = 'dp-ai-payments-platform-gcp'
OLD_NAME = 'dp-pdm-ai-platform'
SKIP_DIRS = {'.git', '.venv', 'venv', 'node_modules', '__pycache__', '.pytest_cache', '.terraform', 'build', 'dist'}
# Do not rewrite cloud identifiers, URLs, bucket names, topic names or arbitrary prose.
LOCAL_PATH = re.compile(r'(?<![A-Za-z0-9_-])(?:/home/hochom/projects/|~/projects/)' + re.escape(OLD_NAME) + r'(?=/|[\\\"\'\s]|$)')
PROJECT_ASSIGN = re.compile(r'(?m)^(\s*(?:export\s+)?(?:PROJECT_NAME|COMPOSE_PROJECT_NAME)\s*=\s*[\"\'])' + re.escape(OLD_NAME) + r'([\"\']\s*(?:#.*)?)$')
PY_ASSIGN = re.compile(r'(?m)^(\s*(?:PROJECT_NAME|COMPOSE_PROJECT_NAME)\s*=\s*[\"\'])' + re.escape(OLD_NAME) + r'([\"\']\s*(?:#.*)?)$')
DANGEROUS = re.compile(r'\bdocker\s+(?:system\s+prune|volume\s+(?:rm|prune)|container\s+prune|image\s+prune)|\bdocker\s+compose\s+down\b|\brm\s+-[A-Za-z]*r[A-Za-z]*f\b')
LEGACY = re.compile(r'(?i)nessie|minio|trino|raw[/_-]v2|s3[_-]?endpoint|s3.compatible|dp-pdm-ai-platform')

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--root', type=Path, default=ROOT)
    ap.add_argument('--apply', action='store_true', help='Write only safe, exact replacements')
    ap.add_argument('--report', type=Path, default=Path('/tmp/gcp-script-cleanup-report.json'))
    args = ap.parse_args()
    root = args.root.resolve()
    if root != ROOT or not root.is_dir() or not (root / '.git').exists():
        print('STOP: expected canonical Git repository at ' + str(ROOT), file=sys.stderr)
        return 2
    changes, blockers, checked = [], [], 0
    staged = []
    for path in sorted(root.rglob('*')):
        if not path.is_file() or path.suffix not in {'.py', '.sh'} or any(p in SKIP_DIRS for p in path.relative_to(root).parts):
            continue
        if path.is_symlink():
            continue
        checked += 1
        try:
            original = path.read_text(encoding='utf-8')
        except (UnicodeError, OSError):
            blockers.append({'path': str(path.relative_to(root)), 'reason': 'unreadable source'})
            continue
        updated = LOCAL_PATH.sub(lambda m: m.group(0).replace(OLD_NAME, REPO_NAME), original)
        updated = PROJECT_ASSIGN.sub(lambda m: m.group(1) + REPO_NAME + m.group(2), updated)
        if path.suffix == '.py':
            updated = PY_ASSIGN.sub(lambda m: m.group(1) + REPO_NAME + m.group(2), updated)
        relative = path.relative_to(root).as_posix()
        if updated != original:
            try:
                if path.suffix == '.py':
                    ast.parse(updated, filename=relative)
                else:
                    result = subprocess.run(['bash', '-n'], input=updated, text=True, capture_output=True)
                    if result.returncode:
                        raise ValueError(result.stderr.strip())
            except (SyntaxError, ValueError) as exc:
                blockers.append({'path': relative, 'reason': 'post-edit syntax failure', 'detail': str(exc)})
                continue
            staged.append((path, updated))
            changes.append({'path': relative, 'action': 'replace verified local path/project assignment'})
        for line_no, line in enumerate(updated.splitlines(), 1):
            if LEGACY.search(line) or DANGEROUS.search(line):
                reasons = []
                if LEGACY.search(line): reasons.append('legacy identifier/reference')
                if DANGEROUS.search(line): reasons.append('potentially destructive Docker/filesystem command')
                blockers.append({'path': relative, 'line': line_no, 'reason': ', '.join(reasons), 'excerpt': line.strip()[:180]})
    report = {'mode': 'APPLY' if args.apply else 'PREVIEW', 'root': str(root), 'scripts_scanned': checked,
              'safe_edits': changes, 'remaining_review_items': blockers,
              'summary': {'safe_files': len(changes), 'review_lines': len(blockers)}}
    # Preflight all changes before touching any file. Never overwrite unexpected modifications.
    if args.apply:
        for path, content in staged:
            path.write_text(content, encoding='utf-8')
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f"MODE: {report['mode']} | scanned: {checked} | safely {'edited' if args.apply else 'editable'}: {len(changes)} | review lines: {len(blockers)}")
    print('REPORT:', args.report)
    for entry in changes[:40]: print('EDIT:', entry['path'])
    if blockers:
        print('IMPORTANT: unresolved legacy/destructive references remain; no claim of complete cleanup.')
        for item in blockers[:25]: print('REVIEW:', item['path'], item.get('line', ''), item['reason'])
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
