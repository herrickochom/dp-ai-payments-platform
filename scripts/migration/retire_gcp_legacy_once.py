#!/usr/bin/env python3
"""One-off, offline, fail-closed retirement of legacy GCP repository references.

Dry-run by default. No backups, cloud calls, git staging, or hidden deletion.
"""
import argparse
import ast
import json
import re
import subprocess
from pathlib import Path

ROOT = Path.cwd()
AUDIT = Path('/tmp/gcp-repository-naming-audit-final.json')
CONFIRM = 'RETIRE-GCP-LEGACY'
PATTERNS = {
    'nessie': re.compile(r'nessie', re.I),
    'minio': re.compile(r'minio', re.I),
    'trino': re.compile(r'trino', re.I),
    'raw_v2': re.compile(r'raw[/_-]?v2|raw_v2|raw v2', re.I),
    'legacy_s3_endpoint': re.compile(r'(?i)(?:s3[._-]?endpoint|s3a://|s3://|AWS_ENDPOINT_URL)'),
}
# The source audit contains references in security tests and runtime configuration.
# Those cannot be deleted or replaced correctly without inspecting their code.
# Only standalone obsolete migration tools are eligible for automatic deletion.
OBSOLETE_TOOLS = {
    'delete_legacy_gcp_technologies.py',
    'finalise_legacy_gcp_cleanup.py',
    'standardise_gcp_repository_once.py',
}
# Historical stage-labelled documents are retired only if they are not referenced
# by another live repository file. No evidence/ directory is ever touched.
OBSOLETE_DOCS = {
    'docs/gcp/STAGE10-DEPLOYMENT-HANDOFF.md',
    'docs/gcp/STAGE10-IMPLEMENTATION-REVIEW.md',
    'docs/gcp/STAGE10-CONSOLIDATED-ACCEPTANCE.md',
}
SKIP_PARTS = {'.git', '.venv', 'venv', 'node_modules', '__pycache__', '.terraform',
              '.pytest_cache', '.mypy_cache', '.ruff_cache', 'dist', 'build'}
SKIP_PREFIXES = ('evidence/', 'docs/evidence/', 'programme/evidence/', 'runtime/repository-cleanup/')
TEXT_EXT = {'.py', '.md', '.yaml', '.yml', '.json', '.toml', '.ini', '.cfg', '.sh',
            '.txt', '.tf', '.tfvars', '.sql', '.avsc', '.xml'}


def repository_files():
    for path in ROOT.rglob('*'):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT).as_posix()
        if any(part in SKIP_PARTS for part in path.relative_to(ROOT).parts):
            continue
        if rel.startswith(SKIP_PREFIXES) or path.suffix not in TEXT_EXT:
            continue
        if path.stat().st_size > 2_000_000:
            continue
        yield path, rel


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--audit', type=Path, default=AUDIT)
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--confirm', default='')
    ap.add_argument('--report', type=Path, default=Path('/tmp/gcp-legacy-retirement-plan.json'))
    args = ap.parse_args()
    git = subprocess.run(['git', 'rev-parse', '--show-toplevel'], cwd=ROOT,
                         text=True, capture_output=True)
    if git.returncode or Path(git.stdout.strip()).resolve() != ROOT.resolve():
        raise SystemExit('BLOCKED: run from the Git repository root')
    if args.apply and args.confirm != CONFIRM:
        raise SystemExit(f'BLOCKED: --confirm {CONFIRM} required')
    if not args.audit.is_file():
        raise SystemExit(f'BLOCKED: audit JSON missing: {args.audit}')
    audit = json.loads(args.audit.read_text())
    if Path(audit.get('repository', '')).resolve() != ROOT.resolve():
        raise SystemExit('BLOCKED: audit belongs to a different repository')
    issues = audit.get('issues', {})
    if not isinstance(issues.get('legacy_references'), list):
        raise SystemExit('BLOCKED: expected issues.legacy_references array missing')

    excluded = {args.audit.resolve(), args.report.resolve()}
    files = {rel: path for path, rel in repository_files()
             if path.resolve() not in excluded}
    # Do not delete a file when it is referenced by another file, even if it
    # appears to be a one-off tool or a historical document.
    proposed = sorted((OBSOLETE_TOOLS | OBSOLETE_DOCS) & files.keys())
    deletions = []
    blocked_deletions = []
    for rel in proposed:
        basename = Path(rel).name
        stem = Path(rel).stem
        users = []
        for other_rel, other_path in files.items():
            if other_rel == rel or other_rel == Path(__file__).name:
                continue
            try:
                content = other_path.read_text('utf-8')
            except (UnicodeError, OSError):
                continue
            if rel in content or basename in content or (rel.endswith('.py') and
                re.search(r'(?<![\w])' + re.escape(stem) + r'(?![\w])', content)):
                users.append(other_rel)
        if users:
            blocked_deletions.append({'path': rel, 'referenced_by': users[:30]})
        else:
            deletions.append(rel)

    # No arbitrary regex rewriting of executable code or test assertions.
    # Classify every actual remaining occurrence so it cannot be hidden by an
    # optimistic summary or a keyword-only audit.
    unresolved = []
    for rel, path in files.items():
        if rel in deletions or rel == Path(__file__).name:
            continue
        try:
            content = path.read_text('utf-8')
        except (UnicodeError, OSError):
            continue
        matches = []
        for lineno, line in enumerate(content.splitlines(), 1):
            found = [name for name, pattern in PATTERNS.items() if pattern.search(line)]
            if found:
                matches.append({'line': lineno, 'categories': found,
                                'excerpt': line.strip()[:180]})
        if matches:
            unresolved.append({'path': rel, 'matches': matches[:100],
                               'classification': 'requires_contextual_rewrite'})

    # A stale audit is not a license to remove newly changed files.
    report = {
        'mode': 'APPLY' if args.apply else 'DRY_RUN',
        'repository': str(ROOT),
        'audit_files_scanned': audit.get('files_scanned'),
        'eligible_deletions': deletions,
        'blocked_deletions': blocked_deletions,
        'unresolved_files': len(unresolved),
        'unresolved_references': unresolved,
        'scope': 'Deletes only unreferenced known disposable tools and stage-labelled docs; '
                 'never rewrites executable logic or deletes evidence',
    }
    args.report.write_text(json.dumps(report, indent=2) + '\n')
    print('MODE:', report['mode'])
    print('ELIGIBLE DELETIONS:', len(deletions))
    for rel in deletions:
        print('  DELETE:', rel)
    print('BLOCKED DELETIONS:', len(blocked_deletions))
    for item in blocked_deletions:
        print('  REFERENCED:', item['path'], 'by', ', '.join(item['referenced_by'][:5]))
    print('UNRESOLVED FILES REQUIRING CODE/DOCUMENT REVIEW:', len(unresolved))
    print('REPORT:', args.report)
    if not args.apply:
        print('DRY RUN ONLY; nothing changed')
        return
    # Validate Python syntax of the script itself before performing deletions.
    ast.parse(Path(__file__).read_text(), filename=__file__)
    for rel in deletions:
        target = files[rel]
        if target.is_symlink():
            raise SystemExit(f'BLOCKED: symlink candidate {rel}')
    for rel in deletions:
        files[rel].unlink()
    print('DELETED:', len(deletions))
    print('NOT COMPLETE: remaining references require targeted source-aware edits.')
    for command in (['git', 'diff', '--check'], ['git', 'diff', '--cached', '--check']):
        result = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
        if result.returncode:
            raise SystemExit('CHECK FAILED: ' + ' '.join(command) + '\n' + result.stdout + result.stderr)
    print('Git whitespace checks passed.')


if __name__ == '__main__':
    main()
