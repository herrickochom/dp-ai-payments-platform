#!/usr/bin/env python3
"""Read-only repository-wide naming and reference audit. Python stdlib only."""
import ast
import json
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path.cwd()
EXCLUDED_DIRS = {'.git', '.venv', 'venv', 'node_modules', '__pycache__', '.pytest_cache', '.mypy_cache', '.ruff_cache', '.terraform', 'dist', 'build', '.tox'}
HISTORICAL_PREFIXES = ('evidence/', 'docs/evidence/', 'programme/evidence/', 'runtime/repository-cleanup/')
TEXT_EXT = {'.py', '.yaml', '.yml', '.json', '.toml', '.ini', '.cfg', '.sh', '.md', '.txt', '.sql', '.tf', '.tfvars', '.avsc', '.properties', '.env', '.xml'}
STAGE_RE = re.compile(r'(?i)(?:^|[_\-])(?:stage\d+[a-z0-9]*|kp\d{2,}[a-z0-9]*)(?:[_\-.]|$)')
LEGACY = {
    'raw_v2': re.compile(r'(?i)(?:raw[/_-]v2|raw_v2|raw/v2)'),
    'nessie': re.compile(r'(?i)\bnessie\b'),
    'minio': re.compile(r'(?i)\bminio\b'),
    'trino': re.compile(r'(?i)\btrino\b'),
    'legacy_s3_endpoint': re.compile(r'(?i)(?:s3[._-]endpoint|s3_compatible|s3-compatible)'),
}
TEMP_RE = re.compile(r'(?i)(?:\.bak(?:\.|$)|\.backup(?:\.|$)|\.orig$|\.rej$|~$|backup[-_]|[-_]backup|\.stage\d+[-_]backup)')

def iter_files():
    for p in ROOT.rglob('*'):
        if not p.is_file():
            continue
        rel = p.relative_to(ROOT)
        if any(part in EXCLUDED_DIRS for part in rel.parts):
            continue
        yield p, rel.as_posix()

def is_historical(rel):
    return rel.startswith(HISTORICAL_PREFIXES)

def git_status():
    try:
        p = subprocess.run(['git', 'status', '--porcelain=v1', '-uall'], capture_output=True, text=True, check=True)
        return {'available': True, 'changed_entries': len(p.stdout.splitlines()), 'entries': p.stdout.splitlines()[:100]}
    except (OSError, subprocess.CalledProcessError):
        return {'available': False}

def main():
    if not (ROOT / '.git').exists():
        raise SystemExit('Run from the GCP repository root (directory containing .git).')
    files = list(iter_files())
    issues = defaultdict(list)
    basenames = defaultdict(list)
    module_paths = set()
    for p, rel in files:
        basenames[p.name].append(rel)
        if p.suffix == '.py':
            module_paths.add(rel[:-3].replace('/', '.'))
        if STAGE_RE.search(p.name):
            issues['stage_or_kp_filename'].append({'path': rel, 'historical': is_historical(rel)})
        if TEMP_RE.search(p.name):
            issues['backup_or_temporary_filename'].append({'path': rel, 'historical': is_historical(rel)})
        if p.suffix not in TEXT_EXT or p.stat().st_size > 2_000_000:
            continue
        try:
            content = p.read_text(encoding='utf-8')
        except (UnicodeError, OSError):
            continue
        for label, pattern in LEGACY.items():
            matches = [i for i, line in enumerate(content.splitlines(), 1) if pattern.search(line)]
            if matches:
                issues['legacy_references'].append({'path': rel, 'category': label, 'lines': matches[:30], 'count': len(matches), 'historical': is_historical(rel)})
        if p.suffix == '.py':
            try:
                tree = ast.parse(content, filename=rel)
            except SyntaxError as exc:
                issues['python_syntax_errors'].append({'path': rel, 'line': exc.lineno, 'message': exc.msg})
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        name = alias.name
                        if name.startswith(('pipelines.gcp.', 'platform.gcp.', 'tests.gcp.')) and name not in module_paths:
                            # module_paths is populated progressively; resolve after traversal
                            issues['_imports_to_check'].append({'path': rel, 'line': node.lineno, 'module': name})
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    name = node.module
                    if name.startswith(('pipelines.gcp.', 'platform.gcp.', 'tests.gcp.')):
                        issues['_imports_to_check'].append({'path': rel, 'line': node.lineno, 'module': name})
    for name, paths in basenames.items():
        if name.endswith('.py') and len(paths) > 1:
            issues['duplicate_python_basenames'].append({'filename': name, 'paths': paths})
    for item in issues.pop('_imports_to_check', []):
        mod = item['module']
        if mod not in module_paths and not (ROOT / (mod.replace('.', '/') + '/__init__.py')).exists() and not (ROOT / mod.replace('.', '/')).is_dir():
            issues['unresolved_local_import_candidates'].append(item)
    issues = dict(issues)
    summary = {k: len(v) for k, v in issues.items()}
    report = {'repository': str(ROOT), 'mode': 'READ_ONLY', 'files_scanned': len(files), 'git_status': git_status(), 'summary': summary, 'issues': issues, 'limitations': ['Import candidates are static checks only; dynamic imports and external packages are not resolved.', 'Duplicate basenames do not necessarily imply duplicate implementations.', 'Legacy references may be negative tests, historical notes or intentionally retained migration safeguards.', 'No files, Git state, cloud resources or evidence were modified.']}
    output = json.dumps(report, indent=2)
    if '--json' in sys.argv:
        print(output)
    else:
        print('GCP REPOSITORY NAMING AUDIT — READ ONLY')
        print('Files scanned:', len(files))
        print('Git changed entries:', report['git_status'].get('changed_entries', 'unknown'))
        for category, count in sorted(summary.items()):
            print(f'{category}: {count}')
        print('\nActive stage/KP filenames (first 80):')
        for item in [x for x in issues.get('stage_or_kp_filename', []) if not x['historical']][:80]:
            print(' ', item['path'])
        print('\nUnresolved local import candidates (first 40):')
        for item in issues.get('unresolved_local_import_candidates', [])[:40]:
            print(f"  {item['path']}:{item['line']}: {item['module']}")
        print('\nRun with --json to capture full machine-readable findings.')
    return 0

if __name__ == '__main__':
    sys.exit(main())
