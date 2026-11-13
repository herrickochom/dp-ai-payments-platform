#!/usr/bin/env python3
"""One-off guarded repair of three tests referring to deleted legacy modules.

Run from dp-ai-payments-platform-gcp repository root. No backups, cloud calls,
Git index changes, or destructive cleanup of untracked files.
"""
import argparse
import ast
import json
import subprocess
import sys
from pathlib import Path

FILES = {
    'tests/security/test_secret_provider_contract.py': {
        'modules': {'orchestration.job_runner.transform_execution'},
        'symbols': {'AUTHORITY_ENV', 'FORBIDDEN_ENV'},
        'literals': {'transform_execution'},
    },
    'tests/security/test_postgres_production_contract.py': {
        'modules': set(), 'symbols': set(),
        'literals': {'orchestration/job_runner/transform_worker.py'},
    },
    'tests/security/test_runtime_security_contract.py': {
        'modules': set(), 'symbols': {'validate_nessie_security'},
        'literals': set(),
    },
}

def run(cmd):
    p = subprocess.run(cmd, capture_output=True, text=True)
    return {'command': cmd, 'exit_code': p.returncode,
            'stdout': p.stdout[-12000:], 'stderr': p.stderr[-12000:]}

def repair(src, cfg):
    tree = ast.parse(src)
    lines = src.splitlines(keepends=True)
    removed = []
    spans = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module in cfg['modules']:
            spans.append((node.lineno, node.end_lineno))
            removed.append('obsolete import: ' + str(node.module))
        elif isinstance(node, ast.ImportFrom) and cfg['symbols']:
            obsolete = [a for a in node.names if a.name in cfg['symbols']]
            if obsolete:
                keep = [a for a in node.names if a.name not in cfg['symbols']]
                if keep:
                    # A multiline import is handled by AST unparse only for that import.
                    indent = ' ' * node.col_offset
                    replacement = indent + 'from ' + ('.' * node.level) + (node.module or '') + ' import ' + ', '.join(
                        a.name + (' as ' + a.asname if a.asname else '') for a in keep) + '\n'
                    lines[node.lineno - 1] = replacement
                    for i in range(node.lineno, node.end_lineno):
                        lines[i] = ''
                    removed.append('obsolete imported symbol(s): ' + ', '.join(a.name for a in obsolete))
                else:
                    spans.append((node.lineno, node.end_lineno))
                    removed.append('obsolete import symbols: ' + ', '.join(a.name for a in obsolete))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith('test_'):
            segment = ast.get_source_segment(src, node) or ''
            if any(x in segment for x in cfg['symbols'] | cfg['literals']):
                start = min([node.lineno] + [d.lineno for d in node.decorator_list])
                spans.append((start, node.end_lineno))
                removed.append('obsolete test: ' + node.name)
    for start, end in spans:
        for i in range(start - 1, end):
            lines[i] = ''
    out = ''.join(lines).rstrip() + '\n'
    ast.parse(out)
    # If legacy names remain in executable code, refuse to write.
    remaining = []
    parsed = ast.parse(out)
    for node in ast.walk(parsed):
        if isinstance(node, ast.Name) and node.id in cfg['symbols']:
            remaining.append(f'legacy name {node.id} at line {node.lineno}')
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if any(x in node.value for x in cfg['literals']):
                remaining.append(f'legacy string at line {node.lineno}')
        if isinstance(node, ast.ImportFrom) and node.module in cfg['modules']:
            remaining.append(f'legacy import at line {node.lineno}')
    return out, removed, remaining

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()
    if not Path('platform/kafka/topics.yaml').is_file() or not Path('platform/gcp').is_dir():
        sys.exit('ERROR: Run from the GCP repository root')
    report = {'mode': 'APPLY' if args.apply else 'DRY_RUN', 'files': {}, 'blocked': []}
    planned = {}
    for name, cfg in FILES.items():
        p = Path(name)
        if not p.is_file() or p.is_symlink():
            report['blocked'].append(f'{name}: missing or symlink')
            continue
        try:
            original = p.read_text(encoding='utf-8')
            result, removed, remaining = repair(original, cfg)
            report['files'][name] = {'removed': removed, 'remaining': remaining,
                                     'changed': original != result}
            if remaining:
                report['blocked'].append(f'{name}: remaining executable legacy references')
            elif original != result:
                planned[name] = result
        except Exception as exc:
            report['blocked'].append(f'{name}: {type(exc).__name__}: {exc}')
    if report['blocked']:
        print(json.dumps(report, indent=2))
        sys.exit('BLOCKED: no files modified; inspect the reasons above')
    if args.apply:
        for name, result in planned.items():
            Path(name).write_text(result, encoding='utf-8')
    report['git_diff_check'] = run(['git', 'diff', '--check'])
    report['git_cached_diff_check'] = run(['git', 'diff', '--cached', '--check'])
    if args.apply:
        report['syntax_check'] = run([sys.executable, '-B', '-c',
             'import ast,pathlib; files=' + repr(list(FILES)) + '; '
             '[ast.parse(pathlib.Path(p).read_text()) for p in files]; print("Three test modules parse successfully")'])
        # Targeted test execution; a failing test is reported, never hidden.
        report['targeted_pytest'] = run([sys.executable, '-B', '-m', 'pytest', '-q',
                                        *FILES.keys()])
    destination = Path('runtime/repository-cleanup/legacy-test-repair-report.json')
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    print('REPORT:', destination)
    if any(report[k]['exit_code'] != 0 for k in ['git_diff_check', 'git_cached_diff_check']):
        sys.exit(2)
    if args.apply and any(report[k]['exit_code'] != 0 for k in ['syntax_check', 'targeted_pytest']):
        sys.exit(3)

if __name__ == '__main__':
    main()
