import json
from pathlib import Path
import pytest
from pdm_stage7_runtime import run, RuntimeError7

XSD = Path(__file__).resolve().parents[1] / 'contracts' / 'iso20022' / 'xsd'
EMPTY = {'sacco_ids': [], 'funding_allocations': [], 'funding_instructions': [], 'funding_statuses': [], 'funding_credits': [], 'loans': [], 'disbursement_instructions': [], 'disbursement_statuses': []}

def manifest(tmp_path, data=EMPTY):
    p = tmp_path / 'manifest.json'
    p.write_text(json.dumps(data))
    return p

def test_validate_only_no_output(tmp_path):
    report = run(manifest(tmp_path), XSD)
    assert report['mode'] == 'validate-only'
    assert report['message_count'] == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == ['manifest.json']

def test_opt_in_write(tmp_path):
    target = tmp_path / 'result'
    report = run(manifest(tmp_path), XSD, target)
    assert report['mode'] == 'write'
    assert json.loads((target / 'audit.json').read_text())['message_count'] == 0

def test_no_overwrite(tmp_path):
    target = tmp_path / 'result'
    target.mkdir()
    with pytest.raises(RuntimeError7, match='already exists'):
        run(manifest(tmp_path), XSD, target)

def test_rejects_unsupported_manifest(tmp_path):
    with pytest.raises(ValueError):
        run(manifest(tmp_path, {'sacco_ids': []}), XSD)

def test_rejects_missing_xsd(tmp_path):
    with pytest.raises(OSError):
        run(manifest(tmp_path), tmp_path / 'missing')
