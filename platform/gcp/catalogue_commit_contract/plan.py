"""KP-014E: offline-only proposed BigLake Iceberg catalogue contract and commit intent.
No REST client, cloud authentication, network calls, Iceberg writes, or ledger mutations.
"""
import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

EXPECTED_SHA = 'b78d55895754312a25392706b5bc2072be9f48ce74ba7905bd1807ddb8483654'
DEFAULT_ROOT = Path('evidence/iceberg-catalogue-and-commit-validation')
CONTRACT = {
    'catalogue_name': 'pdm-payments-iceberg-dev',
    'namespace': 'bronze',
    'table': 'payment_events',
    'proposed_storage_prefix': 'gs://dp-pdm-ai-platform-dev/lakehouse/iceberg/payments/bronze/',
    'region': 'europe-west2',
    'credential_mode_proposed': 'CREDENTIAL_MODE_END_USER',
    'catalogue_endpoint': 'UNVERIFIED_REQUIRES_OFFICIAL_PROVIDER_CONTRACT',
    'table_registration': 'NOT_PERFORMED',
    'catalogue_optimistic_concurrency': 'NOT_TESTED',
    'parquet_files_in_gcs': False,
    'commit_authorized': False,
}

def build(root=DEFAULT_ROOT, ledger=None):
    source = root/'bronze-full-20403/bronze.jsonl'
    exported = json.loads((root/'bronze-full-20403/manifest.json').read_text())
    staged = json.loads((root/'parquet-full-20403/manifest.json').read_text())
    parquet = root/'parquet-full-20403/validated_staging.parquet'
    import pyarrow.parquet as pq
    metadata = pq.read_metadata(parquet)
    digest = hashlib.file_digest(source.open('rb'), 'sha256').hexdigest()
    assert digest == EXPECTED_SHA == exported['sha256'] == staged['source_sha256'], 'source digest mismatch'
    assert exported['rows'] == staged['accepted'] == staged['input_lines'] == metadata.num_rows == 20403, 'row mismatch'
    assert exported['unique_event_ids'] == exported['unique_kafka_identities'] == staged['unique_kafka_identities_in_accepted'] == 20403, 'identity count mismatch'
    assert staged['quarantined'] == 0 and metadata.num_columns == staged['schema_fields'] == 25, 'schema or quarantine mismatch'
    assert staged['iceberg_catalog_commits'] == 0, 'unexpected Iceberg commit evidence'
    parquet_sha = hashlib.file_digest(parquet.open('rb'), 'sha256').hexdigest()
    admission = 'NOT_CHECKED'
    if ledger is not None:
        assert ledger.is_file(), 'ledger not found'
        # Immutable read-only SQLite URI: no schema changes or journal creation.
        with sqlite3.connect(f'file:{ledger.resolve()}?mode=ro&immutable=1', uri=True) as conn:
            matches = conn.execute('SELECT status,row_count FROM runs WHERE source_sha=?', (digest,)).fetchall()
            assert matches == [('PREPARED', 20403)], 'ledger admission mismatch'
            identities = conn.execute('SELECT COUNT(*) FROM identities WHERE source_sha=?', (digest,)).fetchone()[0]
            event_ids = conn.execute('SELECT COUNT(*) FROM event_ids WHERE source_sha=?', (digest,)).fetchone()[0]
            assert identities == event_ids == 20403, 'ledger identities mismatch'
            admission = 'PREPARED_LOCAL_ONLY'
    intent = {
        'status': 'OFFLINE_COMMIT_INTENT_NOT_EXECUTABLE',
        'source_sha256': digest,
        'parquet_sha256': parquet_sha,
        'rows': metadata.num_rows,
        'fields': metadata.num_columns,
        'ledger_admission': admission,
        'catalogue_contract': CONTRACT,
        'preconditions_not_yet_met': [
            'verify official BigLake Iceberg REST endpoint and supported authentication',
            'confirm API availability, IAM permissions, quotas and cost approval',
            'confirm Terraform plan and catalogue/table creation approval',
            'implement actual Iceberg data/manifest/metadata write and atomic catalogue commit',
            'implement remote commit status reconciliation after ambiguous failures',
            'implement distributed conflict detection and retry fencing',
        ],
        'cloud_calls': 0,
        'gcs_writes': 0,
        'iceberg_commits': 0,
    }
    intent['intent_sha256'] = hashlib.sha256(json.dumps(intent,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return intent

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ledger',type=Path,default=Path('evidence/iceberg-catalogue-and-commit-validation/kp014d-offline-ledger.sqlite3'))
    p.add_argument('--output',type=Path,help='new JSON file only; default prints to stdout')
    a=p.parse_args()
    result=build(ledger=a.ledger)
    payload=json.dumps(result,indent=2,sort_keys=True)+'\n'
    if a.output:
        assert a.output.parent.is_dir(), 'output parent must exist'
        with a.output.open('x',encoding='utf-8') as f: f.write(payload)
        print('PASS: wrote offline-only intent:',a.output)
    else: print(payload,end='')

if __name__=='__main__': main()
