#!/usr/bin/env python3
"""KP-014D offline-only transaction admission simulator. NOT an Iceberg commit."""
import argparse
import hashlib
import json
import sqlite3
from pathlib import Path

EXPECTED_SOURCE_SHA = 'b78d55895754312a25392706b5bc2072be9f48ce74ba7905bd1807ddb8483654'
EXPECTED_ROWS = 20403


def inspect(source, export_manifest, stage_manifest, parquet_path):
    import pyarrow.parquet as pq
    source = Path(source)
    export = json.loads(Path(export_manifest).read_text())
    stage = json.loads(Path(stage_manifest).read_text())
    digest = hashlib.file_digest(source.open('rb'), 'sha256').hexdigest()
    table = pq.read_table(parquet_path)
    assert digest == EXPECTED_SOURCE_SHA == export['sha256'] == stage['source_sha256'], 'source SHA mismatch'
    assert export['rows'] == stage['input_lines'] == stage['accepted'] == table.num_rows == EXPECTED_ROWS, 'row count mismatch'
    assert stage['quarantined'] == 0 and table.num_columns == 25, 'schema or quarantine mismatch'
    assert export['unique_event_ids'] == export['unique_kafka_identities'] == EXPECTED_ROWS, 'source uniqueness mismatch'
    assert stage['unique_kafka_identities_in_accepted'] == EXPECTED_ROWS, 'staging uniqueness mismatch'
    assert stage['iceberg_catalog_commits'] == 0, 'unexpected commit claim'
    ids = set()
    events = set()
    for row in table.select(['event_id', '_kafka_topic', '_kafka_partition', '_kafka_offset']).to_pylist():
        key = (row['_kafka_topic'], row['_kafka_partition'], row['_kafka_offset'])
        assert key not in ids, 'duplicate Kafka identity in parquet'
        assert row['event_id'] not in events, 'duplicate event ID in parquet'
        ids.add(key)
        events.add(row['event_id'])
    return {'source_sha256': digest, 'rows': len(ids), 'parquet_schema_fields': table.num_columns,
            'status': 'VALIDATED_OFFLINE_NO_ICEBERG_COMMIT'}


def init_db(conn):
    conn.execute('CREATE TABLE IF NOT EXISTS runs (source_sha TEXT PRIMARY KEY, status TEXT NOT NULL CHECK(status IN (\'PREPARED\',\'COMMITTED_SIMULATED\')), row_count INTEGER NOT NULL)')
    conn.execute('CREATE TABLE IF NOT EXISTS identities (topic TEXT NOT NULL, partition INTEGER NOT NULL, offset INTEGER NOT NULL, source_sha TEXT NOT NULL, PRIMARY KEY(topic,partition,offset))')
    conn.execute('CREATE TABLE IF NOT EXISTS event_ids (event_id TEXT PRIMARY KEY, source_sha TEXT NOT NULL)')


def admit(conn, digest, rows):
    """Atomically reserve identities; no cloud writes. PREPARED remains recoverable."""
    with conn:
        existing = conn.execute('SELECT status, row_count FROM runs WHERE source_sha=?', (digest,)).fetchone()
        if existing:
            assert existing[1] == len(rows), 'existing source row count conflict'
            return 'ALREADY_' + existing[0]
        conn.execute('INSERT INTO runs VALUES (?, ?, ?)', (digest, 'PREPARED', len(rows)))
        for row in rows:
            conn.execute('INSERT INTO identities VALUES (?,?,?,?)', (row['_kafka_topic'], row['_kafka_partition'], row['_kafka_offset'], digest))
            conn.execute('INSERT INTO event_ids VALUES (?,?)', (row['event_id'], digest))
    return 'PREPARED_OFFLINE_ONLY'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', default='evidence/iceberg-catalogue-and-commit-validation')
    p.add_argument('--ledger', default='evidence/iceberg-catalogue-and-commit-validation/kp014d-offline-ledger.sqlite3')
    p.add_argument('--record', action='store_true', help='write local SQLite admission ledger; NEVER commits Iceberg')
    a = p.parse_args()
    root = Path(a.root)
    source = root / 'bronze-full-20403/bronze.jsonl'
    exp = root / 'bronze-full-20403/manifest.json'
    stage = root / 'parquet-full-20403/manifest.json'
    parquet = root / 'parquet-full-20403/validated_staging.parquet'
    result = inspect(source, exp, stage, parquet)
    if a.record:
        import pyarrow.parquet as pq
        ledger = Path(a.ledger)
        assert ledger.suffix == '.sqlite3', 'ledger must be SQLite file'
        assert ledger.parent.is_dir(), 'ledger parent must already exist'
        with sqlite3.connect(ledger, timeout=10) as conn:
            init_db(conn)
            rows = pq.read_table(parquet, columns=['event_id','_kafka_topic','_kafka_partition','_kafka_offset']).to_pylist()
            result['admission'] = admit(conn, result['source_sha256'], rows)
    else:
        result['admission'] = 'DRY_RUN_NO_LEDGER_WRITES'
    result.update(cloud_calls=0, iceberg_commits=0, iceberg_snapshot_created=False,
                  cross_run_protection='LOCAL_SQLITE_SIMULATION_ONLY')
    print(json.dumps(result, indent=2, sort_keys=True))

if __name__ == '__main__':
    main()
