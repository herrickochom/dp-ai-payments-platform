"""Stage 14F-A: offline-only composition of reviewed MERGE contract and fake-client adapter.
No real BigQuery client, cloud reads/writes, GCS listing, or Dataflow launch.
Not a production ingest executor; staging row serialization and real sink checks remain gated.
"""
from pathlib import Path
from .execution_contracts import merge_sql, Reconciliation
from .durable_execution_adapter import OfflineAdapter, ContractError, fingerprint_records


def execute_offline(*, ledger, fake_client, run_id, staging_rows, staging_table,
                    bronze_table, input_records, quarantined_records, duplicate_records,
                    schema_path=None):
    if type(staging_rows) is not list:
        raise ContractError('staging_rows must be a materialized list')
    for n in (input_records, quarantined_records, duplicate_records):
        if type(n) is not int or n < 0:
            raise ContractError('invalid accounting count')
    # Validate counts and identities before preparing any ledger state.
    if input_records != len(staging_rows) + quarantined_records + duplicate_records:
        raise ContractError('source/staging/quarantine accounting mismatch')
    fingerprint_records(staging_rows)
    sql = merge_sql(staging_table=staging_table, bronze_table=bronze_table,
                    run_id=run_id, bronze_schema_path=schema_path)
    # The injected fake client is explicitly required; never instantiate cloud clients.
    if fake_client is None or ledger is None:
        raise ContractError('offline fake client and ledger required')
    status = OfflineAdapter(ledger, fake_client).execute(run_id, staging_rows, sql)
    state = ledger.get(run_id)
    if status == 'COMPLETE':
        if state['state'] != 'COMPLETE' or type(state['before_count']) is not int or type(state['after_count']) is not int:
            raise ContractError('missing durable completion evidence')
        inserted = state['after_count'] - state['before_count']
        if inserted < 0 or inserted > len(staging_rows):
            raise ContractError('invalid sink delta')
        rec = Reconciliation(input_records=input_records,
                             valid_records=len(staging_rows)+duplicate_records,
                             quarantined_records=quarantined_records,
                             duplicate_records=duplicate_records,
                             staged_records=len(staging_rows), inserted_records=inserted,
                             existing_records=len(staging_rows)-inserted).validate()
        return {'status': status, 'reconciliation': rec.as_dict(), 'merge_sql': sql}
    if status == 'ALREADY_COMPLETE':
        return {'status': status, 'reconciliation': None, 'merge_sql': sql}
    raise ContractError('unexpected adapter status')
