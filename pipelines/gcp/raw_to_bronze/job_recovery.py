"""Stage 14F-D: offline deterministic job recovery and reconciliation planning.

No cloud client creation or SDK invocation. A separate, explicitly approved
live runner is required; this module is not that runner.
"""
from __future__ import annotations
from dataclasses import dataclass
from .cloud_service_adapters import job_id, table_id
from .durable_execution_adapter import ContractError, fingerprint_records

@dataclass(frozen=True)
class JobPlan:
    run_id: str
    staging_table: str
    bronze_table: str
    fingerprint: str
    stage_job_id: str
    merge_job_id: str
    staged_records: int
    merge_sql: str

def plan(*, run_id, rows, staging_table, bronze_table, merge_sql):
    """Create immutable job identities from validated records and reviewed SQL."""
    if not isinstance(rows,list) or not rows:
        raise ContractError('nonempty validated rows required')
    fingerprint=fingerprint_records(rows)
    staging=table_id(staging_table); bronze=table_id(bronze_table)
    if staging==bronze: raise ContractError('staging and bronze must differ')
    if not isinstance(merge_sql,str) or not merge_sql.startswith('MERGE '):
        raise ContractError('reviewed MERGE required')
    if f'`{staging}`' not in merge_sql or f'`{bronze}`' not in merge_sql or f"'{run_id}'" not in merge_sql:
        raise ContractError('MERGE run/table scope mismatch')
    return JobPlan(run_id,staging,bronze,fingerprint,
        job_id(run_id,'stage',fingerprint),job_id(run_id,'merge',merge_sql),len(rows),merge_sql)

def recovery_decision(*, stage_state, merge_state, stage_rows, expected_rows,
                      bronze_before, bronze_after, inserted_rows):
    """Fail closed on ambiguous/partial jobs; never infer success from a delta alone.

    Inputs must be independently observed evidence from a trusted job registry
    and scoped sink queries, not assumptions or local success flags.
    """
    allowed={'NOT_FOUND','PENDING','RUNNING','DONE','FAILED','UNKNOWN'}
    if stage_state not in allowed or merge_state not in allowed:
        raise ContractError('invalid remote job state')
    for value in (stage_rows,expected_rows,bronze_before,bronze_after,inserted_rows):
        if type(value) is not int or value<0: raise ContractError('invalid observed count')
    if stage_state!='DONE' or merge_state!='DONE':
        raise ContractError('remote job incomplete or ambiguous; manual reconciliation required')
    if stage_rows!=expected_rows or bronze_after<bronze_before:
        raise ContractError('staging or bronze mismatch')
    if inserted_rows>expected_rows or inserted_rows!=bronze_after-bronze_before:
        raise ContractError('independent inserted count mismatch')
    return 'EVIDENCE_CONSISTENT_NOT_LIVE_ACCEPTED'
