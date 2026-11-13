"""14F-E: SDK-compatible, injected-client remote observation. No live runner.

Calling functions with real SDK clients would perform cloud reads; there is no
CLI or client construction here. Tests use fakes exclusively. No writes.
"""
from __future__ import annotations
from dataclasses import dataclass
from .cloud_service_adapters import InjectedGCSReader, AdapterContractError, table_id
from .durable_execution_adapter import ContractError
from .job_recovery import recovery_decision

@dataclass(frozen=True)
class SourceVersion:
    name: str
    generation: int
    size: int

def fetch_pinned(reader: InjectedGCSReader, manifest: list[SourceVersion]):
    """Fetch exact pinned object generations; reject missing or changed content."""
    if not isinstance(reader,InjectedGCSReader) or not isinstance(manifest,list) or not manifest:
        raise ContractError('explicit injected reader and nonempty source manifest required')
    seen=set()
    objects=[]
    for item in manifest:
        if not isinstance(item,SourceVersion) or type(item.generation) is not int or item.generation<=0 or type(item.size) is not int or item.size<=0:
            raise ContractError('invalid source version')
        if (item.name,item.generation) in seen:
            raise ContractError('duplicate source version')
        seen.add((item.name,item.generation))
        obj=reader.read(item.name,item.generation)
        if obj.name!=item.name or obj.generation!=item.generation or len(obj.content)!=item.size:
            raise ContractError('source version/size mismatch')
        objects.append((obj.name,obj.content))
    return objects

def observe_job(client, *, job_id, location):
    """Read remote job registry only; no retry, submission or job creation."""
    if client is None or not isinstance(job_id,str) or not job_id.startswith('pdm_') or not isinstance(location,str) or not location:
        raise ContractError('invalid job observation request')
    try:
        job=client.get_job(job_id,location=location)
    except Exception as exc:
        # Missing and permission errors are not proof of no prior execution.
        raise ContractError('remote job not conclusively observable') from exc
    if job is None or getattr(job,'job_id',None)!=job_id:
        raise ContractError('job identity mismatch')
    if getattr(job,'error_result',None) or getattr(job,'errors',None):
        return 'FAILED'
    state=getattr(job,'state',None)
    if state not in ('PENDING','RUNNING','DONE'):
        raise ContractError('unknown remote job state')
    return state

def observe_count(client, *, table, run_id, job_config):
    """Explicit read-only run-scoped count; caller must supply safe job config.

    Deliberately does not call client.query: no implicit cloud calls permitted.
    Returns a validated SQL string for separately authorised execution.
    """
    t=table_id(table)
    if not isinstance(run_id,str) or not run_id or len(run_id)>96 or not all(c.isalnum() or c in '_-' for c in run_id) or not run_id.isascii():
        raise ContractError('invalid run id')
    if job_config is None:
        raise ContractError('explicit job configuration required')
    return f"SELECT COUNT(*) AS n FROM `{t}` WHERE `_ingest_run_id` = '{run_id}'"

def accept_observed(*, stage_state, merge_state, stage_rows, expected_rows,
                    bronze_before, bronze_after, inserted_rows):
    """Reuses existing strict reconciliation; not a live acceptance decision."""
    return recovery_decision(stage_state=stage_state,merge_state=merge_state,
        stage_rows=stage_rows,expected_rows=expected_rows,
        bronze_before=bronze_before,bronze_after=bronze_after,inserted_rows=inserted_rows)
