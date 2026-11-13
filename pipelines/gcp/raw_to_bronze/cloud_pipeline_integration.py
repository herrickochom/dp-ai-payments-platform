"""Stage 14F-G: cloud integration boundary, default-deny and injected-only.

This module creates no SDK clients and has no live CLI. Cloud operations require
explicit injected clients, an immutable plan and a separately constructed permit.
No production acceptance is asserted; remote job recovery is intentionally blocked.
"""
from __future__ import annotations
import hashlib
import json
import re
from dataclasses import dataclass
from .cloud_service_adapters import InjectedGCSReader, table_id, job_id
from .offline_workflow import prepare_objects
from .execution_contracts import merge_sql
from .durable_execution_adapter import ContractError, RUN_ID, fingerprint_records

CANONICAL_BUCKET='dp-pdm-ai-platform-dev'
CANONICAL_BRONZE='bronze.payments_raw_v1'

@dataclass(frozen=True)
class PinnedSource:
    name: str
    generation: int
    size: int
    sha256: str

@dataclass(frozen=True)
class CloudPlan:
    run_id: str
    source_digest: str
    staging_table: str
    quarantine_table: str
    bronze_table: str
    stage_job_id: str
    quarantine_job_id: str
    merge_job_id: str
    merge_statement: str
    input_records: int
    staged_records: int
    quarantined_records: int
    duplicate_records: int
    staging_rows: tuple
    quarantine_rows: tuple


def _run_id(run_id):
    if not isinstance(run_id,str) or not RUN_ID.fullmatch(run_id):
        raise ContractError('invalid run id')
    return run_id


def _source_manifest(manifest):
    if not isinstance(manifest,list) or not manifest: raise ContractError('nonempty pinned manifest required')
    seen=set()
    for item in manifest:
        if not isinstance(item,PinnedSource): raise ContractError('PinnedSource required')
        if not re.fullmatch(r'raw/v1/[A-Za-z0-9_./=-]+\.avro',item.name) or '..' in item.name.split('/'):
            raise ContractError('noncanonical GCS source')
        if type(item.generation) is not int or item.generation<=0 or type(item.size) is not int or item.size<=0:
            raise ContractError('invalid pinned source generation or size')
        if not isinstance(item.sha256,str) or not re.fullmatch('[0-9a-f]{64}',item.sha256):
            raise ContractError('invalid source checksum')
        key=(item.name,item.generation)
        if key in seen: raise ContractError('duplicate pinned source')
        seen.add(key)
    return sorted(manifest,key=lambda x:(x.name,x.generation))


def build_plan(*,run_id,manifest,reader,bronze_table):
    """Read exact pinned generations through injected reader; no BigQuery calls."""
    _run_id(run_id)
    sources=_source_manifest(manifest)
    bronze=table_id(bronze_table)
    if not bronze.endswith('.'+CANONICAL_BRONZE): raise ContractError('noncanonical Bronze target')
    if not isinstance(reader,InjectedGCSReader) or reader.bucket!=CANONICAL_BUCKET:
        raise ContractError('canonical injected GCS reader required')
    objects=[]
    for source in sources:
        got=reader.read(source.name,source.generation)
        if got.name!=source.name or got.generation!=source.generation or got.bucket!=CANONICAL_BUCKET:
            raise ContractError('source version mismatch')
        if len(got.content)!=source.size or hashlib.sha256(got.content).hexdigest()!=source.sha256:
            raise ContractError('source content checksum or size mismatch')
        objects.append((f'gs://{CANONICAL_BUCKET}/{source.name}#generation={source.generation}',got.content))
    prepared=prepare_objects(objects)
    if not prepared['staging_rows']: raise ContractError('quarantine-only execution not supported')
    # Separate per-run tables: no cross-run staging contamination.
    suffix=hashlib.sha256(run_id.encode()).hexdigest()[:20]
    project=bronze.split('.')[0]
    staging=f'{project}.bronze.payments_raw_v1_staging_{suffix}'
    quarantine=f'{project}.bronze.payments_raw_v1_quarantine_{suffix}'
    # Stage rows carry run lineage; canonical Bronze schema remains unchanged.
    rows=[dict(row,_ingest_run_id=run_id) for row in prepared['staging_rows']]
    quarantine_rows=[]
    for item in prepared['quarantine']:
        quarantine_rows.append({
            'event_id':item.get('event_id'),
            'source_path':str(item.get('source_path') or item.get('_raw_object_path') or 'unknown'),
            'reason':str(item.get('reason') or item.get('error') or 'validation rejected'),
            'record_json':json.dumps(item,sort_keys=True,default=str),
            '_pipeline_version':'stage14fg',
            # _quarantined_at is assigned by the approved sink; never fabricate timestamps.
        })
    digest=hashlib.sha256(json.dumps([vars(s) for s in sources],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    sql=merge_sql(staging_table=staging,bronze_table=bronze,run_id=run_id)
    fp=fingerprint_records(prepared['staging_rows'])
    return CloudPlan(run_id,digest,staging,quarantine,bronze,
        job_id(run_id,'stage',digest+fp),
        # job_id permits only stage/merge/reconcile; use stage operation with distinct payload
        job_id(run_id,'stage','quarantine:'+digest),
        job_id(run_id,'merge',sql),sql,prepared['input_records'],len(rows),
        len(quarantine_rows),prepared['duplicate_records'],tuple(rows),tuple(quarantine_rows))


def validate_plan(plan):
    if not isinstance(plan,CloudPlan): raise ContractError('CloudPlan required')
    _run_id(plan.run_id)
    if not plan.staging_table.endswith('_'+hashlib.sha256(plan.run_id.encode()).hexdigest()[:20]):
        raise ContractError('staging table is not run-isolated')
    if plan.staged_records!=len(plan.staging_rows) or plan.quarantined_records!=len(plan.quarantine_rows):
        raise ContractError('plan count mismatch')
    if plan.input_records!=plan.staged_records+plan.quarantined_records+plan.duplicate_records:
        raise ContractError('source accounting mismatch')
    if any(row.get('_ingest_run_id')!=plan.run_id for row in plan.staging_rows):
        raise ContractError('staging run lineage mismatch')
    if merge_sql(staging_table=plan.staging_table,bronze_table=plan.bronze_table,run_id=plan.run_id)!=plan.merge_statement:
        raise ContractError('MERGE statement changed')
    return plan


def execution_gate(*,plan,approval=None):
    """Explicitly block all remote writes pending durable recovery and remote reconciliation.

    There is deliberately no bypass flag. This is not a live deployment script.
    """
    validate_plan(plan)
    raise ContractError('LIVE_BLOCKED: durable remote job recovery, quarantine persistence, and independent reconciliation not yet accepted')
