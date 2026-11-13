"""14F-K: production-shaped BigQuery adapter, offline-only entrypoint.

The module does not create credentials or clients. A caller may supply an SDK
client only through a separately reviewed deployment. This stage's runner
never submits cloud work. Every operation is journaled before submission.
"""
from __future__ import annotations
from datetime import datetime,timezone
from .durable_execution_adapter import ContractError
from .cloud_pipeline_integration import validate_plan
from .remote_job_recovery import JobJournal,quarantine_payload

QUARANTINE_COLUMNS=(('event_id','STRING'),('source_path','STRING'),('reason','STRING'),('record_json','STRING'),('_quarantined_at','TIMESTAMP'),('_pipeline_version','STRING'))

def quarantine_ddl(plan):
    validate_plan(plan)
    cols=', '.join(f'`{name}` {kind}' for name,kind in QUARANTINE_COLUMNS)
    return f'CREATE TABLE IF NOT EXISTS `{plan.quarantine_table}` ({cols})'

def stage_schema(bronze_fields):
    """Add only staging lineage to the canonical schema; do not mutate input."""
    if not isinstance(bronze_fields,list) or not bronze_fields:raise ContractError('Bronze schema required')
    if any(not isinstance(x,dict) or not isinstance(x.get('name'),str) for x in bronze_fields):raise ContractError('invalid schema')
    names=[x['name'] for x in bronze_fields]
    if len(set(names))!=len(names) or '_ingest_run_id' in names:raise ContractError('invalid staging fields')
    return [dict(x) for x in bronze_fields]+[{'name':'_ingest_run_id','type':'STRING','mode':'REQUIRED'}]

def submit_with_journal(*,plan,journal,client,configs,location,timestamp,offline_only=True):
    """SDK-shaped submission; intentionally refuses real clients in this stage.

    This is a verified integration adapter, not an authorisation mechanism.
    The caller cannot turn on live mode via a boolean switch.
    """
    validate_plan(plan)
    if not isinstance(journal,JobJournal):raise ContractError('durable journal required')
    if location!='europe-west2':raise ContractError('unapproved region')
    if not offline_only or not getattr(client,'__stage14fk_offline_fake__',False) or type(client).__module__.startswith('google.'):
        raise ContractError('LIVE_BLOCKED: production SDK calls require separate approval')
    if not isinstance(configs,dict) or set(configs)!= {'stage','quarantine','merge'} or any(v is None for v in configs.values()):raise ContractError('explicit configs required')
    if not isinstance(timestamp,datetime) or timestamp.tzinfo is None:raise ContractError('UTC timestamp required')
    journal.register(plan,location=location)
    operations=(('stage',list(plan.staging_rows)),('quarantine',quarantine_payload(plan,timestamp=timestamp)),('merge',None))
    for operation,rows in operations:
        record=journal.get(plan.run_id,operation)
        if record['state']=='DONE':continue
        if record['state']!='PLANNED':
            if journal.observe(plan.run_id,operation,client=client)!='DONE':raise ContractError('uncertain job; retry blocked')
            continue
        journal.mark_submitted(plan.run_id,operation)
        try:
            if operation=='stage':
                job=client.load_table_from_json(rows,plan.staging_table,job_id=record['job_id'],location=location,job_config=configs['stage'])
            elif operation=='quarantine' and rows:
                job=client.load_table_from_json(rows,plan.quarantine_table,job_id=record['job_id'],location=location,job_config=configs['quarantine'])
            elif operation=='quarantine':
                job=client.query(quarantine_ddl(plan),job_id=record['job_id'],location=location,job_config=configs['quarantine'])
            else:
                job=client.query(plan.merge_statement,job_id=record['job_id'],location=location,job_config=configs['merge'])
            if getattr(job,'job_id',None)!=record['job_id']:raise ContractError('job ID mismatch')
            job.result()
        except Exception as exc:
            raise ContractError('job submission ambiguous; observe pinned job before retry') from exc
        if journal.observe(plan.run_id,operation,client=client)!='DONE':raise ContractError('job completion unverified')
    if journal.readiness(plan.run_id)!='REMOTE_JOBS_DONE_RECONCILIATION_REQUIRED':raise ContractError('reconciliation prerequisite failed')
    return {'status':'OFFLINE_FAKE_JOBS_DONE_RECONCILIATION_REQUIRED','production_accepted':False,'cloud_calls':0}
