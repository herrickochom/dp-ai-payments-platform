"""Stage 14F-I: SDK-shaped job execution, fail-closed and injection-only.

No SDK construction, CLI, network configuration or live authorisation.
A future separately approved entrypoint must supply a production permit and
perform independent remote reconciliation before declaring acceptance.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from .durable_execution_adapter import ContractError
from .cloud_pipeline_integration import validate_plan
from .remote_job_recovery import JobJournal, quarantine_payload, reconcile_evidence

@dataclass(frozen=True)
class ExecutionPermit:
    """A permit is scoped to one run, project, location and intended execution mode."""
    run_id: str
    bronze_table: str
    location: str
    mode: str


def _check(plan, journal, client, permit, configs):
    validate_plan(plan)
    if not isinstance(journal, JobJournal): raise ContractError('durable journal required')
    if client is None: raise ContractError('injected BigQuery client required')
    if not isinstance(permit, ExecutionPermit) or permit.mode != 'OFFLINE_FAKE_CLIENT':
        raise ContractError('LIVE_BLOCKED: explicit future production authorisation required')
    if (permit.run_id,permit.bronze_table)!=(plan.run_id,plan.bronze_table):
        raise ContractError('permit not scoped to this run and Bronze target')
    if permit.location!='europe-west2': raise ContractError('unapproved job location')
    if not isinstance(configs,dict) or set(configs)!={'stage','quarantine','merge'} or any(v is None for v in configs.values()):
        raise ContractError('explicit job configs required')
    # Fake-client only: a real google.cloud.bigquery.Client cannot be passed.
    if type(client).__module__.startswith(('google.', 'google_cloud_')) or not getattr(client,'__stage14fi_offline_fake__',False):
        raise ContractError('LIVE_BLOCKED: offline fake client required')
    return permit.location


def _submit_once(*, journal, plan, client, operation, rows, config, location):
    record=journal.get(plan.run_id,operation)
    if record is None: raise ContractError('unregistered job')
    if record['location']!=location: raise ContractError('job location mismatch')
    if record['state']=='DONE': return 'DONE'
    if record['state']!='PLANNED':
        # Recovery must observe by ID; NEVER automatically submit again.
        state=journal.observe(plan.run_id,operation,client=client)
        if state!='DONE': raise ContractError('remote job not conclusively DONE; retry blocked')
        return 'DONE'
    journal.mark_submitted(plan.run_id,operation)  # durable write BEFORE external call
    try:
        if operation=='merge':
            job=client.query(plan.merge_statement,job_id=record['job_id'],location=location,job_config=config)
        else:
            target=plan.staging_table if operation=='stage' else plan.quarantine_table
            job=client.load_table_from_json(rows,target,job_id=record['job_id'],location=location,job_config=config)
        if getattr(job,'job_id',None)!=record['job_id']:
            raise ContractError('remote job identity mismatch')
        job.result()
    except Exception as exc:
        # A timeout or SDK exception is ambiguous; journal stays SUBMITTED.
        raise ContractError('remote submission/result ambiguous; observe pinned job before recovery') from exc
    state=journal.observe(plan.run_id,operation,client=client)
    if state!='DONE': raise ContractError('remote job not conclusively DONE')
    return state


def execute_offline_fake(*,plan,journal,client,permit,configs,timestamp):
    """Run the actual three-operation SDK call sequence against an offline fake.

    Deliberately does not declare reconciliation complete. Cloud operations
    remain blocked: no production permit is accepted in this stage.
    """
    location=_check(plan,journal,client,permit,configs)
    if not plan.quarantine_rows:
        raise ContractError('zero-quarantine table creation not yet implemented; fail closed')
    qrows=quarantine_payload(plan,timestamp=timestamp)
    journal.register(plan,location=location)
    for operation,rows in (('stage',list(plan.staging_rows)),('quarantine',qrows),('merge',None)):
        _submit_once(journal=journal,plan=plan,client=client,operation=operation,
                     rows=rows,config=configs[operation],location=location)
    if journal.readiness(plan.run_id)!='REMOTE_JOBS_DONE_RECONCILIATION_REQUIRED':
        raise ContractError('remote jobs not complete')
    return {'status':'REMOTE_JOBS_DONE_RECONCILIATION_REQUIRED',
            'run_id':plan.run_id,'cloud_calls':0,'production_accepted':False}


def verify_offline_fake(*,plan,journal,client,permit,stage_count,quarantine_count,bronze_identities,quarantine_paths):
    """Caller supplies independently obtained observations; this method checks them.
    This function does not execute a BigQuery query or claim independence itself.
    """
    _check(plan,journal,client,permit,{'stage':object(),'quarantine':object(),'merge':object()})
    if journal.readiness(plan.run_id)!='REMOTE_JOBS_DONE_RECONCILIATION_REQUIRED':
        raise ContractError('job completion evidence required')
    return reconcile_evidence(plan=plan,observed_stage_count=stage_count,
        observed_quarantine_count=quarantine_count,
        observed_bronze_identities=bronze_identities,
        observed_quarantine_source_paths=quarantine_paths)
