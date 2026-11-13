"""14F-L: local acceptance gate, not a cloud production acceptance certificate."""
from __future__ import annotations
from .durable_execution_adapter import ContractError
from .cloud_pipeline_integration import validate_plan
from .remote_job_recovery import JobJournal
from .remote_job_reconciliation import execute_offline_reconciliation

def accept_offline(*,plan,journal,query_client,configs):
    validate_plan(plan)
    if not isinstance(journal,JobJournal):raise ContractError('journal required')
    if journal.readiness(plan.run_id)!='REMOTE_JOBS_DONE_RECONCILIATION_REQUIRED':raise ContractError('jobs not completed')
    evidence=execute_offline_reconciliation(plan=plan,client=query_client,configs=configs)
    if evidence.get('production_accepted') is not False or evidence.get('cloud_calls')!=0:raise ContractError('invalid offline evidence')
    return {'status':'STAGE14FL_OFFLINE_ACCEPTED_LIVE_NOT_RUN','run_id':plan.run_id,'source_records':evidence['source_records'],'bronze_identities_verified':evidence['bronze_identities_verified'],'quarantine_verified':evidence['quarantine_verified'],'production_accepted':False,'cloud_calls':0}

def production_acceptance(*args,**kwargs):
    raise ContractError('LIVE_BLOCKED: independent GCP execution and evidence require explicit approval')
