"""Fenced one-attempt Dataflow Flex Template submission protocol.

IMPORTANT: An ambiguous provider response is never retried. This adapter does
not claim exactly-once external effects. Caller must supply an approved,
immutable Flex Template spec, verified billing and a durable lease claim.
"""
from __future__ import annotations
from dataclasses import dataclass
from submission_guard import SubmissionIdentity, UnsafeSubmission
from cost_guard import require_approval

class SubmissionBlocked(UnsafeSubmission): pass

@dataclass(frozen=True)
class LaunchConfig:
    template_gcs_path: str
    parameters: dict
    worker_service_account: str
    subnetwork: str
    temp_location: str
    max_workers: int = 1
    machine_type: str = 'e2-small'

    def validate(self):
        if not self.template_gcs_path.startswith('gs://') or not self.template_gcs_path.endswith('.json'):
            raise SubmissionBlocked('immutable approved Flex Template path required')
        if not isinstance(self.parameters,dict) or not self.parameters:
            raise SubmissionBlocked('template parameters required')
        if any(not isinstance(k,str) or not isinstance(v,str) for k,v in self.parameters.items()):
            raise SubmissionBlocked('template parameters must be strings')
        if not self.worker_service_account.endswith('.iam.gserviceaccount.com'):
            raise SubmissionBlocked('worker service account required')
        if not self.subnetwork.startswith('regions/') and not self.subnetwork.startswith('https://www.googleapis.com/compute/v1/projects/'):
            raise SubmissionBlocked('explicit subnetwork required')
        if not self.temp_location.startswith('gs://'):
            raise SubmissionBlocked('temp bucket required')
        if self.max_workers != 1 or self.machine_type != 'e2-small':
            raise SubmissionBlocked('unapproved Dataflow worker shape')
        return self

def build_launch_request(identity:SubmissionIdentity,config:LaunchConfig):
    identity.validate(); config.validate()
    if identity.project != 'project-e48f194e-f44f-458d-b4f' or identity.region != 'europe-west2':
        raise SubmissionBlocked('unapproved GCP scope')
    # Dataflow labels have a 63-character value maximum. Full SHA-256 hashes
    # cannot be represented as Dataflow labels; keep full identity in Postgres.
    labels={'pdm_pilot':'stage10','pdm_exec_ref':identity.execution_id[:32],
            'pdm_spec_ref':identity.spec_digest[:32]}
    return {'project_id':identity.project,'location':identity.region,
      'launch_parameter':{'job_name':identity.job_name,
        'container_spec_gcs_path':config.template_gcs_path,
        'parameters':dict(config.parameters),
        'environment':{'max_workers':1,'num_workers':1,
          'machine_type':config.machine_type,
          'service_account_email':config.worker_service_account,
          'subnetwork':config.subnetwork,'temp_location':config.temp_location,
          'additional_user_labels':labels}}}

def submit_one_attempt(*,store,provider,claim,identity,config,cost_inputs,execute=False):
    """Never launch without a fresh fenced PREPARED claim and explicit approval.

    The caller must disable SDK automatic retries for the launch call. The
    network result may be ambiguous; durable state stays UNCERTAIN regardless.
    """
    identity.validate(); request=build_launch_request(identity,config)
    if claim.get('state')!='PREPARED' or claim.get('execution_id')!=identity.execution_id or \
       claim.get('attempt_number')!=identity.attempt_number or claim.get('digest')!=identity.spec_digest or \
       claim.get('job_name')!=identity.job_name or claim.get('project')!=identity.project or \
       claim.get('region')!=identity.region or not claim.get('token'):
        raise SubmissionBlocked('fenced durable PREPARED claim required')
    decision=require_approval(project=identity.project,region=identity.region,
       workers=1,runtime_minutes=30,**cost_inputs)
    if decision['decision']!='ALLOW_PLANNING_ONLY':
        raise SubmissionBlocked('cost policy denied')
    if not execute:
        return {'decision':'DRY_RUN','mutated':False,'request':request}
    # Critical: durable transition committed BEFORE external side effect.
    store.transition(claim,'PREPARED','SUBMISSION_UNCERTAIN',delay_seconds=60)
    try:
        # SDK retries disabled: no automatic second POST on ambiguous timeout.
        result=provider.launch_flex_template(request=request,retry=None,timeout=60)
    except Exception as exc:
        raise SubmissionBlocked('provider response uncertain: reconcile; never auto-resubmit') from exc
    # Provider success is NOT durable completion; next reconciliation observes
    # exact job and output evidence.
    return {'decision':'SUBMISSION_UNCERTAIN_RECONCILE','mutated':True,
            'provider_job_id':str(getattr(getattr(result,'job',None),'id',''))}
