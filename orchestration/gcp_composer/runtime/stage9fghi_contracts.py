"""Stage 9F-I offline adapters and readiness gates. NO cloud SDK/network access.

This module deliberately does not submit Dataflow jobs. Cloud adapter interfaces
are contracts only; production enablement requires provider semantics validation.
"""
from dataclasses import dataclass
import hashlib
import json
import re
from typing import Protocol

class UnsafeToDeploy(RuntimeError): pass

@dataclass(frozen=True)
class JobIdentity:
    execution_id: str
    attempt: int
    project: str
    region: str
    job_name: str
    spec_digest: str

    def validate(self):
        if not re.fullmatch(r'[0-9a-f]{64}', self.execution_id): raise ValueError('execution ID must be SHA256')
        if not isinstance(self.attempt,int) or isinstance(self.attempt,bool) or self.attempt < 1: raise ValueError('attempt invalid')
        if not re.fullmatch(r'[a-z][a-z0-9-]{2,62}', self.job_name): raise ValueError('job name invalid')
        if not re.fullmatch(r'[0-9a-f]{64}', self.spec_digest): raise ValueError('spec digest invalid')
        if not self.project or not self.region: raise ValueError('project and region required')
        return self

class DataflowProvider(Protocol):
    def list_matching(self, identity: JobIdentity) -> list[dict] | None: ...
    def get_job(self, identity: JobIdentity, job_id: str) -> dict: ...
    # Submission intentionally omitted pending verified provider-side deduplication.

class BigQueryEvidence(Protocol):
    def inspect_outputs(self, identity: JobIdentity, job_id: str) -> dict: ...


def classify_jobs(identity: JobIdentity, jobs: list[dict] | None):
    """Missing visibility is UNKNOWN, never permission to resubmit."""
    identity.validate()
    if jobs is None or not jobs: return 'UNKNOWN', None
    if len(jobs) != 1: return 'QUARANTINE', None
    job=jobs[0]
    expected=(identity.project,identity.region,identity.job_name,identity.spec_digest)
    actual=(job.get('project'),job.get('region'),job.get('name'),job.get('spec_digest'))
    if actual != expected or not job.get('id'): return 'QUARANTINE', None
    return 'FOUND', job


def verified_evidence(identity: JobIdentity, job: dict, result: dict) -> dict:
    """Check explicit manifest binding and BigQuery verification, then digest evidence."""
    identity.validate()
    if job.get('state') != 'JOB_STATE_DONE': raise UnsafeToDeploy('Dataflow not DONE')
    if (job.get('project'),job.get('region'),job.get('name'),job.get('spec_digest')) != (identity.project,identity.region,identity.job_name,identity.spec_digest):
        raise UnsafeToDeploy('Dataflow identity mismatch')
    if not job.get('id'): raise UnsafeToDeploy('Dataflow job ID missing')
    required={'execution_id':identity.execution_id,'attempt_number':identity.attempt,
              'dataflow_job_id':job['id'],'spec_digest':identity.spec_digest}
    if any(result.get(k)!=v for k,v in required.items()): raise UnsafeToDeploy('Evidence identity mismatch')
    if result.get('bigquery_verified') is not True or result.get('manifest_verified') is not True:
        raise UnsafeToDeploy('BigQuery and manifest verification required')
    if not isinstance(result.get('output_objects'),list) or not result['output_objects']:
        raise UnsafeToDeploy('Output object evidence required')
    for obj in result['output_objects']:
        if not isinstance(obj,dict) or not obj.get('uri') or not re.fullmatch(r'[0-9a-f]{64}',str(obj.get('sha256',''))):
            raise UnsafeToDeploy('Invalid output object evidence')
    canonical=json.dumps(result,sort_keys=True,separators=(',',':')).encode()
    return {'evidence_digest':hashlib.sha256(canonical).hexdigest(),'evidence':result}


def deployment_gate(checks: dict):
    """Fail closed. This does not authorise GCP deployment."""
    required=('postgres_live_tests','crash_concurrency_tests','provider_duplicate_semantics',
              'provider_visibility_reconciliation','bigquery_evidence_integration',
              'composer_automatic_scheduler','least_privilege_iam','budget_guardrails',
              'secrets_management','stage8_regression','stage9_regression','explicit_gcp_approval')
    missing=[key for key in required if checks.get(key) is not True]
    return {'ready':not missing,'missing':missing}


def orchestration_tick(store, provider, verifier, worker='stage9-reconciler'):
    """Bounded reconciliation tick using existing Stage 9E worker. Never submits.

    Scheduler is external; no manual production triggering is intended.
    """
    from stage9_offline_orchestrator import tick
    return tick(store,provider,verifier,worker=worker)
