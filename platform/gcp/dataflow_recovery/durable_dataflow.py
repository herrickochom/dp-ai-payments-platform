"""Stage 9 offline reference contract; no GCP SDK calls or database migrations.

Integrate with Stage 7/8 PostgreSQL ledger through the Ledger protocol.
A stable name is not a substitute for checking the provider's idempotency semantics.
"""
from dataclasses import dataclass
from enum import Enum
import hashlib
import re
from typing import Protocol

class Decision(str, Enum):
    SUBMIT = 'SUBMIT'
    OBSERVE = 'OBSERVE'
    COMPLETE = 'COMPLETE'
    RECOVER = 'RECOVER'
    WAIT = 'WAIT'
    QUARANTINE = 'QUARANTINE'

@dataclass(frozen=True)
class Intent:
    execution_key: str
    job_name: str
    project: str
    region: str
    spec_digest: str

@dataclass(frozen=True)
class Observation:
    job_id: str
    job_name: str
    state: str
    project: str
    region: str
    spec_digest: str | None = None

class Provider(Protocol):
    def find_by_name(self, intent: Intent) -> list[Observation]: ...
    def submit(self, intent: Intent) -> Observation: ...

class Ledger(Protocol):
    def record_submission(self, key: str, fencing_token: str, job_id: str) -> bool: ...
    def mark_complete(self, key: str, fencing_token: str, job_id: str, evidence_digest: str) -> bool: ...

class Evidence(Protocol):
    def verify(self, intent: Intent, job: Observation) -> str | None: ...

class UnsafeSubmission(RuntimeError): pass

TERMINAL_FAILURE = {'JOB_STATE_FAILED', 'JOB_STATE_CANCELLED', 'JOB_STATE_DRAINED'}

def make_intent(execution_key: str, project: str, region: str, spec_digest: str) -> Intent:
    if not all((execution_key, project, region, spec_digest)):
        raise ValueError('all identity inputs required')
    slug = re.sub(r'[^a-z0-9-]', '-', execution_key.lower()).strip('-')[:35].rstrip('-') or 'execution'
    suffix = hashlib.sha256(f'{execution_key}\0{project}\0{region}'.encode()).hexdigest()[:16]
    return Intent(execution_key, f'pdm-{slug}-{suffix}', project, region, spec_digest)

def reconcile(intent: Intent, jobs: list[Observation], evidence: Evidence) -> tuple[Decision, Observation | None, str | None]:
    """Pure reconciliation; caller must hold a live fenced PostgreSQL lease."""
    if len(jobs) > 1:
        return Decision.QUARANTINE, None, None
    if not jobs:
        return Decision.SUBMIT, None, None
    job = jobs[0]
    if (job.job_name, job.project, job.region) != (intent.job_name, intent.project, intent.region):
        return Decision.QUARANTINE, job, None
    if job.spec_digest is None or job.spec_digest != intent.spec_digest:
        return Decision.QUARANTINE, job, None
    if job.state == 'JOB_STATE_DONE':
        digest = evidence.verify(intent, job)
        return (Decision.COMPLETE, job, digest) if digest else (Decision.WAIT, job, None)
    if job.state in TERMINAL_FAILURE:
        return Decision.RECOVER, job, None
    return Decision.OBSERVE, job, None

def run_once(intent: Intent, token: str, provider: Provider, ledger: Ledger, evidence: Evidence, *, lease_valid: bool, submission_uncertain: bool = False) -> Decision:
    """One bounded step. Caller must persist intent before calling this function.

    An uncertain submission must be reconciled until provider visibility is conclusive;
    never resubmit solely because find_by_name temporarily returns no jobs.
    """
    if not lease_valid or not token:
        return Decision.WAIT
    decision, job, digest = reconcile(intent, provider.find_by_name(intent), evidence)
    if decision == Decision.SUBMIT:
        if submission_uncertain:
            return Decision.WAIT
        # External adapter MUST enforce provider-side single-flight/duplicate rejection.
        # Ledger fencing alone cannot atomically fence a remote API call.
        try:
            job = provider.submit(intent)
        except (TimeoutError, ConnectionError):
            return Decision.WAIT
        if (job.job_name, job.project, job.region, job.spec_digest) != (intent.job_name, intent.project, intent.region, intent.spec_digest):
            return Decision.QUARANTINE
        return Decision.OBSERVE if ledger.record_submission(intent.execution_key, token, job.job_id) else Decision.QUARANTINE
    if decision == Decision.COMPLETE:
        return Decision.COMPLETE if ledger.mark_complete(intent.execution_key, token, job.job_id, digest) else Decision.QUARANTINE
    return decision
