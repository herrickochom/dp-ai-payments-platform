"""Fail-closed Dataflow initial submission guard.

Cross-system PostgreSQL/Dataflow atomic exactly-once submission is unavailable.
This adapter *never* infers permission to submit from an empty provider listing.
An external, provider-validated idempotency design is needed before production enablement.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any

class UnsafeSubmission(RuntimeError):
    pass

@dataclass(frozen=True)
class SubmissionIdentity:
    execution_id: str
    attempt_number: int
    project: str
    region: str
    job_name: str
    spec_digest: str

    def validate(self):
        import re
        if not re.fullmatch(r'[0-9a-f]{64}', self.execution_id):
            raise UnsafeSubmission('invalid execution ID')
        if not isinstance(self.attempt_number, int) or isinstance(self.attempt_number, bool) or self.attempt_number < 1:
            raise UnsafeSubmission('invalid attempt')
        if not re.fullmatch(r'[a-z][a-z0-9-]{2,62}', self.job_name):
            raise UnsafeSubmission('invalid Dataflow name')
        if not re.fullmatch(r'[0-9a-f]{64}', self.spec_digest):
            raise UnsafeSubmission('invalid spec digest')
        if not self.project or not self.region:
            raise UnsafeSubmission('missing project/region')
        return self


def assess_submission(identity: SubmissionIdentity, durable_state: str, visible_jobs: list[dict] | None) -> str:
    """Observe only; no ambiguous condition authorises external side effects."""
    identity.validate()
    if durable_state not in {'PREPARED','SUBMISSION_UNCERTAIN','OBSERVING','VERIFYING'}:
        raise UnsafeSubmission('unrecognised durable state')
    if visible_jobs is None or len(visible_jobs) == 0:
        return 'RECONCILE_ONLY'
    if len(visible_jobs) != 1:
        return 'QUARANTINE'
    job=visible_jobs[0]
    if (job.get('name'),job.get('project'),job.get('region'),job.get('spec_digest')) != (identity.job_name,identity.project,identity.region,identity.spec_digest):
        return 'QUARANTINE'
    return 'OBSERVE_EXISTING' if job.get('id') else 'QUARANTINE'


def submit(*args: Any, **kwargs: Any) -> None:
    raise UnsafeSubmission('Dataflow submission disabled pending verified provider idempotency and durable atomic handoff')
