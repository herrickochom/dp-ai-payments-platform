"""Fail-closed, explicitly opt-in Dataflow cancellation adapter.

No automatic scheduling or cost-based launch permission is implied. Provider
cancellation is allowed only after a fresh exact-ID get and ownership checks.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from bounded_watchdog import WatchdogScope, WatchdogBlocked, assess_job

@dataclass(frozen=True)
class CancellationTarget:
    project: str
    region: str
    job_id: str
    job_name: str
    execution_id: str
    spec_digest: str


def verify_target(job, target: CancellationTarget, *, now: datetime, scope=WatchdogScope()):
    if (target.project, target.region) != (scope.project, scope.region):
        raise WatchdogBlocked('unexpected project/region')
    if not re.fullmatch(r'[0-9a-f]{64}', target.execution_id) or not re.fullmatch(r'[0-9a-f]{64}', target.spec_digest):
        raise WatchdogBlocked('invalid execution/spec identity')
    if not target.job_id or not re.fullmatch(r'[a-z][a-z0-9-]{2,62}', target.job_name):
        raise WatchdogBlocked('invalid provider job identity')
    if str(getattr(job, 'id', '')) != target.job_id or str(getattr(job, 'name', '')) != target.job_name:
        raise WatchdogBlocked('provider job identity mismatch')
    labels = dict(getattr(job, 'labels', {}) or {})
    # Label names must be valid for Dataflow; these are installed by the launcher
    # once that launcher is independently accepted.
    if labels.get('pdm_execution') != target.execution_id or labels.get('pdm_spec') != target.spec_digest:
        raise WatchdogBlocked('execution/spec labels missing or mismatched')
    decision = assess_job(job, project=target.project, region=target.region, now=now, scope=scope)
    if decision != 'EXPIRED':
        raise WatchdogBlocked(f'job is not an eligible expired pilot: {decision}')
    return True


def cancel_exact_expired(client, target: CancellationTarget, *, now=None, execute=False, approval=False):
    """Read provider by exact ID; mutation requires two explicit flags.

    This is an operator-tested primitive, NOT an unattended scheduler. Callers
    must supply independently persisted execution identity and trusted approval.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise WatchdogBlocked('naive clock')
    if not execute or not approval:
        return {'decision': 'DRY_RUN_ONLY', 'mutated': False}
    # Provider GET must succeed and return an exact job. No listing inference.
    try:
        job = client.get_job(request={'project_id': target.project, 'location': target.region, 'job_id': target.job_id})
    except Exception as exc:
        raise WatchdogBlocked('exact provider get failed') from exc
    verify_target(job, target, now=now)
    try:
        result = client.update_job(request={
            'project_id': target.project, 'location': target.region,
            'job_id': target.job_id,
            'job': {'requested_state': 'JOB_STATE_CANCELLED'},
        })
    except Exception as exc:
        # Provider response ambiguity: NEVER assume cancellation succeeded.
        raise WatchdogBlocked('provider cancellation uncertain; reconcile exact job') from exc
    return {'decision': 'CANCELLATION_REQUESTED', 'mutated': True,
            'provider_job_id': str(getattr(result, 'id', target.job_id))}
