"""Pre-submit policy binding. Approval is planning only; provider submit remains disabled.

Call this before any future Dataflow launch. Never turn ALLOW_PLANNING_ONLY
into provider submission without implementing durable idempotency and deadline enforcement.
"""
from cost_guard import require_approval, CostGateBlocked
from submission_guard import SubmissionIdentity, UnsafeSubmission


def evaluate_launch(identity: SubmissionIdentity, *, workers, runtime_minutes,
                    estimated_launch_gbp, reported_spend_gbp,
                    spend_observed_at, approval, now=None):
    identity.validate()
    decision = require_approval(project=identity.project, region=identity.region,
        workers=workers, runtime_minutes=runtime_minutes,
        estimated_launch_gbp=estimated_launch_gbp,
        reported_spend_gbp=reported_spend_gbp,
        spend_observed_at=spend_observed_at, approval=approval, now=now)
    return {**decision, 'submission_enabled': False,
            'reason': 'durable provider idempotency and automatic enforcement not proven'}
