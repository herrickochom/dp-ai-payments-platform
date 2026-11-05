"""Fail-closed, offline Stage 10 cost gate for planned *ephemeral* GCP workloads.
This does not replace billing: cost reports lag and cannot impose hard caps.
Call require_approval before each ephemeral launch; separately enforce worker limits
in the actual provider launch request and schedule bounded cancellation.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
import json

class CostGateBlocked(RuntimeError):
    pass

@dataclass(frozen=True)
class PilotLimits:
    project: str = 'project-e48f194e-f44f-458d-b4f'
    region: str = 'europe-west2'
    monthly_budget_gbp: float = 20.0
    stop_launch_at_gbp: float = 15.0
    max_workers: int = 1
    max_runtime_minutes: int = 30
    max_estimated_launch_gbp: float = 3.0
    max_spend_observation_age_minutes: int = 120


def require_approval(*, project: str, region: str, workers: int,
                     runtime_minutes: int, estimated_launch_gbp: float,
                     reported_spend_gbp: float | None, spend_observed_at: datetime | None,
                     approval: bool, limits: PilotLimits = PilotLimits(),
                     now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise CostGateBlocked('now must have timezone')
    if not approval:
        raise CostGateBlocked('explicit pilot deployment approval absent')
    if project != limits.project or region != limits.region:
        raise CostGateBlocked('project/region mismatch')
    if type(workers) is not int or not 1 <= workers <= limits.max_workers:
        raise CostGateBlocked('worker limit exceeded')
    if type(runtime_minutes) is not int or not 1 <= runtime_minutes <= limits.max_runtime_minutes:
        raise CostGateBlocked('runtime limit exceeded')
    if reported_spend_gbp is None or spend_observed_at is None or spend_observed_at.tzinfo is None:
        raise CostGateBlocked('fresh billing observation required')
    age = (now - spend_observed_at).total_seconds() / 60
    if not 0 <= age <= limits.max_spend_observation_age_minutes:
        raise CostGateBlocked('billing observation stale or in future')
    if not 0 <= reported_spend_gbp < limits.stop_launch_at_gbp:
        raise CostGateBlocked('spend at or above stop-launch threshold')
    if not 0 < estimated_launch_gbp <= limits.max_estimated_launch_gbp:
        raise CostGateBlocked('launch estimate missing or above ceiling')
    if reported_spend_gbp + estimated_launch_gbp > limits.stop_launch_at_gbp:
        raise CostGateBlocked('estimated launch breaches stop-launch threshold')
    return {'decision': 'ALLOW_PLANNING_ONLY', 'project': project, 'region': region,
            'max_workers': workers, 'cancel_after_minutes': runtime_minutes,
            'requires_provider_enforced_limits': True,
            'requires_automated_teardown': True,
            'warning': 'Billing can lag; this is not a hard spending cap'}
