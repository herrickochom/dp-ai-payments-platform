"""Deterministic, offline lifecycle eligibility; no executor or infrastructure IO.

ELIGIBLE_FOR_DISPOSAL is consideration only, never deletion authority. Retention
periods must be supplied from an approved asset policy, not inferred from YAML.
"""
from dataclasses import dataclass
from enum import Enum


class HoldStatus(str, Enum):
    CLEAR = 'CLEAR'
    ACTIVE = 'ACTIVE'
    UNKNOWN = 'UNKNOWN'


class LifecycleDecision(str, Enum):
    RETAIN = 'RETAIN'
    ELIGIBLE_FOR_ARCHIVE = 'ELIGIBLE_FOR_ARCHIVE'
    ELIGIBLE_FOR_DISPOSAL = 'ELIGIBLE_FOR_DISPOSAL'
    BLOCKED_LEGAL_HOLD = 'BLOCKED_LEGAL_HOLD'
    BLOCKED_RECOVERY_REFERENCE = 'BLOCKED_RECOVERY_REFERENCE'
    BLOCKED_RECOVERY_BACKUP = 'BLOCKED_RECOVERY_BACKUP'
    BLOCKED_PROTECTED_BASELINE = 'BLOCKED_PROTECTED_BASELINE'
    BLOCKED_DEPENDENCIES = 'BLOCKED_DEPENDENCIES'
    BLOCKED_APPROVAL = 'BLOCKED_APPROVAL'
    POLICY_CONFLICT = 'POLICY_CONFLICT'
    POLICY_UNRESOLVED = 'POLICY_UNRESOLVED'


@dataclass(frozen=True)
class LifecycleAsset:
    """Explicit metadata assertion, not independently verified asset evidence.

    None means unknown. Protection/recovery flags must be explicitly False and
    the hold explicitly CLEAR before consideration. Ages/periods are whole days;
    None or invalid day values mean an unresolved retention clock/policy.
    archive_requested selects archive consideration instead of disposal.
    """
    asset_id: str
    asset_type: str
    sensitivity: str
    age_days: int | None = None
    retention_days: int | None = None
    policy_resolved: bool | None = None
    policy_conflict: bool | None = None
    hold_status: HoldStatus | None = HoldStatus.UNKNOWN
    owner_approved: bool | None = None
    privacy_legal_approved: bool | None = None
    recovery_approved: bool | None = None
    dependencies_resolved: bool | None = None
    protected_baseline: bool | None = None
    recovery_reference: bool | None = None
    recovery_backup: bool | None = None
    archive_requested: bool = False
    archive_approved: bool | None = None


def plan_lifecycle(asset: LifecycleAsset) -> LifecycleDecision:
    """Return one decision with fixed fail-closed precedence and no side effects."""
    decision = LifecycleDecision
    if asset.protected_baseline is not False:
        return decision.BLOCKED_PROTECTED_BASELINE
    if not isinstance(asset.hold_status, HoldStatus) or asset.hold_status is not HoldStatus.CLEAR:
        return decision.BLOCKED_LEGAL_HOLD
    if asset.recovery_reference is not False:
        return decision.BLOCKED_RECOVERY_REFERENCE
    if asset.recovery_backup is not False:
        return decision.BLOCKED_RECOVERY_BACKUP
    if asset.dependencies_resolved is not True:
        return decision.BLOCKED_DEPENDENCIES
    if asset.policy_conflict is True:
        return decision.POLICY_CONFLICT
    if (asset.policy_conflict is not False or asset.policy_resolved is not True
            or any(not isinstance(value, str) or not value.strip()
                   for value in (asset.asset_id, asset.asset_type, asset.sensitivity))
            or any(type(value) is not int or value < 0
                   for value in (asset.age_days, asset.retention_days))):
        return decision.POLICY_UNRESOLVED
    if asset.age_days < asset.retention_days:
        return decision.RETAIN
    if any(approved is not True for approved in (
            asset.owner_approved, asset.privacy_legal_approved, asset.recovery_approved)):
        return decision.BLOCKED_APPROVAL
    if asset.archive_requested is True:
        if asset.archive_approved is not True:
            return decision.BLOCKED_APPROVAL
        return decision.ELIGIBLE_FOR_ARCHIVE
    if asset.archive_requested is not False:
        return decision.BLOCKED_APPROVAL
    return decision.ELIGIBLE_FOR_DISPOSAL
