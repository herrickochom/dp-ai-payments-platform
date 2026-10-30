"""Offline Stage 9B recovery policy. No database, network or GCP calls.

An adapter must persist these transitions transactionally and enforce live leases.
Unknown provider visibility is never interpreted as safe-to-resubmit.
"""
from dataclasses import dataclass
from enum import Enum

class RecoveryAction(str, Enum):
    RECONCILE = 'RECONCILE'
    VERIFY_OUTPUT = 'VERIFY_OUTPUT'
    OBSERVE = 'OBSERVE'
    QUARANTINE = 'QUARANTINE'
    COMPLETE = 'COMPLETE'
    WAIT = 'WAIT'

@dataclass(frozen=True)
class RecoverySnapshot:
    intent_state: str
    provider_state: str | None
    provider_identity_matches: bool | None
    evidence_verified: bool
    submission_may_have_occurred: bool
    reconciliation_attempts: int


def next_delay_seconds(attempts: int, *, base: int = 10, cap: int = 900) -> int:
    if not isinstance(attempts, int) or attempts < 0 or base < 1 or cap < base:
        raise ValueError('invalid backoff inputs')
    return min(cap, base * (2 ** min(attempts, 20)))


def decide_recovery(snapshot: RecoverySnapshot, *, max_observations: int = 20) -> RecoveryAction:
    if max_observations < 1 or snapshot.reconciliation_attempts < 0:
        raise ValueError('invalid observation budget')
    if snapshot.intent_state in ('COMPLETED',):
        return RecoveryAction.COMPLETE if snapshot.evidence_verified else RecoveryAction.QUARANTINE
    if snapshot.intent_state == 'QUARANTINED':
        return RecoveryAction.QUARANTINE
    if snapshot.provider_identity_matches is False:
        return RecoveryAction.QUARANTINE
    if snapshot.provider_state == 'JOB_STATE_DONE':
        return RecoveryAction.COMPLETE if snapshot.evidence_verified and snapshot.provider_identity_matches is True else RecoveryAction.VERIFY_OUTPUT
    if snapshot.provider_state in ('JOB_STATE_FAILED', 'JOB_STATE_CANCELLED', 'JOB_STATE_DRAINED'):
        return RecoveryAction.QUARANTINE  # New attempt needs separate governed retry decision.
    if snapshot.provider_state is not None:
        return RecoveryAction.OBSERVE
    if snapshot.reconciliation_attempts >= max_observations:
        return RecoveryAction.QUARANTINE
    if snapshot.submission_may_have_occurred or snapshot.intent_state == 'SUBMISSION_UNCERTAIN':
        return RecoveryAction.RECONCILE
    return RecoveryAction.WAIT  # Submission requires separate atomic DB transition.
