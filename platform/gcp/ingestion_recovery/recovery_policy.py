"""Offline recovery policy. Never interprets an ack() call as server confirmation."""
from dataclasses import dataclass
from enum import Enum

class Outcome(str, Enum):
    VERIFIED_ACK = 'VERIFIED_ACK'
    ACK_UNCONFIRMED = 'ACK_UNCONFIRMED'
    RETRYABLE = 'RETRYABLE'
    QUARANTINE_REQUIRED = 'QUARANTINE_REQUIRED'

@dataclass(frozen=True)
class Decision:
    outcome: Outcome
    allow_ack: bool
    reason: str

def decide(*, durable_verified: bool, identity_valid: bool,
           content_conflict: bool = False, ack_future_confirmed: bool = False,
           processing_error: bool = False) -> Decision:
    if not identity_valid or content_conflict:
        return Decision(Outcome.QUARANTINE_REQUIRED, False, 'invalid identity or immutable conflict')
    if processing_error or not durable_verified:
        return Decision(Outcome.RETRYABLE, False, 'durable verification incomplete')
    if ack_future_confirmed:
        return Decision(Outcome.VERIFIED_ACK, False, 'acknowledgement confirmed by provider')
    return Decision(Outcome.ACK_UNCONFIRMED, True, 'durable object verified; provider acknowledgement pending')
