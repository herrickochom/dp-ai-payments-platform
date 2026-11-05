"""Offline-only bounded message processing; no cloud clients or subscriptions.

The process callback must synchronously finish durable GCS write and readback
before returning. A real adapter must supply an authenticated lease extender
and a subscriber with explicit flow control; this module does not create one.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol


class Message(Protocol):
    def ack(self) -> None: ...


@dataclass(frozen=True)
class Outcome:
    acknowledged: bool
    reason: str


def process_one(message: Message, durable_process: Callable[[Message], bool],
                extend_lease: Callable[[Message], None]) -> Outcome:
    """Fail closed: ack only after verified durable processing.

    Caller must manage a lease-renewal loop for processing longer than the
    subscription ack deadline; one extension here is only an initial guard.
    """
    try:
        extend_lease(message)
    except Exception:
        return Outcome(False, 'lease_extension_failed')
    try:
        verified = durable_process(message)
    except Exception:
        return Outcome(False, 'processing_failed')
    if verified is not True:
        return Outcome(False, 'not_durably_verified')
    try:
        message.ack()
    except Exception:
        return Outcome(False, 'ack_failed_redelivery_possible')
    return Outcome(True, 'acknowledged_after_verification')
