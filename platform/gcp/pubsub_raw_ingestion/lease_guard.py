"""Offline lease-renewal guard for one Pub/Sub message.

The caller supplies a real deadline-extension operation; this module does not
connect to GCP or infer IAM, message envelope, or subscriber flow control.
A lease failure is fail-closed: never ack after loss of renewal assurance.
"""
from __future__ import annotations

from dataclasses import dataclass
from threading import Event, Thread
from typing import Callable, Protocol


class Message(Protocol):
    def ack(self) -> None: ...


@dataclass(frozen=True)
class Result:
    acknowledged: bool
    reason: str


def process_with_renewal(
    message: Message,
    durable_verify: Callable[[Message], bool],
    extend_lease: Callable[[Message], None],
    *,
    renewal_interval_seconds: float = 5.0,
) -> Result:
    """Renew throughout processing; ack only after verified durable write.

    Does not nack automatically: failed messages remain eligible for redelivery.
    A real Pub/Sub adapter must enforce a maximum lease duration, flow control,
    bounded processing time and an appropriate dead-letter/retry policy.
    """
    if not 0 < renewal_interval_seconds <= 10:
        raise ValueError('renewal interval must be in (0, 10] seconds')
    try:
        extend_lease(message)  # initial renewal must succeed before work
    except Exception:
        return Result(False, 'initial_lease_failed')

    stop = Event()
    failed = Event()

    def renew() -> None:
        while not stop.wait(renewal_interval_seconds):
            try:
                extend_lease(message)
            except Exception:
                failed.set()
                stop.set()
                return

    thread = Thread(target=renew, name='kp001w-lease-renewal', daemon=True)
    thread.start()
    try:
        try:
            verified = durable_verify(message)
        except Exception:
            return Result(False, 'durable_processing_failed')
        if verified is not True:
            return Result(False, 'not_durably_verified')
        if failed.is_set():
            return Result(False, 'lease_renewal_failed')
        # Stop renewal and join before ack, preventing an in-flight extension
        # failure from racing with the acknowledgement decision.
        stop.set()
        thread.join()
        if failed.is_set():
            return Result(False, 'lease_renewal_failed')
        try:
            message.ack()
        except Exception:
            return Result(False, 'ack_failed_redelivery_possible')
        return Result(True, 'acknowledged_after_verification')
    finally:
        stop.set()
        thread.join()
