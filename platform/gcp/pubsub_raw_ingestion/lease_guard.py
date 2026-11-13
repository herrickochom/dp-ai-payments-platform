"""Offline lease-renewal guard for one Pub/Sub message.

The caller supplies a real deadline-extension operation; this module does not
connect to GCP or infer IAM, message envelope, or subscriber flow control.
A lease failure is fail-closed: never ack after loss of renewal assurance.
"""
from __future__ import annotations

from dataclasses import dataclass
from time import monotonic
from threading import Event, Lock, Thread
from typing import Callable, Protocol


class Message(Protocol):
    def ack(self) -> None: ...


@dataclass(frozen=True)
class Result:
    acknowledged: bool
    reason: str


# A stalled renewal worker must not accumulate across messages.
# This lock serialises renewal processing in this Python process.
_renewal_gate = Lock()


def process_with_renewal(
    message: Message,
    durable_verify: Callable[[Message], bool],
    extend_lease: Callable[[Message], None],
    *,
    renewal_interval_seconds: float = 5.0,
    max_processing_seconds: float | None = None,
    shutdown_timeout_seconds: float = 5.0,
) -> Result:
    """Renew throughout processing; ack only after verified durable write.

    Does not nack automatically: failed messages remain eligible for redelivery.
    A real Pub/Sub adapter must enforce a maximum lease duration, flow control,
    bounded processing time and an appropriate dead-letter/retry policy.
    """
    if not 0 < renewal_interval_seconds <= 10:
        raise ValueError('renewal interval must be in (0, 10] seconds')
    if max_processing_seconds is not None:
        if not 0 < max_processing_seconds < float('inf'):
            raise ValueError('max processing duration must be finite and positive')
    if not 0 < shutdown_timeout_seconds < float('inf'):
        raise ValueError('shutdown timeout must be finite and positive')

    deadline = (
        monotonic() + max_processing_seconds
        if max_processing_seconds is not None else None
    )
    if not _renewal_gate.acquire(blocking=False):
        return Result(False, 'renewal_worker_unavailable')

    try:
        extend_lease(message)  # initial renewal must succeed before work
    except Exception:
        _renewal_gate.release()
        return Result(False, 'initial_lease_failed')

    if deadline is not None and monotonic() >= deadline:
        _renewal_gate.release()
        return Result(False, 'processing_deadline_exceeded')

    stop = Event()
    failed = Event()
    expired = Event()

    def renew() -> None:
        while True:
            remaining = (
                deadline - monotonic() if deadline is not None else None
            )
            if remaining is not None and remaining <= 0:
                expired.set()
                stop.set()
                return
            wait_seconds = (
                min(renewal_interval_seconds, remaining)
                if remaining is not None else renewal_interval_seconds
            )
            if stop.wait(wait_seconds):
                return
            if deadline is not None and monotonic() >= deadline:
                expired.set()
                stop.set()
                return
            try:
                extend_lease(message)
            except Exception:
                failed.set()
                stop.set()
                return

    try:
        thread = Thread(
            target=renew,
            name='kp001w-lease-renewal',
            daemon=True,
        )
        thread.start()
    except Exception:
        _renewal_gate.release()
        raise

    shutdown_deadline = None

    def stop_and_wait() -> bool:
        nonlocal shutdown_deadline
        stop.set()
        if shutdown_deadline is None:
            shutdown_deadline = monotonic() + shutdown_timeout_seconds
        remaining = max(0.0, shutdown_deadline - monotonic())
        thread.join(timeout=remaining)
        return not thread.is_alive()
    try:
        try:
            verified = durable_verify(message)
        except Exception:
            return Result(False, 'durable_processing_failed')
        if verified is not True:
            return Result(False, 'not_durably_verified')
        if expired.is_set():
            return Result(False, 'processing_deadline_exceeded')
        if failed.is_set():
            return Result(False, 'lease_renewal_failed')
        # Stop renewal and join before ack, preventing an in-flight extension
        # failure from racing with the acknowledgement decision.
        if not stop_and_wait():
            return Result(False, 'lease_renewal_shutdown_unresolved')
        if expired.is_set():
            return Result(False, 'processing_deadline_exceeded')
        if failed.is_set():
            return Result(False, 'lease_renewal_failed')
        if deadline is not None and monotonic() >= deadline:
            return Result(False, 'processing_deadline_exceeded')
        try:
            message.ack()
        except Exception:
            return Result(False, 'ack_failed_redelivery_possible')
        return Result(True, 'acknowledged_after_verification')
    finally:
        stopped = stop_and_wait()
        if stopped:
            _renewal_gate.release()
        else:
            # Never release exclusion while renewal remains unresolved.
            # If the watcher cannot start, exclusion remains held.
            # Controlled process recovery is required.
            def release_when_finished():
                thread.join()
                _renewal_gate.release()

            try:
                reaper = Thread(
                    target=release_when_finished,
                    name='kp001w-renewal-reaper',
                    daemon=True,
                )
                reaper.start()
            except Exception:
                # Fail closed: retain the gate. Operational intervention
                # is required if the worker subsequently terminates.
                pass
