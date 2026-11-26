"""Bounded Stage 8 to Stage 9 handoff controller.

LOCAL/OFFLINE IMPLEMENTATION ONLY.

This controller:
- accepts a Stage 8 claim supplied by an authorised scheduler;
- validates its required fields;
- invokes the existing transactional handoff adapter;
- returns structured execution evidence;
- never calls a cloud provider;
- never submits a Dataflow job.

It does not independently claim work or schedule recurring execution.
"""

from dataclasses import dataclass

from transactional_recovery_handoff import (
    TransactionalRecoveryHandoff,
    HandoffConflict,
    HandoffFenced,
)


@dataclass(frozen=True)
class HandoffRequest:
    execution_id: str
    attempt_number: int
    worker: str
    lease_token: str
    job_name: str
    project: str
    region: str
    spec_digest: str


@dataclass(frozen=True)
class HandoffResult:
    execution_id: str
    attempt_number: int
    decision: str
    state: str


class RecoveryHandoffController:
    """Scheduler-callable transactional handoff controller."""

    def __init__(self, connection_factory, handoff=None):
        self._handoff = (
            handoff
            if handoff is not None
            else TransactionalRecoveryHandoff(connection_factory)
        )

    def run_once(self, request: HandoffRequest) -> HandoffResult:
        if not isinstance(request, HandoffRequest):
            raise TypeError("Expected HandoffRequest")

        result = self._handoff.prepare(
            execution_id=request.execution_id,
            attempt_number=request.attempt_number,
            worker=request.worker,
            lease_token=request.lease_token,
            job_name=request.job_name,
            project=request.project,
            region=request.region,
            spec_digest=request.spec_digest,
        )

        if result["decision"] not in ("NEW", "DUPLICATE"):
            raise RuntimeError("Unexpected handoff decision")

        if result["state"] not in (
            "PREPARED",
            "SUBMISSION_UNCERTAIN",
            "OBSERVING",
            "VERIFYING",
            "RETRY_ELIGIBLE",
            "QUARANTINED",
            "COMPLETED",
        ):
            raise RuntimeError("Unexpected outbox state")

        return HandoffResult(
            execution_id=result["execution_id"],
            attempt_number=result["attempt_number"],
            decision=result["decision"],
            state=result["state"],
        )

    def run_batch(self, requests, max_items=10):
        """Bounded sequential processing; never silently skips work."""

        if (
            not isinstance(max_items, int)
            or isinstance(max_items, bool)
            or not 1 <= max_items <= 10
        ):
            raise ValueError("max_items must be between 1 and 10")

        requests = list(requests)

        if len(requests) > max_items:
            raise ValueError("Batch exceeds configured bound")

        identities = [
            (request.execution_id, request.attempt_number)
            for request in requests
        ]

        if len(identities) != len(set(identities)):
            raise ValueError("Duplicate execution attempts in batch")

        return [self.run_once(request) for request in requests]
