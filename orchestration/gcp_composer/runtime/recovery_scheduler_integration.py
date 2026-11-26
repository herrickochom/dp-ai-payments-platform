"""Bounded Stage 8–9 scheduler integration.

No cloud calls or Dataflow submission.

The caller must supply already-claimed Stage 8 work.
The existing transactional handoff adapter performs
lease fencing and outbox preparation.
"""

from recovery_handoff_controller import (
    HandoffRequest,
    RecoveryHandoffController,
)


class RecoverySchedulerIntegration:
    def __init__(
        self,
        connection_factory,
        claim_source,
        controller=None,
        max_items=10,
    ):
        if not callable(claim_source):
            raise TypeError("claim_source must be callable")

        if (
            type(max_items) is not int
            or not 1 <= max_items <= 10
        ):
            raise ValueError("max_items must be 1..10")

        self.claim_source = claim_source
        self.max_items = max_items
        self.controller = (
            controller
            if controller is not None
            else RecoveryHandoffController(connection_factory)
        )

    def run_once(self):
        claims = list(self.claim_source())

        if len(claims) > self.max_items:
            raise ValueError("Claim batch exceeds bound")

        for claim in claims:
            if not isinstance(claim, HandoffRequest):
                raise TypeError(
                    "Claim source must return HandoffRequest objects"
                )

        identities = [
            (claim.execution_id, claim.attempt_number)
            for claim in claims
        ]

        if len(identities) != len(set(identities)):
            raise ValueError("Duplicate claims in batch")

        return self.controller.run_batch(
            claims,
            max_items=self.max_items,
        )
