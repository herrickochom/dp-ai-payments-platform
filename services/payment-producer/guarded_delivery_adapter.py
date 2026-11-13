"""Offline-tested guarded delivery integration.

No Kafka connection is created here.
No live publisher entrypoint is modified.

An approved, verified admission boundary is required.
The caller supplies the producer and durable journal.
"""

import hashlib

from event_admission_policy import (
    Admission,
    classify_event,
    assert_live_activation_allowed,
)
from delivery_coordinator import DeliveryCoordinator


class GuardedDeliveryError(RuntimeError):
    pass


class GuardedDeliveryAdapter:
    def __init__(
        self,
        journal,
        producer,
        boundary,
        *,
        offline_test_mode=False,
    ):
        self.boundary = boundary
        self.offline_test_mode = offline_test_mode

        if not offline_test_mode:
            raise GuardedDeliveryError(
                "STOP: Stage 2G live integration disabled"
            )

        if not boundary.boundary_verified:
            raise GuardedDeliveryError(
                "STOP: historical identity boundary unverified"
            )

        if boundary.live_activation_approved:
            raise GuardedDeliveryError(
                "STOP: live approval must not be used "
                "in offline test mode"
            )

        self.coordinator = DeliveryCoordinator(
            journal, producer
        )

    def publish(
        self,
        topic,
        event_id,
        content,
        key,
        wire,
    ):
        decision = classify_event(
            event_id, topic, self.boundary
        )

        if decision.status is not Admission.NEW_EVENT_ELIGIBLE:
            raise GuardedDeliveryError(
                "STOP: " + decision.reason
            )

        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")

        if not isinstance(key, bytes):
            raise TypeError("key must be bytes")

        if not isinstance(wire, bytes):
            raise TypeError("wire must be bytes")

        if not wire:
            raise GuardedDeliveryError(
                "STOP: empty Kafka wire payload"
            )

        # Stage 2G deliberately does not call
        # assert_live_activation_allowed().
        # It is an offline harness, not an activation path.
        self.coordinator.publish(
            topic,
            event_id,
            content,
            key,
            wire,
        )

    def finish(self):
        return self.coordinator.finish()
