"""Fail-closed event admission for future publisher integration.

This module does not publish, read Kafka, or modify journals.
A new-event admission is not permission to activate live publishing.
"""
from dataclasses import dataclass
from enum import Enum


class Admission(Enum):
    HISTORICAL_QUARANTINE = "HISTORICAL_QUARANTINE"
    NEW_EVENT_ELIGIBLE = "NEW_EVENT_ELIGIBLE"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class AdmissionDecision:
    status: Admission
    reason: str


@dataclass(frozen=True)
class AdmissionBoundary:
    historical_ids: frozenset
    approved_new_namespace: str
    boundary_verified: bool = False
    live_activation_approved: bool = False

    def __post_init__(self):
        if not isinstance(self.historical_ids, frozenset):
            raise TypeError("historical_ids must be frozenset")
        if not isinstance(self.approved_new_namespace, str):
            raise TypeError("approved_new_namespace must be string")
        if not self.approved_new_namespace:
            raise ValueError("new-event namespace required")


def classify_event(
    event_id: str,
    topic: str,
    boundary: AdmissionBoundary,
) -> AdmissionDecision:
    if not isinstance(event_id, str) or not event_id.strip():
        return AdmissionDecision(
            Admission.REJECTED, "Missing event identity"
        )

    if not isinstance(topic, str) or not topic.strip():
        return AdmissionDecision(
            Admission.REJECTED, "Missing topic"
        )

    if event_id in boundary.historical_ids:
        return AdmissionDecision(
            Admission.HISTORICAL_QUARANTINE,
            "Identity appears in historical ledger",
        )

    if not boundary.boundary_verified:
        return AdmissionDecision(
            Admission.REJECTED,
            "Historical/new-event boundary not verified",
        )

    if not event_id.startswith(boundary.approved_new_namespace):
        return AdmissionDecision(
            Admission.REJECTED,
            "Event outside approved new-event namespace",
        )

    return AdmissionDecision(
        Admission.NEW_EVENT_ELIGIBLE,
        "New-event admission conditions satisfied",
    )


def assert_live_activation_allowed(
    decision: AdmissionDecision,
    boundary: AdmissionBoundary,
) -> None:
    if decision.status is not Admission.NEW_EVENT_ELIGIBLE:
        raise RuntimeError(
            "STOP: event not eligible for new-event publishing"
        )

    if not boundary.live_activation_approved:
        raise RuntimeError(
            "STOP: live publisher activation not approved"
        )
