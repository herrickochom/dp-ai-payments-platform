import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(
    0, str(ROOT / "services/payment-producer")
)

from event_admission_policy import (
    Admission,
    AdmissionBoundary,
    classify_event,
    assert_live_activation_allowed,
)


def boundary(**changes):
    values = {
        "historical_ids": frozenset({
            "legacy-001",
            "new:duplicate",
        }),
        "approved_new_namespace": "new:",
        "boundary_verified": True,
        "live_activation_approved": False,
    }
    values.update(changes)
    return AdmissionBoundary(**values)


def test_historical_event_quarantined():
    decision = classify_event(
        "legacy-001", "agent.transactions", boundary()
    )
    assert decision.status is Admission.HISTORICAL_QUARANTINE


def test_historical_collision_quarantined():
    decision = classify_event(
        "new:duplicate", "agent.transactions", boundary()
    )
    assert decision.status is Admission.HISTORICAL_QUARANTINE


def test_new_event_eligible_offline():
    decision = classify_event(
        "new:unique-001", "agent.transactions", boundary()
    )
    assert decision.status is Admission.NEW_EVENT_ELIGIBLE


def test_unverified_boundary_rejected():
    decision = classify_event(
        "new:unique-001",
        "agent.transactions",
        boundary(boundary_verified=False),
    )
    assert decision.status is Admission.REJECTED


def test_wrong_namespace_rejected():
    decision = classify_event(
        "legacy-999", "agent.transactions", boundary()
    )
    assert decision.status is Admission.REJECTED


def test_empty_event_identity_rejected():
    decision = classify_event(
        "", "agent.transactions", boundary()
    )
    assert decision.status is Admission.REJECTED


def test_empty_topic_rejected():
    decision = classify_event(
        "new:unique-001", "", boundary()
    )
    assert decision.status is Admission.REJECTED


def test_live_activation_blocked():
    b = boundary()
    decision = classify_event(
        "new:unique-001", "agent.transactions", b
    )
    with pytest.raises(RuntimeError, match="not approved"):
        assert_live_activation_allowed(decision, b)


def test_quarantined_event_cannot_activate():
    b = boundary(live_activation_approved=True)
    decision = classify_event(
        "legacy-001", "agent.transactions", b
    )
    with pytest.raises(RuntimeError, match="not eligible"):
        assert_live_activation_allowed(decision, b)


def test_explicit_activation_flag_required():
    b = boundary(live_activation_approved=True)
    decision = classify_event(
        "new:unique-001", "agent.transactions", b
    )
    assert_live_activation_allowed(decision, b)


def test_invalid_boundary_type_rejected():
    with pytest.raises(TypeError):
        AdmissionBoundary(
            historical_ids=set(),
            approved_new_namespace="new:",
        )
