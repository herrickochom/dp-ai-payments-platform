"""JOB A2.3 - FAIL-FIRST PROOF: governance permission spoofing.

A1 finding under test
---------------------
``GovernancePolicyEngine.effective_permissions()`` unions the raw
``request.identity.permissions`` list supplied by the caller with the
role-derived permissions.  The caller therefore contributes to the permission
set that every classification policy is evaluated against.

Security property under test
----------------------------
A caller-supplied permission must never change a policy decision.  Only
role-derived authority may do that.

These tests are intentionally FAIL-FIRST: they assert the security property
the platform must have, not the behaviour it currently has.  A failure here is
recorded as::

    A2_3_PERMISSION_SPOOFING=VULNERABILITY_PROVEN

They are deterministic and offline.  No live Trino, Superset, container or
network access is required or performed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

if str(AGENT_API_DIR) not in sys.path:
    sys.path.insert(0, str(AGENT_API_DIR))


from config import Settings
from governance import GovernancePolicyEngine
from governance_models import (
    DataClassification,
    GovernanceRequest,
    IdentityContext,
    PolicyDecisionType,
    ResourceContext,
)
from tools import GovernanceDenied, ToolRegistry


#
# INTERNAL in the trusted platform registry, with a non-restricted field, so
# that the only thing that can change the decision is the permission set.
#
INTERNAL_DATASET = "iceberg.silver.slv_pdm_loans"
INTERNAL_SQL = f"SELECT loan_id FROM {INTERNAL_DATASET}"

SPOOFED_PERMISSION = "can_view_internal_data"


class RecordingGateway:
    """Fake Trino gateway. Records every statement and never opens a socket."""

    def __init__(self):
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)
        return ["loan_id"], [["L-0001"]], "fake-analytical"

    def health(self):
        return {"status": "healthy", "query_id": "fake-health"}


def _governance_request(caller_permissions):
    """Unprivileged ``public_viewer`` identity; only ``permissions`` varies."""

    return GovernanceRequest(
        identity=IdentityContext(
            subject_id="public-viewer",
            roles=["public_viewer"],
            permissions=list(caller_permissions),
            purpose="public_reporting",
        ),
        action="read",
        resource=ResourceContext(
            dataset=INTERNAL_DATASET,
            fields=["loan_id"],
            classification=DataClassification.INTERNAL,
        ),
    )


# ---------------------------------------------------------------------------
# A2.3 - policy proof: the permission union flips DENY to ALLOW
# ---------------------------------------------------------------------------


def test_a2_3_caller_supplied_permission_must_not_flip_a_denied_decision():
    """A caller-supplied permission must not alter the policy outcome."""

    engine = GovernancePolicyEngine()

    honest = engine.evaluate(_governance_request([]))
    spoofed = engine.evaluate(_governance_request([SPOOFED_PERMISSION]))

    observed = {
        "role_derived_permissions": sorted(
            engine.effective_permissions(_governance_request([]))
        ),
        "honest_decision": honest.decision.value,
        "spoofed_decision": spoofed.decision.value,
    }

    assert spoofed.decision == honest.decision, (
        "SECURITY: IdentityContext.permissions is caller-supplied input. "
        f"Adding {SPOOFED_PERMISSION!r} to it must not change the decision "
        "for an identity holding no privileged role.\n"
        f"observed={observed}\n"
        "required={'honest_decision': spoofed.decision.value}"
    )

    assert honest.decision == PolicyDecisionType.DENY, (
        "SECURITY: an unprivileged identity must be denied INTERNAL data.\n"
        f"observed={observed}"
    )


# ---------------------------------------------------------------------------
# A2.3 - execution proof: the same flip reaches the Trino gateway
# ---------------------------------------------------------------------------


def test_a2_3_spoofed_permission_must_not_reach_trino():
    """Through ``ToolRegistry.execute_query`` the spoof changes deny to execute."""

    honest_gateway = RecordingGateway()
    honest_tools = ToolRegistry(
        honest_gateway,
        Settings(),
        governance_request=_governance_request([]),
    )

    with pytest.raises(GovernanceDenied):
        honest_tools.execute_query(INTERNAL_SQL, 10)

    spoofed_gateway = RecordingGateway()
    spoofed_tools = ToolRegistry(
        spoofed_gateway,
        Settings(),
        governance_request=_governance_request([SPOOFED_PERMISSION]),
    )

    try:
        spoofed_tools.execute_query(INTERNAL_SQL, 10)
        spoofed_outcome = "executed"
    except GovernanceDenied:
        spoofed_outcome = "denied"

    observed = {
        "honest_outcome": "denied",
        "honest_trino_executions": len(honest_gateway.executed),
        "spoofed_outcome": spoofed_outcome,
        "spoofed_trino_executions": len(spoofed_gateway.executed),
    }

    assert spoofed_gateway.executed == [], (
        "SECURITY: a caller-supplied permission must not unlock a governed "
        "dataset for an identity that holds no privileged role.\n"
        f"observed={observed}\n"
        "required={'spoofed_trino_executions': 0}"
    )