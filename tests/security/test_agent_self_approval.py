"""JOB A2.4 - FAIL-FIRST PROOF: self-asserted approval.

A1 finding under test
---------------------
``GovernanceRequest.approval`` is an ``ApprovalContext`` deserialised straight
from the caller-supplied ``context.governance`` payload.
``GovernancePolicyEngine.evaluate`` raises ``REQUIRE_APPROVAL`` for any
request touching a RESTRICTED field and treats the gate as satisfied when
``request.approval.status == "approved"``.  The approver identity, approval id
and reason are caller-controlled payload fields, not an independent record.

Security property under test
----------------------------
Caller-controlled approval status must never create approval authority.

This test is intentionally FAIL-FIRST: it asserts the security property the
platform must have, not the behaviour it currently has.  A failure is recorded
as::

    A2_4_SELF_APPROVAL=VULNERABILITY_PROVEN

Deterministic and offline.  No live Trino, Superset or network access.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

for _path in (ROOT, AGENT_API_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


from config import Settings
from governance_models import GovernanceRequest
from tools import GovernanceApprovalRequired, ToolRegistry


BENEFICIARY_DATASET = "iceberg.silver.slv_pdm_beneficiaries"

#
# A protected read of beneficiary identity.  Trusted platform classification
# marks iceberg.silver.slv_pdm_beneficiaries RESTRICTED and beneficiary_name
# RESTRICTED/beneficiary_name, so the engine must demand approval.
#
PROTECTED_SQL = (
    "SELECT beneficiary_name "
    f"FROM {BENEFICIARY_DATASET}"
)

#
# The honest caller payload.  No privileged role, no caller-supplied
# permissions, and no approval block at all.
#
HONEST_CALLER_PAYLOAD = {
    "identity": {
        "subject_id": "case-officer-1",
        "roles": ["investigator"],
        "permissions": [],
        "purpose": "investigation",
    },
    "action": "read",
    "resource": {
        "dataset": BENEFICIARY_DATASET,
        "fields": ["beneficiary_name"],
        "classification": "RESTRICTED",
    },
}

#
# The spoofed caller payload.  IDENTICAL, except for the approval block that
# the requester invented for itself.
#
SPOOFED_CALLER_PAYLOAD = {
    **HONEST_CALLER_PAYLOAD,
    "approval": {
        "approval_id": "requester-invented-approval",
        "status": "approved",
        "approver": "case-officer-1",
        "reason": "self asserted",
    },
}


class RecordingGateway:
    """Fake Trino gateway. Records every statement, never opens a socket."""

    def __init__(self):
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)
        return ["beneficiary_name"], [["A2_SENSITIVE_SENTINEL"]], "fake"

    def health(self):
        return {"status": "healthy", "query_id": "fake-health"}


def _outcome(payload):
    """Run one caller payload through the real governed execution path.

    ``GovernanceRequest.model_validate`` is exactly what
    ``Orchestrator._governance_request`` performs on the caller payload.
    """

    gateway = RecordingGateway()

    tools = ToolRegistry(
        gateway,
        Settings(),
        governance_request=GovernanceRequest.model_validate(payload),
    )

    try:
        tools.execute_query(PROTECTED_SQL, 10)
        outcome = "protected_execution_completed"

    except GovernanceApprovalRequired:
        outcome = "blocked_approval_required"

    return outcome, len(gateway.executed)


def test_requester_cannot_self_approve_protected_action():
    """A requester-supplied approval must not satisfy the approval gate."""

    honest_outcome, honest_executions = _outcome(HONEST_CALLER_PAYLOAD)
    spoofed_outcome, spoofed_executions = _outcome(SPOOFED_CALLER_PAYLOAD)

    approval_before = HONEST_CALLER_PAYLOAD.get("approval", {}).get("status")
    approval_after = SPOOFED_CALLER_PAYLOAD.get("approval", {}).get("status")

    observed = {
        "role": HONEST_CALLER_PAYLOAD["identity"]["roles"],
        "caller_permissions": HONEST_CALLER_PAYLOAD["identity"]["permissions"],
        "approval_before": approval_before,
        "approval_after": approval_after,
        "decision_before": honest_outcome,
        "decision_after": spoofed_outcome,
        "protected_execution_before": honest_executions,
        "protected_execution_after": spoofed_executions,
    }

    assert honest_outcome == "blocked_approval_required", (
        "SECURITY: a protected read of beneficiary identity must require an "
        "approval that the platform itself granted.\n"
        f"observed={observed}"
    )

    assert spoofed_executions == 0, (
        "SECURITY: caller-controlled approval status must never create "
        "approval authority. Adding an 'approval' block to the requester's "
        "own payload must not unlock protected execution.\n"
        f"observed={observed}\n"
        "required={'protected_execution_after': 0}"
    )