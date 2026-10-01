"""Focused field-data-protection regression matrix for the agent tool path.

Added by JOB A3-MASKING to lock in the projection-level protections that close
the A2.5 alias bypass and the A2.6 star-projection bypass.

Every case runs the real governed path: SQL validation, trusted projection
lineage resolution, trusted classification, policy decision, gateway execution
and masking.  The fake gateway returns a synthetic sentinel, never real
beneficiary data.

Deterministic and offline.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

for _path in (ROOT / "tests", ROOT, AGENT_API_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


from agent_authority_fixtures import (
    approval_registry,
    governance_context,
)
from config import Settings
from governance import GovernancePolicyEngine
from governance_models import (
    ApprovalContext,
    GovernanceRequest,
    IdentityContext,
    ResourceContext,
)
from models import AgentRequest
from tools import GovernanceDenied, ToolRegistry


BENEFICIARY_DATASET = "iceberg.silver.slv_pdm_beneficiaries"
LOANS_DATASET = "iceberg.silver.slv_pdm_loans"
PUBLIC_DATASET = "iceberg.consumption.cns_pdm_local_government_performance"

SENTINEL = "A2_SENSITIVE_SENTINEL"

APPROVAL_ID = "trusted-approval-regression"

INVESTIGATOR = IdentityContext(
    subject_id="case-officer-1",
    roles=["investigator"],
    purpose="investigation",
)


class SentinelGateway:
    """Fake Trino gateway returning a fixed synthetic result set."""

    def __init__(self, columns, row):
        self.columns = list(columns)
        self.row = list(row)
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)
        return list(self.columns), [list(self.row)], "fake-query"

    def health(self):
        return {"status": "healthy", "query_id": "fake-health"}


def _registry(gateway):
    return ToolRegistry(
        gateway,
        Settings(),
        governance_engine=GovernancePolicyEngine(
            approval_registry=approval_registry(
                approval_id=APPROVAL_ID,
                subject_id=INVESTIGATOR.subject_id,
                action="read",
                dataset=BENEFICIARY_DATASET,
            ),
        ),
        governance_request=GovernanceRequest(
            identity=INVESTIGATOR,
            action="read",
            approval=ApprovalContext(approval_id=APPROVAL_ID),
            resource=ResourceContext(dataset=BENEFICIARY_DATASET),
        ),
    )


# ---------------------------------------------------------------------------
# Projection-level protections
# ---------------------------------------------------------------------------


def test_direct_sensitive_projection_is_masked():
    gateway = SentinelGateway(
        ["beneficiary_name", "district"],
        [SENTINEL, "Kabale"],
    )

    result = _registry(gateway).execute_query(
        f"SELECT beneficiary_name, district FROM {BENEFICIARY_DATASET}",
        10,
    )

    assert result["governance"]["masked_fields"] == [
        "beneficiary_name"
    ]
    assert SENTINEL not in str(result["rows"])
    assert "Kabale" in str(result["rows"])


def test_aliased_sensitive_projection_is_masked():
    gateway = SentinelGateway(["bn", "district"], [SENTINEL, "Kabale"])

    result = _registry(gateway).execute_query(
        f"SELECT beneficiary_name AS bn, district FROM {BENEFICIARY_DATASET}",
        10,
    )

    assert result["governance"]["masked_fields"] == [
        "beneficiary_name"
    ]
    assert SENTINEL not in str(result["rows"])
    assert "Kabale" in str(result["rows"])


def test_mixed_public_and_sensitive_projection_masks_only_sensitive():
    gateway = SentinelGateway(
        ["district", "phone_number"],
        ["Kabale", SENTINEL],
    )

    result = _registry(gateway).execute_query(
        f"SELECT district, phone_number FROM {BENEFICIARY_DATASET}",
        10,
    )

    assert result["governance"]["masked_fields"] == ["phone_number"]
    assert SENTINEL not in str(result["rows"])
    assert "Kabale" in str(result["rows"])


def test_expressions_with_an_alias_are_masked_by_source_field():
    gateway = SentinelGateway(["masked_identity"], [SENTINEL])

    result = _registry(gateway).execute_query(
        f"SELECT beneficiary_name AS masked_identity "
        f"FROM {BENEFICIARY_DATASET}",
        10,
    )

    assert SENTINEL not in str(result["rows"])
# ---------------------------------------------------------------------------
# Fail-closed protections
# ---------------------------------------------------------------------------


def test_select_star_is_denied_before_execution():
    gateway = SentinelGateway(
        ["loan_id", "beneficiary_name", "district"],
        ["L-0001", SENTINEL, "Kabale"],
    )

    with pytest.raises(GovernanceDenied):
        _registry(gateway).execute_query(
            f"SELECT * FROM {BENEFICIARY_DATASET}",
            10,
        )

    assert gateway.executed == []


def test_caller_cannot_self_assert_a_privileged_role():
    """A claimed role must not grant authority without a server role registry.

    This closes the remaining caller-controlled authority field identified in
    JOB A4.4: `GovernanceRequest.identity.roles` arrives in the request body,
    so the Orchestrator intersects it with the server-side role registry before
    any policy decision is made.
    """

    from authority import ServerAuthorityResolver
    from orchestrator import Orchestrator

    orchestrator = Orchestrator(
        Settings(),
        SentinelGateway(["beneficiary_name"], [SENTINEL]),
        authority_resolver=ServerAuthorityResolver(
            role_authorities={},
        ),
    )

    response = orchestrator.execute(
        AgentRequest(
            agent="analytics",
            objective="What is repayment performance by district?",
            context=governance_context(
                subject_id="attacker",
                roles=("governance_administrator",),
                dataset=BENEFICIARY_DATASET,
            ),
        )
    )

    assert response.status == "failed", (
        "SECURITY: a request that merely claims governance_administrator "
        "must not be authorised when no server-side role registry entry "
        f"exists. observed={response.error.category if response.error else None}"
    )


def test_unknown_dataset_is_denied_before_execution():
    gateway = SentinelGateway(["district"], ["Kabale"])

    with pytest.raises(GovernanceDenied):
        _registry(gateway).execute_query(
            "SELECT district FROM iceberg.silver.no_such_table",
            10,
        )

    assert gateway.executed == []


def test_multi_dataset_query_is_denied_before_execution():
    gateway = SentinelGateway(["district"], ["Kabale"])

    with pytest.raises(GovernanceDenied):
        _registry(gateway).execute_query(
            "SELECT district FROM "
            f"{BENEFICIARY_DATASET} JOIN {LOANS_DATASET} USING (district)",
            10,
        )

    assert gateway.executed == []


def test_unknown_field_inherits_restricted_dataset_classification():
    """An unrecognised field must not become public by default."""

    gateway = SentinelGateway(["some_unknown_column"], [SENTINEL])

    result = _registry(gateway).execute_query(
        f"SELECT some_unknown_column FROM {BENEFICIARY_DATASET}",
        10,
    )

    assert result["governance"]["masked_fields"] == [
        "some_unknown_column"
    ]
    assert SENTINEL not in str(result["rows"])


# ---------------------------------------------------------------------------
# Controls that must not regress
# ---------------------------------------------------------------------------


def test_restricted_row_level_denial_is_preserved():
    """RESTRICTED row-level denial for a subject without the permission."""

    gateway = SentinelGateway(["beneficiary_name"], [SENTINEL])

    unprivileged = GovernanceRequest(
        identity=IdentityContext(
            subject_id="public-viewer",
            roles=["public_viewer"],
            purpose="public_reporting",
        ),
        action="read",
        resource=ResourceContext(dataset=BENEFICIARY_DATASET),
    )

    tools = ToolRegistry(
        gateway,
        Settings(),
        governance_request=unprivileged,
    )

    with pytest.raises(GovernanceDenied):
        tools.execute_query(
            f"SELECT beneficiary_name FROM {BENEFICIARY_DATASET}",
            10,
        )

    assert gateway.executed == []


def test_public_aggregate_dataset_remains_readable():
    gateway = SentinelGateway(
        ["district", "principal_repayment_rate"],
        ["Kabale", 0.24],
    )

    result = _registry(gateway).execute_query(
        f"SELECT district, principal_repayment_rate FROM {PUBLIC_DATASET}",
        10,
    )

    assert result["governance"]["decision"] == "ALLOW"
    assert "Kabale" in str(result["rows"])
    assert "0.24" in str(result["rows"])