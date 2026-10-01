"""JOB A2.5 / A2.6 - FAIL-FIRST PROOFS: sensitive-field masking bypasses.

Two separate security properties over the same subsystem
(``ToolRegistry._mask_query_rows`` driven by ``GovernancePolicyEngine``):

A2.5  ALIAS
    ``SELECT beneficiary_name AS bn`` resolves the trusted classification of
    the SOURCE field (``beneficiary_name``), so the policy decision masks
    ``beneficiary_name``.  Masking is applied by matching ``masked_fields``
    against the column labels Trino returns, which is ``bn``.  The match never
    happens, so the source value leaves unmasked.

A2.6  SELECT STAR
    ``projected_fields_from_sql`` deliberately yields no field for ``*``, so a
    ``SELECT *`` produces no field classifications, no field-level decision,
    no approval requirement and no ``masked_fields``.  The identical sensitive
    value leaves unmasked purely because the AST names no column.

Security properties under test
-------------------------------
Aliasing a sensitive source field must not cause the original value to escape
masking.  ``SELECT *`` must not allow sensitive fields to leave the governed
tool path unmasked.

These tests are intentionally FAIL-FIRST.  A failure of the sentinel
assertions is recorded as::

    A2_5_ALIAS_MASKING=VULNERABILITY_PROVEN
    A2_6_SELECT_STAR=VULNERABILITY_PROVEN

Both run the full meaningful path - SQL validation, trusted classification,
governance decision, gateway execution and masking - never
``_mask_query_rows`` in isolation.

Deterministic and offline.  No live Trino, Superset or network access.  No
real beneficiary data: a synthetic sentinel is used throughout.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

for _path in (ROOT / "tests", ROOT, AGENT_API_DIR):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))


from agent_authority_fixtures import approval_registry
from classification import TrustedClassificationResolver
from config import Settings
from governance import GovernancePolicyEngine
from governance_models import (
    ApprovalContext,
    GovernanceRequest,
    IdentityContext,
    ResourceContext,
)
from tools import ToolRegistry


BENEFICIARY_DATASET = "iceberg.silver.slv_pdm_beneficiaries"

#
# Synthetic sentinel.  Not beneficiary data; a fixed marker used only to
# detect whether a sensitive value survives the governed tool path.
#
SENTINEL = "A2_SENSITIVE_SENTINEL"

#
# Honest investigator identity: no caller-supplied permissions, so every
# permission below is role-derived from "investigator".
#
IDENTITY = IdentityContext(
    subject_id="case-officer-1",
    roles=["investigator"],
    permissions=[],
    purpose="investigation",
)


class SentinelGateway:
    """Fake Trino gateway returning a fixed synthetic sensitive value."""

    def __init__(self, columns, row):
        self.columns = columns
        self.row = row
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)
        return list(self.columns), [list(self.row)], "fake-query"

    def health(self):
        return {"status": "healthy", "query_id": "fake-health"}


def _governance_request(approval_id=None):
    return GovernanceRequest(
        identity=IDENTITY,
        action="read",
        approval=(
            ApprovalContext(approval_id=approval_id)
            if approval_id
            else None
        ),
        resource=ResourceContext(dataset=BENEFICIARY_DATASET),
    )


def _approval_registry(approval_id):
    """A server-held approval bound to this subject/action/resource (A3-CORE).

    The masking proofs must reach the masking stage, so they run with a
    TRUSTED approval fixture.  A request-supplied ``status="approved"`` is
    correctly rejected and is covered separately by the A2.4 proof.
    """

    return approval_registry(
        approval_id=approval_id,
        subject_id=IDENTITY.subject_id,
        action="read",
        dataset=BENEFICIARY_DATASET,
    )


def _governed_query(sql, gateway, approval_id=None):
    """Execute through the real governed path and return result plus evidence."""

    resolver = TrustedClassificationResolver()

    source_fields = resolver.projected_fields_from_sql(sql)

    tools = ToolRegistry(
        gateway,
        Settings(),
        governance_engine=GovernancePolicyEngine(
            approval_registry=_approval_registry(approval_id),
        ) if approval_id else GovernancePolicyEngine(),
        governance_request=_governance_request(approval_id),
    )

    result = tools.execute_query(sql, 10)

    return result, source_fields


# ---------------------------------------------------------------------------
# A2.5 - alias masking bypass
# ---------------------------------------------------------------------------


def test_alias_must_not_defeat_sensitive_field_masking():
    """``SELECT beneficiary_name AS bn`` must not return the sensitive value.

    The trusted approval below is a deliberate, documented precondition that
    isolates THIS proof from A2.4: the policy engine returns MASK (and never
    ALLOW) for a RESTRICTED field, so an approval must already have been
    granted for the masking stage to be reached at all.  Approval is
    therefore held constant and is not the variable under test.
    """

    sql = (
        "SELECT beneficiary_name AS bn "
        f"FROM {BENEFICIARY_DATASET}"
    )

    gateway = SentinelGateway(["bn"], [SENTINEL])

    result, source_fields = _governed_query(
        sql,
        gateway,
        approval_id="a2-5-granted",
    )

    governance = result["governance"]

    observed = {
        "source_field_classified": source_fields,
        "decision": governance["decision"],
        "masked_fields": governance["masked_fields"],
        "returned_columns": result["columns"],
        "sentinel_in_result": SENTINEL in str(result["rows"]),
        "gateway_executions": len(gateway.executed),
    }

    assert SENTINEL not in str(result["rows"]), (
        "SECURITY: aliasing a sensitive source field must not cause the "
        "original value to escape masking. The source field "
        "'beneficiary_name' is classified RESTRICTED and appears in "
        "masked_fields, but masking is applied by matching masked_fields "
        "against the returned column label, so the alias defeats it.\n"
        f"observed={observed}"
    )
# ---------------------------------------------------------------------------
# A2.6 - SELECT * masking bypass (separate security property from A2.5)
# ---------------------------------------------------------------------------


def test_select_star_must_not_defeat_sensitive_field_masking():
    """``SELECT *`` must not return unmasked sensitive fields.

    The control query below names the sensitive column explicitly.  It is
    recorded as evidence only: it demonstrates that this identity IS subject to
    field-level governance for this data, so any unmasked value returned by
    the ``SELECT *`` query is caused by the star projection alone.
    """

    star_sql = (
        f"SELECT * FROM {BENEFICIARY_DATASET}"
    )

    explicit_sql = (
        "SELECT beneficiary_name "
        f"FROM {BENEFICIARY_DATASET}"
    )

    star_gateway = SentinelGateway(
        ["loan_id", "beneficiary_name", "district"],
        ["L-0001", SENTINEL, "TEST_DISTRICT"],
    )

    #
    # A secure outcome is EITHER a pre-execution denial (the star projection
    # cannot be resolved against trusted metadata) OR an executed query whose
    # sensitive value was masked.  An executed, unmasked value is the only
    # failure this proof accepts.
    #
    star_result = None
    star_source_fields = []
    denied = None

    try:
        star_result, star_source_fields = _governed_query(
            star_sql,
            star_gateway,
        )

    except Exception as exc:
        denied = type(exc).__name__

    control_gateway = SentinelGateway(
        ["beneficiary_name"],
        [SENTINEL],
    )

    control_decision = "executed"

    try:
        _governed_query(explicit_sql, control_gateway)

    except Exception as exc:  # governance block on the named column
        control_decision = type(exc).__name__

    sentinel_escaped = (
        SENTINEL in str(star_result["rows"])
        if star_result is not None
        else False
    )

    observed = {
        "fields_inferred_from_select_star": star_source_fields,
        "denied_before_execution": denied,
        "decision": (
            star_result["governance"]["decision"]
            if star_result is not None
            else None
        ),
        "masked_fields": (
            star_result["governance"]["masked_fields"]
            if star_result is not None
            else None
        ),
        "gateway_result_columns": (
            star_result["columns"]
            if star_result is not None
            else None
        ),
        "sentinel_in_result": sentinel_escaped,
        "gateway_executions": len(star_gateway.executed),
        "control_explicit_projection": control_decision,
    }

    assert not sentinel_escaped, (
        "SECURITY: SELECT * must not allow sensitive fields to leave the "
        "governed tool path unmasked merely because the AST contains no "
        "explicit sensitive projection.\n"
        f"observed={observed}"
    )

    assert denied is not None or len(star_gateway.executed) == 0, (
        "SECURITY: an unresolvable SELECT * must be denied before any "
        "protected execution.\n"
        f"observed={observed}"
    )