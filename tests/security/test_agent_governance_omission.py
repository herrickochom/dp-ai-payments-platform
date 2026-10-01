"""JOB A2.1 - FAIL-FIRST PROOF: governance omission on the agent query path.

A1 finding under test
---------------------
``Orchestrator._governance_request()`` returns ``None`` whenever the caller
omits ``context.governance``.  ``ToolRegistry.execute_query()`` treats that
``None`` as "no governance applies to this query" and therefore skips trusted
classification, policy evaluation and masking, while still calling the Trino
gateway.

Security property under test
----------------------------
Missing governance must prevent execution.

These tests are intentionally FAIL-FIRST: they assert the security property
the platform must have, not the behaviour it currently has.  A failure here is
recorded as::

    A2_1_GOVERNANCE_OMISSION=VULNERABILITY_PROVEN

They are deterministic and offline.  No live Trino, Superset, container or
network access is required or performed.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

if str(AGENT_API_DIR) not in sys.path:
    sys.path.insert(0, str(AGENT_API_DIR))


from classification import TrustedClassificationResolver
from config import Settings
from governance import GovernancePolicyEngine
from models import AgentRequest
from orchestrator import Orchestrator
from tools import ToolRegistry


# ---------------------------------------------------------------------------
# Offline doubles
# ---------------------------------------------------------------------------


class RecordingGateway:
    """Fake Trino gateway. Records every statement and never opens a socket."""

    INFORMATION_SCHEMA = "information_schema"

    def __init__(self):
        self.executed = []

    @property
    def analytical(self):
        """Statements that read governed data, not catalog metadata."""
        return [
            statement
            for statement in self.executed
            if self.INFORMATION_SCHEMA not in statement.lower()
        ]

    def execute(self, sql):
        self.executed.append(sql)

        lowered = sql.lower()

        if self.INFORMATION_SCHEMA in lowered and "where table_schema" in lowered:
            return (
                ["column_name", "data_type", "ordinal_position"],
                [
                    ["district", "varchar", 1],
                    ["region", "varchar", 2],
                    ["principal_repayment_rate", "double", 3],
                    ["loan_count", "bigint", 4],
                ],
                "fake-describe",
            )

        if self.INFORMATION_SCHEMA in lowered:
            return (
                ["table_schema", "table_name", "column_name", "data_type"],
                [
                    [
                        "consumption",
                        "cns_pdm_local_government_performance",
                        "district",
                        "varchar",
                    ],
                    [
                        "consumption",
                        "cns_pdm_local_government_performance",
                        "principal_repayment_rate",
                        "double",
                    ],
                ],
                "fake-search",
            )

        return (
            ["district", "repayment_performance"],
            [["Kabale", 0.24]],
            "fake-analytical",
        )

    def health(self):
        return {"status": "healthy", "query_id": "fake-health"}


class RecordingClassificationResolver:
    """Delegating wrapper that records every trusted classification request."""

    def __init__(self):
        self.resolver = TrustedClassificationResolver()
        self.resolved = []

    def resolve_sql(self, sql, district=None, parish=None):
        self.resolved.append(sql)
        return self.resolver.resolve_sql(
            sql,
            district=district,
            parish=parish,
        )


# ---------------------------------------------------------------------------
# A2.1 - boundary proof: ToolRegistry.execute_query with no governance context
# ---------------------------------------------------------------------------


def test_a2_1_missing_governance_context_must_prevent_execution():
    """A query with no governance context must never reach Trino.

    ``Orchestrator._governance_request()`` yields ``None`` for such a request,
    so ``ToolRegistry`` is constructed with ``governance_request=None``.  That
    is the exact state reproduced here.

    The security property under test is NO PROTECTED EXECUTION.  Whether the
    service denies up front or classifies and then denies is an
    implementation detail; the classification and evaluation counters are
    retained purely as evidence of which path was taken.
    """

    gateway = RecordingGateway()
    classifier = RecordingClassificationResolver()
    engine = RecordingGovernanceEngine()

    tools = ToolRegistry(
        gateway,
        Settings(),
        governance_engine=engine,
        classification_resolver=classifier,
        governance_request=None,
    )

    denied = False

    try:
        tools.execute_query(LOAN_SQL, 10)

    except Exception:
        denied = True

    observed = {
        "denied_before_execution": denied,
        "trusted_classifications": len(classifier.resolved),
        "governance_evaluations": len(engine.evaluated),
        "trino_executions": len(gateway.executed),
    }

    assert gateway.executed == [], (
        "SECURITY: a query that carries no governance context must never be "
        "executed, no matter how the service chooses to fail closed.\n"
        f"observed={observed}\n"
        "required={'trino_executions': 0}"
    )


# ---------------------------------------------------------------------------
# A2.1 - reachability proof: the omission is reachable from the public path
# ---------------------------------------------------------------------------


def test_a2_1_orchestrator_executes_query_when_governance_context_is_absent(
    monkeypatch,
):
    """End-to-end through ``Orchestrator.execute`` with a governance-free body.

    This is the request an API caller can send today: an objective, and no
    ``context.governance`` at all.
    """

    gateway = RecordingGateway()
    engine = RecordingGovernanceEngine()

    classifications = []
    resolve_sql = TrustedClassificationResolver.resolve_sql

    def spy_resolve_sql(self, sql, district=None, parish=None):
        classifications.append(sql)
        return resolve_sql(
            self,
            sql,
            district=district,
            parish=parish,
        )

    monkeypatch.setattr(
        TrustedClassificationResolver,
        "resolve_sql",
        spy_resolve_sql,
    )

    orchestrator = Orchestrator(
        Settings(),
        gateway=gateway,
        governance_engine=engine,
    )

    request = AgentRequest(
        objective="Show district repayment performance",
        context={},
    )

    #
    # The production resolution step, executed by Orchestrator.execute().
    #
    resolved_context = orchestrator._governance_request(request)

    orchestrator.execute(request)

    observed = {
        "governance_context": resolved_context,
        "trusted_classifications": len(classifications),
        "governance_evaluations": len(engine.evaluated),
        "analytical_executions": len(gateway.analytical),
    }

    assert gateway.analytical == [], (
        "SECURITY: absent governance context must prevent governed execution.\n"
        f"observed={observed}\n"
        "required={'analytical_executions': 0}"
    )
class RecordingGovernanceEngine:
    """Delegating wrapper that records every policy evaluation."""

    def __init__(self):
        self.engine = GovernancePolicyEngine()
        self.evaluated = []

    def evaluate(self, request):
        self.evaluated.append(request)
        return self.engine.evaluate(request)


LOAN_SQL = "SELECT loan_id FROM iceberg.silver.slv_pdm_loans"