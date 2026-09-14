import json
import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services/agent-api"))

import api
from audit import JsonlAuditStore
from bi_adapter import AdapterError, AdapterPermissionDenied, SupersetAdapter
from builder_models import (
    DashboardBuildRequest, DataSourceBuildRequest, Encoding, LayoutItem,
    VisualizationBuildRequest,
)
from builders import DashboardBuilder, DataSourceBuilder, VisualizationBuilder
from config import Settings
from governance import GovernancePolicyEngine
from governance_models import (
    ApprovalContext, DataClassification, GovernanceRequest, IdentityContext,
    ResourceContext,
)
from knowledge import LocalKnowledgeRetriever
from knowledge_models import KnowledgeDocument, KnowledgeMetadata, KnowledgeQuery
from models import AnalyticalRequest, Measure, Permissions
from multi_agent import MultiAgentOrchestrator
from orchestration_models import (
    AgentEvidence, AgentResult, AgentTask, ExecutionContext, OrchestrationStatus,
)
from tools import (
    GovernanceDenied, ToolLimitExceeded, ToolRegistry, ToolTimeout,
    validate_read_query,
)


class HealthyGateway:
    def health(self): return {"status": "healthy", "query_id": "health-query"}


class UnhealthyGateway:
    def health(self): raise TimeoutError("private dependency detail")


DATASET = "iceberg.consumption.cns_pdm_local_government_performance"
COLUMNS = [
    ["district", "varchar", 1], ["region", "varchar", 2],
    ["principal_repayment_rate", "double", 3], ["loan_count", "bigint", 4],
    ["reporting_month", "date", 5], ["label", "varchar", 6],
]


class SmokeGateway:
    """Offline fake gateway: canned metadata and query rows, no network."""

    def __init__(self, columns=None, rows=None):
        self.columns = columns or ["district", "repayment_performance"]
        self.rows = rows or [["Kabale", 0.24]]
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)
        lowered = sql.lower()
        if "information_schema.columns" in lowered and "where table_schema" in lowered:
            return ["column_name", "data_type", "ordinal_position"], COLUMNS, "q-describe"
        if "information_schema.columns" in lowered:
            rows = [["consumption", "cns_pdm_local_government_performance", *row[:2]] for row in COLUMNS]
            return ["table_schema", "table_name", "column_name", "data_type"], rows, "q-search"
        return self.columns, [list(r) for r in self.rows], "q-data"

    def health(self):
        return {"status": "healthy", "query_id": "q-health"}


def identity(subject="analyst-1", roles=None, permissions=None, purpose="programme_monitoring"):
    return IdentityContext(subject_id=subject, roles=list(roles or ["programme_analyst"]),
                           permissions=list(permissions or ["can_view_internal_data"]),
                           purpose=purpose)


def test_configuration_fails_fast_for_invalid_critical_values():
    try:
        Settings(trino_port=70000)
        assert False, "invalid port accepted"
    except ValueError as exc:
        assert "TRINO_PORT" in str(exc)


def test_liveness_readiness_and_request_correlation(monkeypatch):
    monkeypatch.setattr(api.orchestrator, "gateway", HealthyGateway())
    client = TestClient(api.app)
    assert client.get("/health/live").json() == {"status": "alive"}
    ready = client.get("/health/ready", headers={"x-request-id": "smoke-123"})
    assert ready.status_code == 200 and ready.headers["x-request-id"] == "smoke-123"
    monkeypatch.setattr(api.orchestrator, "gateway", UnhealthyGateway())
    unavailable = client.get("/health/ready")
    assert unavailable.status_code == 503
    assert "private dependency detail" not in unavailable.text


def test_governance_audit_is_durable_structured_and_payload_free(tmp_path):
    request = GovernanceRequest(
        identity=IdentityContext(subject_id="analyst", roles=["programme_analyst"],
                                 purpose="programme_monitoring"), action="read",
        resource=ResourceContext(dataset="iceberg.silver.slv_pdm_loans",
                                 classification=DataClassification.INTERNAL),
    )
    decision = GovernancePolicyEngine().evaluate(request)
    store = JsonlAuditStore(tmp_path / "audit.jsonl")
    event = store.record_decision(request, decision, "request-7")
    persisted = json.loads((tmp_path / "audit.jsonl").read_text())
    assert persisted["audit_event_id"] == event.audit_event_id
    assert persisted["request_id"] == "request-7"
    assert "payload" not in persisted and "rows" not in persisted


def test_bounded_production_smoke_orchestration_preserves_tool_evidence():
    def handler(step, _task, _dependencies):
        return AgentResult(output={"dry_run": True}, tool_calls=1,
                           evidence=[AgentEvidence(agent=step.agent, kind="tool",
                                                   reference="safe-query", tool="query.execute",
                                                   request={"kind": "structured_read"})])
    orchestrator = MultiAgentOrchestrator({"analytics": handler}, lambda *_: "ALLOW")
    task = AgentTask(objective="Show district repayment performance",
                     identity=IdentityContext(subject_id="pilot-user"))
    result = orchestrator.execute(task, ExecutionContext(request_id="smoke", identity=task.identity))
    assert result.status.value == "COMPLETED"
    assert result.outputs["step-1"]["dry_run"] is True
    assert result.evidence[0].tool == "query.execute"
# ---------------------------------------------------------------------------
# 9.2 / 9.3  Hardening controls: timeouts, retries, request limits, auth hook
# ---------------------------------------------------------------------------


def test_tool_timeout_is_bounded_and_structured():
    def stall():
        time.sleep(2)
        return {"slow": True}

    tools = ToolRegistry(SmokeGateway(), Settings(tool_timeout_seconds=1, tool_max_retries=0))
    with pytest.raises(ToolTimeout):
        tools._call("query.slow", stall)
    assert tools.calls[0].status == "failed"
    assert tools.calls[0].metadata["error_category"] == "ToolTimeout"


def test_bounded_transient_retry_records_evidence():
    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectionError("transient downstream reset")
        return {"ok": True}

    tools = ToolRegistry(SmokeGateway(), Settings(tool_max_retries=1))
    assert tools._call("query.retryable", flaky) == {"ok": True}
    assert tools.calls[0].status == "completed"
    assert tools.calls[0].metadata.get("retries") == 1


def test_non_transient_failures_and_tool_limits_are_not_retried():
    calls = {"count": 0}

    def broken():
        calls["count"] += 1
        raise ValueError("deterministic domain failure")

    tools = ToolRegistry(SmokeGateway(), Settings(tool_max_retries=2))
    with pytest.raises(ValueError):
        tools._call("query.fail", broken)
    assert calls["count"] == 1
    assert tools.calls[0].metadata["error_category"] == "ValueError"

    limited = ToolRegistry(SmokeGateway(), Settings(max_tool_calls=1))
    limited.platform_health()
    with pytest.raises(ToolLimitExceeded):
        limited.platform_health()


def test_oversized_request_body_is_rejected_before_handlers(monkeypatch):
    monkeypatch.setattr(api, "settings", Settings(max_request_bytes=1024))
    client = TestClient(api.app)
    response = client.post("/health/live", content=b"x" * 4096)
    assert response.status_code == 413
    assert response.json()["error"]["category"] == "RequestTooLarge"


def test_optional_auth_hook_blocks_anonymous_when_enabled(monkeypatch):
    monkeypatch.setattr(api, "settings", Settings(auth_enabled=True))
    client = TestClient(api.app)
    denied = client.get("/health/live")
    assert denied.status_code == 401
    allowed = client.get("/health/live", headers={"x-subject-id": "analyst-1"})
    assert allowed.status_code == 200


def test_request_timeout_returns_504_without_stack_trace(monkeypatch):
    @api.app.get("/health/slow")
    def slow():
        time.sleep(2)
        return {"status": "alive"}

    monkeypatch.setattr(api, "settings", Settings(request_timeout_seconds=1,
                                                  tool_timeout_seconds=10))
    client = TestClient(api.app)
    response = client.get("/health/slow")
    assert response.status_code == 504
    assert response.json()["error"]["category"] == "RequestTimeout"
    assert "Traceback" not in response.text
# ---------------------------------------------------------------------------
# 9.10  Bounded local production smoke suite (no network, no live platform)
# ---------------------------------------------------------------------------


def test_smoke_metadata_lookup_and_safe_trino_query():
    gateway = SmokeGateway()
    tools = ToolRegistry(gateway, Settings())
    found = tools.search("district repayment")
    assert found and found[0]["table"] == "cns_pdm_local_government_performance"

    governed = GovernanceRequest(
        identity=identity(),
        action="read",
        resource=ResourceContext(dataset="iceberg.silver.slv_pdm_loans",
                                 classification=DataClassification.INTERNAL),
    )
    result = tools.execute_query(
        "SELECT district, principal_repayment_rate FROM iceberg.silver.slv_pdm_loans",
        25, governed,
    )
    assert result["row_count"] == 1
    assert result["governance"]["decision"] == "ALLOW"
    assert "LIMIT 25" in result["sql"].upper()


def test_smoke_rag_retrieval_through_tool_boundary():
    retriever = LocalKnowledgeRetriever(chunk_size=400)
    retriever.ingest(KnowledgeDocument(
        content="A delayed payment remains pending beyond the policy service window.",
        metadata=KnowledgeMetadata(title="PDM payment policy", source_type="policy",
                                   source_uri="policy://pdm/payments"),
    ))
    tools = ToolRegistry(SmokeGateway(), Settings(), knowledge_retriever=retriever)
    retrieval = tools.rag_search(KnowledgeQuery(
        query="definition delayed payment policy",
        user_context=identity(), limit=3,
    ))
    assert retrieval["hits"]
    assert retrieval["citations"][0]["title"] == "PDM payment policy"
    assert retrieval["backend"] == "local_deterministic"


def test_smoke_dq_catalogue_request():
    tools = ToolRegistry(SmokeGateway(), Settings())
    rules = tools.dq_list_rules(dataset="iceberg.silver.slv_pdm_loans")
    assert rules and any(rule.rule_type == "not_null" for rule in rules)


def test_smoke_dashboard_dry_run_is_offline_and_governed():
    tools = ToolRegistry(SmokeGateway(), Settings())
    semantic = AnalyticalRequest(
        datasets=[DATASET], dimensions=["district"],
        measures=[Measure(name="repayment", expression="principal_repayment_rate",
                          aggregation="avg")],
        group_by=["district"], limit=20,
    )
    source = DataSourceBuilder().build(
        DataSourceBuildRequest(semantic_request=semantic, semantic_request_id="s-1",
                               query_id="q-1"), tools)
    visual = VisualizationBuilder().build(VisualizationBuildRequest(
        id="v-1", type="bar", title="districts", data_source=source,
        encoding=[Encoding(channel="x", field="district", role="dimension"),
                  Encoding(channel="y", field="principal_repayment_rate", role="measure")],
        permissions=Permissions()))
    dashboard = DashboardBuilder().build(DashboardBuildRequest(
        title="districts at repayment risk", visualizations=[visual],
        layout=[LayoutItem(visualization_id="v-1", x=0, y=0, width=12, height=4)],
        permissions=Permissions()))

    plan = SupersetAdapter(Settings()).plan(dashboard)
    assert plan["mode"] == "dry_run" and plan["status"] == "validated"

    with pytest.raises(AdapterError) as excinfo:
        SupersetAdapter(Settings()).publish(dashboard, Permissions(can_publish_bi_assets=True))
    assert "SUPERSET_PASSWORD" in str(excinfo.value)
def test_smoke_restricted_beneficiary_publication_fails_closed():
    tools = ToolRegistry(SmokeGateway(columns=[["beneficiary_name", "varchar", 1],
                                               ["district", "varchar", 2]]), Settings())
    dataset = "iceberg.silver.slv_pdm_beneficiaries"
    semantic = AnalyticalRequest(datasets=[dataset], dimensions=["district"],
                                 measures=[Measure(name="count", expression="loan_count",
                                                   aggregation="count")],
                                 group_by=["district"], limit=20)
    source = DataSourceBuilder().build(
        DataSourceBuildRequest(semantic_request=semantic, semantic_request_id="s-restricted",
                               query_id="q-2"), tools)
    visual = VisualizationBuilder().build(VisualizationBuildRequest(
        id="v-r", type="bar", title="restricted", data_source=source,
        encoding=[Encoding(channel="x", field="district", role="dimension"),
                  Encoding(channel="y", field="loan_count", role="measure")],
        permissions=Permissions()))
    dashboard = DashboardBuilder().build(DashboardBuildRequest(
        title="restricted beneficiary view", visualizations=[visual],
        layout=[LayoutItem(visualization_id="v-r", x=0, y=0, width=12, height=4)],
        permissions=Permissions()))
    adapter = SupersetAdapter(Settings(superset_password="x"))
    with pytest.raises(AdapterPermissionDenied) as excinfo:
        adapter.publish(dashboard, Permissions(can_publish_bi_assets=True))
    assert "RESTRICTED" in str(excinfo.value)


def test_smoke_technical_payment_lifecycle_query_is_governed():
    validated = validate_read_query(
        "SELECT technical_status, technical_stage, event_timestamp "
        "FROM iceberg.silver.slv_pdm_payments_plm_lifecycle_events",
        50, 10000,
    )
    assert validated["valid"] and "LIMIT 50" in validated["sql"].upper()

    tools = ToolRegistry(SmokeGateway(columns=["technical_status", "technical_stage",
                                               "event_timestamp"],
                                      rows=[["REJECTED", "PLM_RESPONSE", "2026-08-01"]]),
                         Settings())
    governed = GovernanceRequest(
        identity=identity(),
        action="read",
        resource=ResourceContext(dataset="iceberg.silver.slv_pdm_payments_plm_lifecycle_events",
                                 classification=DataClassification.INTERNAL),
    )
    result = tools.execute_query(validated["sql"], 50, governed)
    assert result["row_count"] == 1
    assert result["governance"]["decision"] == "ALLOW"
    assert result["columns"][0] == "technical_status"


def test_smoke_unauthorised_beneficiary_query_is_denied_or_masked():
    gateway = SmokeGateway()
    tools = ToolRegistry(gateway, Settings())
    denied = GovernanceRequest(
        identity=IdentityContext(subject_id="analyst", roles=["programme_analyst"],
                                 permissions=["can_view_internal_data"],
                                 purpose="programme_monitoring"),
        action="read",
        resource=ResourceContext(dataset="iceberg.silver.slv_pdm_beneficiaries",
                                 classification=DataClassification.RESTRICTED),
    )
    with pytest.raises(GovernanceDenied):
        tools.execute_query(
            "SELECT district, beneficiary_name FROM iceberg.silver.slv_pdm_beneficiaries",
            10, denied,
        )
    assert gateway.executed == []

    masked = GovernanceRequest(
        identity=IdentityContext(subject_id="investigator", roles=["investigator"],
                                 permissions=["can_view_internal_data",
                                              "can_view_restricted_data",
                                              "can_view_beneficiary_data"],
                                 purpose="investigation"),
        action="read",
        approval=ApprovalContext(approval_id="approval-smoke", status="approved",
                                 approver="governance-admin"),
        resource=ResourceContext(dataset="iceberg.silver.slv_pdm_beneficiaries",
                                 classification=DataClassification.RESTRICTED,
                                 fields=["district", "beneficiary_name"]),
    )
    result = tools.execute_query(
        "SELECT district, beneficiary_name FROM iceberg.silver.slv_pdm_beneficiaries",
        10, masked,
    )
    assert result["governance"]["decision"] == "MASK"
    assert result["rows"][0][1] != "Jane Doe"
def test_smoke_audit_event_created_for_protected_action_denial(tmp_path):
    store = JsonlAuditStore(tmp_path / "audit.jsonl")
    gateway = SmokeGateway()
    tools = ToolRegistry(gateway, Settings(), audit_store=store, request_id="req-protected")
    denied = GovernanceRequest(
        identity=IdentityContext(subject_id="analyst", roles=["programme_analyst"],
                                 permissions=["can_view_internal_data"]),
        action="read",
        resource=ResourceContext(dataset="iceberg.silver.slv_pdm_beneficiaries",
                                 classification=DataClassification.RESTRICTED),
    )
    with pytest.raises(GovernanceDenied):
        tools.execute_query(
            "SELECT beneficiary_name FROM iceberg.silver.slv_pdm_beneficiaries", 10, denied,
        )
    persisted = json.loads((tmp_path / "audit.jsonl").read_text())
    assert persisted["decision"] == "DENY"
    assert persisted["request_id"] == "req-protected"
    assert persisted["resource"] == "iceberg.silver.slv_pdm_beneficiaries"
    assert "payload" not in persisted


def test_smoke_multi_agent_analytical_request_and_governance_denial():
    def analytics(step, _task, dependencies):
        return AgentResult(output={"agent": step.agent, "deps": sorted(dependencies)},
                           tool_calls=1,
                           evidence=[AgentEvidence(agent="analytics", kind="tool",
                                                   reference="trino-1", tool="query.execute")])
    orchestrator = MultiAgentOrchestrator({"analytics": analytics}, lambda *_: "ALLOW")
    task = AgentTask(objective="Show district repayment performance",
                     identity=IdentityContext(subject_id="analyst-1"))
    result = orchestrator.execute(task, ExecutionContext(request_id="smoke-analytical",
                                                         identity=task.identity))
    assert result.status == OrchestrationStatus.COMPLETED
    assert result.plan.steps[0].agent == "analytics"

    def guard(_step, requested, _phase):
        return "DENY" if "restricted beneficiary" in requested.objective.lower() else "ALLOW"
    denied = MultiAgentOrchestrator({"analytics": analytics}, guard)
    blocked = denied.execute(
        AgentTask(objective="publish restricted beneficiary detail",
                  identity=IdentityContext(subject_id="analyst-1")),
        ExecutionContext(request_id="smoke-denied", identity=task.identity))
    assert blocked.status == OrchestrationStatus.DENIED


def test_destructive_sql_is_rejected_at_validation():
    for destructive in (
        "DROP TABLE iceberg.silver.slv_pdm_loans",
        "DELETE FROM iceberg.silver.slv_pdm_loans",
        "GRANT ALL ON iceberg.silver.slv_pdm_loans TO analyst",
    ):
        with pytest.raises(Exception):
            validate_read_query(destructive, 10, 10000)
