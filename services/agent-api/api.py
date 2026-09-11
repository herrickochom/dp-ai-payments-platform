import logging
import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from bi_adapter import (
    DashboardAgentRequest,
    PublicationRequest,
    SupersetAdapter,
)
from builder_models import (
    BuildWorkflowRequest,
    DashboardBuildRequest,
    DataSourceBuildRequest,
    VisualizationBuildRequest,
)
from builders import recommend_visualizations
from config import Settings
from governance import GovernancePolicyEngine
from governance_models import GovernanceRequest
from models import AgentRequest
from orchestrator import Orchestrator
from phase5_agents import GovernanceAgent
from quality_models import (
    DQCheckRequest,
    DQProfileRequest,
    InsightRequest,
)
from tools import ToolRegistry


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)

settings = Settings()

orchestrator = Orchestrator(settings)

governance_policy_engine = GovernancePolicyEngine()

governance_agent = GovernanceAgent(
    policy_engine=governance_policy_engine
)

app = FastAPI(
    title="DP AI Agent API",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Agent catalogue
# ---------------------------------------------------------------------------


@app.get("/agents")
def agents():
    return {
        "agents": [
            {
                "id": "data_discovery",
                "capabilities": [
                    "metadata search",
                    "schema inspection",
                    "join inference",
                ],
            },
            {
                "id": "analytics",
                "capabilities": [
                    "semantic analytical request",
                    "bounded read-only query",
                ],
            },
            {
                "id": "visualization",
                "capabilities": [
                    "bounded visualization recommendation",
                ],
            },
            {
                "id": "dashboard",
                "capabilities": [
                    "bounded dashboard composition",
                ],
            },
            {
                "id": "data_quality",
                "capabilities": [
                    "dbt-backed rules",
                    "bounded profiling",
                    "read-only checks",
                ],
            },
            {
                "id": "insight",
                "capabilities": [
                    "observed-period comparison",
                    "ranking",
                    "explainable anomaly flags",
                ],
            },
            {
                "id": "governance",
                "capabilities": [
                    "deterministic policy evaluation",
                    "RBAC and ABAC evaluation",
                    "data classification",
                    "sensitive-field masking decisions",
                    "beneficiary access control",
                    "approval requirements",
                    "governance explanation",
                ],
            },
        ]
    }


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/agents/health")
def health():
    try:
        platform = orchestrator.gateway.health()

        return {
            "status": "healthy",
            "trino": platform,
            "model_provider": settings.model_provider,
            "rag": (
                "enabled"
                if settings.rag_enabled
                else "not_configured"
            ),
            "governance": {
                "status": "enabled",
                "policy_engine": "deterministic",
                "fail_closed": governance_policy_engine.fail_closed,
            },
        }

    except Exception as exc:
        logger.exception("Agent API health check failed")

        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "trino": {
                    "status": "unavailable",
                    "error_category": type(exc).__name__,
                },
                "governance": {
                    "status": "enabled",
                    "policy_engine": "deterministic",
                },
            },
        )


# ---------------------------------------------------------------------------
# Phase 1 — Discovery / Analytics
# ---------------------------------------------------------------------------


@app.post("/agents/query")
def query(request: AgentRequest):
    response = orchestrator.execute(request)

    if response.status == "completed":
        status_code = 200
    elif response.status == "unsupported":
        status_code = 422
    else:
        status_code = 503

    return JSONResponse(
        status_code=status_code,
        content=response.model_dump(mode="json"),
    )


# ---------------------------------------------------------------------------
# Phase 4 — Data Quality
# ---------------------------------------------------------------------------


@app.post("/agents/data-quality")
def data_quality(request: AgentRequest):
    request.agent = "data_quality"

    response = orchestrator.execute(request)

    return JSONResponse(
        status_code=(
            200
            if response.status == "completed"
            else 422
        ),
        content=response.model_dump(mode="json"),
    )


@app.get("/dq/rules")
def dq_rules(dataset: str | None = None):
    tools = ToolRegistry(
        orchestrator.gateway,
        settings,
    )

    rules = tools.dq_list_rules(dataset)

    return {
        "rules": [
            rule.model_dump(mode="json")
            for rule in rules
        ],
        "count": len(rules),
        "tool_calls": [
            call.model_dump(mode="json")
            for call in tools.calls
        ],
    }


@app.post("/dq/check")
def dq_check(request: DQCheckRequest):
    tools = ToolRegistry(
        orchestrator.gateway,
        settings,
    )

    try:
        finding = tools.dq_run_rule(
            request.rule,
            request.permissions,
        )

        return {
            "status": "completed",
            "finding": finding.model_dump(mode="json"),
            "tool_calls": [
                call.model_dump(mode="json")
                for call in tools.calls
            ],
        }

    except Exception as exc:
        logger.exception(
            "Data quality rule execution failed"
        )

        return JSONResponse(
            status_code=422,
            content={
                "status": "failed",
                "error": {
                    "category": type(exc).__name__,
                    "message": str(exc),
                },
                "tool_calls": [
                    call.model_dump(mode="json")
                    for call in tools.calls
                ],
            },
        )


@app.post("/dq/profile")
def dq_profile(request: DQProfileRequest):
    tools = ToolRegistry(
        orchestrator.gateway,
        settings,
    )

    try:
        profile = tools.dq_profile(request)

        return {
            "status": "completed",
            "profile": profile,
            "tool_calls": [
                call.model_dump(mode="json")
                for call in tools.calls
            ],
        }

    except Exception as exc:
        logger.exception(
            "Data quality profiling failed"
        )

        return JSONResponse(
            status_code=422,
            content={
                "status": "failed",
                "error": {
                    "category": type(exc).__name__,
                    "message": str(exc),
                },
                "tool_calls": [
                    call.model_dump(mode="json")
                    for call in tools.calls
                ],
            },
        )


# ---------------------------------------------------------------------------
# Phase 4 — Insight / Monitoring
# ---------------------------------------------------------------------------


@app.post("/agents/insights")
def insights(request: InsightRequest):
    context = request.model_dump(
        exclude={
            "objective",
            "permissions",
        },
        exclude_none=True,
        mode="json",
    )

    response = orchestrator.execute(
        AgentRequest(
            agent="insight",
            objective=request.objective,
            context=context,
            permissions=request.permissions,
        )
    )

    return JSONResponse(
        status_code=(
            200
            if response.status == "completed"
            else 422
        ),
        content=response.model_dump(mode="json"),
    )


# ---------------------------------------------------------------------------
# Phase 5 — Governance
# ---------------------------------------------------------------------------


@app.post("/governance/evaluate")
def governance_evaluate(
    request: GovernanceRequest,
):
    """
    Evaluate a governance request using deterministic policy logic.

    This endpoint does not invoke an LLM.

    The policy engine evaluates:
    - identity
    - role permissions
    - resource classification
    - field classification
    - geography
    - purpose
    - sensitive-data requirements
    - approval context
    """

    try:
        decision = governance_policy_engine.evaluate(
            request
        )

        return {
            "status": "completed",
            "decision": decision.model_dump(
                mode="json"
            ),
        }

    except Exception as exc:
        logger.exception(
            "Governance policy evaluation failed"
        )

        #
        # Governance failures must fail closed.
        #
        return JSONResponse(
            status_code=422,
            content={
                "status": "failed",
                "decision": "DENY",
                "error": {
                    "category": type(exc).__name__,
                    "message": str(exc),
                },
                "reason": (
                    "Governance evaluation failed. "
                    "Access was not granted."
                ),
            },
        )


@app.post("/agents/governance")
def governance_agent_query(
    request: GovernanceRequest,
):
    """
    Governance Agent endpoint.

    The agent does not make authorization decisions.

    It invokes the deterministic policy engine and explains
    the resulting governance decision.
    """

    try:
        response = governance_agent.evaluate(
            request
        )

        decision = response["decision"]

        return {
            "status": "completed",
            "agent": response["agent"],
            "decision": decision.model_dump(
                mode="json"
            ),
            "explanation": response[
                "explanation"
            ],
        }

    except Exception as exc:
        logger.exception(
            "Governance agent evaluation failed"
        )

        #
        # Fail closed.
        #
        return JSONResponse(
            status_code=422,
            content={
                "status": "failed",
                "decision": "DENY",
                "error": {
                    "category": type(exc).__name__,
                    "message": str(exc),
                },
                "reason": (
                    "Governance agent processing failed. "
                    "Access was not granted."
                ),
            },
        )


# ---------------------------------------------------------------------------
# Phase 2 — Builders
# ---------------------------------------------------------------------------


def _builder_response(operation):
    tools = ToolRegistry(
        orchestrator.gateway,
        settings,
    )

    try:
        artifact = operation(tools)

        return {
            "status": "completed",
            "artifact": artifact.model_dump(
                mode="json"
            ),
            "tool_calls": [
                call.model_dump(mode="json")
                for call in tools.calls
            ],
        }

    except Exception as exc:
        logger.exception(
            "Builder execution failed"
        )

        return JSONResponse(
            status_code=422,
            content={
                "status": "failed",
                "error": {
                    "category": type(exc).__name__,
                    "message": str(exc),
                },
                "tool_calls": [
                    call.model_dump(mode="json")
                    for call in tools.calls
                ],
            },
        )


@app.post("/builders/data-source")
def build_data_source(
    request: DataSourceBuildRequest,
):
    return _builder_response(
        lambda tools: tools.build_data_source(
            request
        )
    )


@app.post("/builders/visualization")
def build_visualization(
    request: VisualizationBuildRequest,
):
    return _builder_response(
        lambda tools: tools.build_visualization(
            request
        )
    )


@app.post("/builders/dashboard")
def build_dashboard(
    request: DashboardBuildRequest,
):
    return _builder_response(
        lambda tools: tools.build_dashboard(
            request
        )
    )


# ---------------------------------------------------------------------------
# Phase 2 — Deterministic analytical build workflow
# ---------------------------------------------------------------------------


@app.post("/agents/build")
def build_workflow(
    request: BuildWorkflowRequest,
):
    analytical = orchestrator.execute(
        AgentRequest(
            agent="analytics",
            objective=request.objective,
            permissions=request.permissions,
        )
    )

    if (
        analytical.status != "completed"
        or not analytical.result.get(
            "data_source"
        )
    ):
        return JSONResponse(
            status_code=422,
            content=analytical.model_dump(
                mode="json"
            ),
        )

    from builder_models import (
        DataSourceSpec,
        LayoutItem,
    )

    data_source = DataSourceSpec.model_validate(
        analytical.result["data_source"]
    )

    tools = ToolRegistry(
        orchestrator.gateway,
        settings,
    )

    try:
        recommendations = (
            recommend_visualizations(
                data_source,
                request.objective,
                request.permissions,
            )
        )

        visualizations = [
            tools.build_visualization(item)
            for item in recommendations
        ]

        layout = [
            LayoutItem(
                visualization_id=item.id,
                x=0,
                y=index * 4,
                width=12,
                height=4,
            )
            for index, item in enumerate(
                visualizations
            )
        ]

        dashboard = tools.build_dashboard(
            DashboardBuildRequest(
                title=request.objective,
                description=(
                    "Deterministically composed "
                    "Phase 2 definition"
                ),
                visualizations=visualizations,
                layout=layout,
                permissions=request.permissions,
            )
        )

        return {
            "status": "completed",
            "analytical_result": (
                analytical.result
            ),
            "data_source": (
                data_source.model_dump(
                    mode="json"
                )
            ),
            "visualizations": [
                item.model_dump(mode="json")
                for item in visualizations
            ],
            "dashboard": dashboard.model_dump(
                mode="json"
            ),
            "tool_calls": [
                call.model_dump(mode="json")
                for call in (
                    analytical.tool_calls
                    + tools.calls
                )
            ],
        }

    except Exception as exc:
        logger.exception(
            "Build workflow failed"
        )

        return JSONResponse(
            status_code=422,
            content={
                "status": "failed",
                "error": {
                    "category": type(exc).__name__,
                    "message": str(exc),
                },
            },
        )


# ---------------------------------------------------------------------------
# Phase 3 — Visualization Agent
# ---------------------------------------------------------------------------


@app.post("/agents/visualize")
def visualize(
    request: BuildWorkflowRequest,
):
    response = orchestrator.execute(
        AgentRequest(
            agent="visualization",
            objective=request.objective,
            permissions=request.permissions,
        )
    )

    return JSONResponse(
        status_code=(
            200
            if response.status == "completed"
            else 422
        ),
        content=response.model_dump(mode="json"),
    )


# ---------------------------------------------------------------------------
# Phase 3 — Dashboard Agent
# ---------------------------------------------------------------------------


@app.post("/agents/dashboard")
def dashboard(
    request: DashboardAgentRequest,
):
    response = orchestrator.execute(
        AgentRequest(
            agent="dashboard",
            objective=request.objective,
            permissions=request.permissions,
        )
    )

    if (
        response.status != "completed"
        or not response.result.get(
            "dashboard"
        )
    ):
        return JSONResponse(
            status_code=422,
            content=response.model_dump(
                mode="json"
            ),
        )

    from builder_models import DashboardSpec

    artifact = DashboardSpec.model_validate(
        response.result["dashboard"]
    )

    adapter = SupersetAdapter(settings)

    try:
        if request.publish:
            publication = adapter.publish(
                artifact,
                request.permissions,
            )
        else:
            publication = adapter.plan(
                artifact
            )

    except Exception as exc:
        logger.exception(
            "Dashboard publication failed"
        )

        return JSONResponse(
            status_code=502,
            content={
                "status": "failed",
                "error": {
                    "category": type(exc).__name__,
                    "message": str(exc),
                },
                "dashboard": (
                    artifact.model_dump(
                        mode="json"
                    )
                ),
            },
        )

    return {
        "status": "completed",
        "agent_response": (
            response.model_dump(
                mode="json"
            )
        ),
        "dashboard": artifact.model_dump(
            mode="json"
        ),
        "superset": publication,
    }


# ---------------------------------------------------------------------------
# Phase 3 — Direct Superset Adapter
# ---------------------------------------------------------------------------


@app.post("/publish/superset")
def publish_superset(
    request: PublicationRequest,
):
    adapter = SupersetAdapter(settings)

    try:
        if request.publish:
            result = adapter.publish(
                request.dashboard,
                request.permissions,
            )
        else:
            result = adapter.plan(
                request.dashboard
            )

        return {
            "status": "completed",
            "superset": result,
        }

    except Exception as exc:
        logger.exception(
            "Superset publication failed"
        )

        return JSONResponse(
            status_code=502,
            content={
                "status": "failed",
                "error": {
                    "category": type(exc).__name__,
                    "message": str(exc),
                },
            },
        )