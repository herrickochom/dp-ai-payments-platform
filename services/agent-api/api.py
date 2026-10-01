import logging
import os
import asyncio
import json
import re
import time
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from authentication import (
    AuthenticationError,
    JwksSigningKeyResolver,
    TokenVerifier,
)
from authority import (
    Authority,
    ServerAuthorityResolver,
    publication_authority,
    unauthenticated,
)
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
from role_bindings import load_role_bindings
from phase5_agents import GovernanceAgent
from quality_models import (
    DQCheckRequest,
    DQProfileRequest,
    InsightRequest,
)
from tools import ToolRegistry
from audit import JsonlAuditStore


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)

settings = Settings()

audit_store = JsonlAuditStore(settings.audit_log_path)

#
# F1 server-owned subject-to-role bindings.  Read from server configuration
# only; never from a request body, header, or token claim.
#
role_authorities = load_role_bindings(
    settings.auth_role_bindings_file
)

#
# F1 trusted identity.  With authentication disabled the verifier exists but is
# never consulted, so the A3/A4 fail-closed posture is preserved exactly.
#
token_verifier = TokenVerifier(
    issuer=settings.auth_issuer or "unconfigured-issuer",
    audience=settings.auth_audience or "unconfigured-audience",
    signing_keys=JwksSigningKeyResolver(
        settings.auth_jwks_url or "https://unconfigured.invalid/jwks.json",
        cache_seconds=settings.auth_jwks_cache_seconds,
    ),
    allowed_algorithms=tuple(
        algorithm.strip().upper()
        for algorithm in settings.auth_allowed_algorithms.split(",")
        if algorithm.strip()
    ),
    clock_skew_seconds=settings.auth_clock_skew_seconds,
)

orchestrator = Orchestrator(
    settings,
    audit_store=audit_store,
    authority_resolver=ServerAuthorityResolver(
        role_authorities=role_authorities,
        max_rows=settings.max_rows,
    ),
)

governance_policy_engine = GovernancePolicyEngine()

governance_agent = GovernanceAgent(
    policy_engine=governance_policy_engine
)

#
# Paths exempt from authentication: liveness/readiness probes only.  The
# container healthcheck calls /agents/health unauthenticated, and none of
# these expose governed platform data.
#
HEALTH_PATHS = frozenset({
    "/health/live",
    "/health/ready",
    "/agents/health",
})


app = FastAPI(
    title="DP AI Agent API",
    version="1.0.0",
)


@app.middleware("http")
async def request_context(request: Request, call_next):
    supplied = request.headers.get("x-request-id", "")
    request_id = supplied if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", supplied) else str(uuid4())
    request.state.request_id = request_id
    started = time.monotonic()

    #
    # Bounded request size. A body larger than the configured ceiling is
    # rejected before any handler runs, so oversized payloads cannot exhaust
    # memory or hide in slow reads.
    #
    content_length = request.headers.get("content-length")
    if content_length is not None and content_length.isdigit() and \
            int(content_length) > settings.max_request_bytes:
        return JSONResponse(status_code=413, content={"status": "payload_too_large",
                            "request_id": request_id,
                            "error": {"category": "RequestTooLarge",
                                      "message": "Request body exceeds the configured size limit"}})

    logger.info(json.dumps({"event": "request_received", "request_id": request_id,
                            "method": request.method, "path": request.url.path}))
    try:
        response = await asyncio.wait_for(call_next(request), timeout=settings.request_timeout_seconds)
    except asyncio.TimeoutError:
        logger.info(json.dumps({"event": "request_timeout", "request_id": request_id}))
        return JSONResponse(status_code=504, content={"status": "timed_out", "request_id": request_id,
                            "error": {"category": "RequestTimeout",
                                      "message": "Request processing exceeded the configured timeout"}})
    except Exception:
        logger.exception(json.dumps({"event": "request_failed", "request_id": request_id}))
        return JSONResponse(status_code=500, content={"status": "failed", "request_id": request_id,
                                                     "error": {"category": "InternalError",
                                                               "message": "Request processing failed"}})
    response.headers["x-request-id"] = request_id
    logger.info(json.dumps({"event": "request_completed", "request_id": request_id,
                            "status_code": response.status_code,
                            "duration_ms": round((time.monotonic() - started) * 1000, 2)}))
    return response


@app.middleware("http")
async def authentication_hook(request: Request, call_next):
    """Authenticate the caller with a cryptographically verified bearer token.

    F1 replaces the previous ``x-subject-id`` presence check.  A bare header
    can no longer make ``subject_verified`` true, so it can never grant
    authority.

    When ``AGENT_AUTH_ENABLED`` is false the service keeps the deliberate A3/A4
    posture: no trusted identity, no governed-data authority, no privileged
    publication.  Discovery (metadata) remains available, which is exactly the
    surface the A3 contract permits.

    Health endpoints are exempt because the container healthcheck probes
    ``/agents/health`` unauthenticated; they expose no governed data.
    """

    if request.url.path in HEALTH_PATHS:
        request.state.subject_id = "health-probe"
        request.state.subject_verified = False
        request.state.trusted_identity = None
        request.state.authority = unauthenticated()
        return await call_next(request)

    trusted_identity = None

    if settings.auth_enabled:
        try:
            trusted_identity = token_verifier.verify(
                request.headers.get("authorization")
            )

        except AuthenticationError as exc:
            logger.warning(
                json.dumps(
                    {
                        "event": "authentication_failed",
                        "path": request.url.path,
                        "error_category": exc.category,
                    }
                )
            )
            request.state.subject_verified = False
            request.state.trusted_identity = None
            request.state.authority = unauthenticated()

            #
            # Generic client-facing message: never the expected issuer,
            # audience, key id, or a raw library exception.
            #
            return JSONResponse(
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
                content={
                    "status": "unauthenticated",
                    "error": {
                        "category": exc.category,
                        "message": "Valid authentication is required.",
                    },
                },
            )

    #
    # `x-subject-id` survives ONLY as an untrusted diagnostic hint.  It is
    # never consulted for authority and never sets subject_verified.
    #
    request.state.subject_id = (
        trusted_identity.subject_id
        if trusted_identity is not None
        else request.headers.get("x-subject-id", "local-pilot-user")
    )
    request.state.subject_verified = trusted_identity is not None
    request.state.trusted_identity = trusted_identity
    request.state.authority = authority_for_identity(
        trusted_identity,
        role_authorities,
    )

    return await call_next(request)


def authority_for_identity(
    identity,
    bindings: dict[str, frozenset[str]],
) -> Authority:
    """Server-derived authority for a verified identity.

    Roles come from the server-owned subject-role binding ONLY.  Token claims
    are never consulted, and an unknown subject resolves to no roles at all:
    authenticated, but unprivileged.
    """

    if identity is None or not getattr(identity, "authenticated", False):
        return unauthenticated()

    return ServerAuthorityResolver(
        role_authorities=bindings,
    ).resolve_verified(identity.subject_id)


def _publication_authority_for(request: Request) -> Authority:
    """Server-derived publication authority for the current request.

    Requires a cryptographically verified identity AND a server-side
    publication-role binding.  A caller-supplied
    ``permissions.can_publish_bi_assets`` flag cannot reach this result.
    """

    identity = getattr(request.state, "trusted_identity", None)

    if identity is None or not getattr(identity, "authenticated", False):
        return unauthenticated()

    return publication_authority(
        identity.subject_id,
        tuple(sorted(role_authorities.get(identity.subject_id, frozenset()))),
        authenticated=True,
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


@app.get("/health/live")
def liveness():
    return {"status": "alive"}


@app.get("/health/ready")
def readiness():
    try:
        dependency = orchestrator.gateway.health()
        if dependency.get("status") != "healthy":
            raise RuntimeError("required analytical dependency is unhealthy")
        return {"status": "ready", "required": {"trino": "healthy"},
                "optional": {"rag": "enabled" if settings.rag_enabled else "not_configured"}}
    except Exception as exc:
        return JSONResponse(status_code=503, content={"status": "not_ready",
                            "required": {"trino": "unavailable"},
                            "error_category": type(exc).__name__})


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
    http_request: Request,
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
        audit_event = audit_store.record_decision(
            request, decision, http_request.state.request_id
        )

        return {
            "status": "completed",
            "decision": decision.model_dump(
                mode="json"
            ),
            "audit_event_id": audit_event.audit_event_id,
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
    http_request: Request,
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

        audit_event = audit_store.record_decision(
            request, decision, http_request.state.request_id
        )

        return {
            "status": "completed",
            "agent": response["agent"],
            "decision": decision.model_dump(
                mode="json"
            ),
            "explanation": response[
                "explanation"
            ],
            "audit_event_id": audit_event.audit_event_id,
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
            context=request.context,
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
            context=request.context,
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
    http_request: Request,
):
    response = orchestrator.execute(
        AgentRequest(
            agent="dashboard",
            objective=request.objective,
            context=request.context,
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

    #
    # A2.7: publication authority is server-derived, never the request body's
    # `permissions.can_publish_bi_assets`.
    #
    authority = _publication_authority_for(http_request)

    if request.publish and not authority.permits(
        "can_publish_bi_assets",
        require_authenticated=True,
    ):
        return JSONResponse(
            status_code=403,
            content={
                "status": "forbidden",
                "error": {
                    "category": "PublicationAuthorityRequired",
                    "message": (
                        "BI publication requires a server-authorised, "
                        "authenticated publication role."
                    ),
                },
            },
        )

    adapter = SupersetAdapter(settings)

    try:
        if request.publish:
            publication = adapter.publish(
                artifact,
                authority,
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
    http_request: Request,
):
    #
    # A2.7: publication authority is server-derived.  An anonymous or
    # unverified caller is refused BEFORE the adapter is constructed, so no
    # Superset network boundary can be reached on request-supplied capability.
    #
    authority = _publication_authority_for(http_request)

    if not authority.permits(
        "can_publish_bi_assets",
        require_authenticated=True,
    ):
        return JSONResponse(
            status_code=403,
            content={
                "status": "forbidden",
                "error": {
                    "category": "PublicationAuthorityRequired",
                    "message": (
                        "BI publication requires a server-authorised, "
                        "authenticated publication role."
                    ),
                },
            },
        )

    adapter = SupersetAdapter(settings)

    try:
        if request.publish:
            result = adapter.publish(
                request.dashboard,
                authority,
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
