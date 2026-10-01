import logging
from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError

from agents import AnalyticsAgent, DataDiscoveryAgent
from authority import (
    ServerAuthorityResolver,
    narrow_permissions,
)
from config import Settings
from governance import GovernancePolicyEngine
from governance_models import GovernanceRequest
from models import AgentRequest, AgentResponse, StructuredError
from observability import brief, emit
from phase3_agents import DashboardAgent, VisualizationAgent
from phase4_agents import DataQualityAgent, InsightAgent
from tools import ToolError, ToolRegistry
from trino_gateway import TrinoGateway


logger = logging.getLogger(__name__)


#
# Agents that read governed platform data.  These require resolved governance
# authority; metadata-only discovery does not (A2.1).
#
GOVERNED_DATA_AGENTS = frozenset(
    {
        "analytics",
        "visualization",
        "dashboard",
        "data_quality",
        "insight",
    }
)


class Orchestrator:
    def __init__(
        self,
        settings: Settings | None = None,
        gateway=None,
        governance_engine: GovernancePolicyEngine | None = None,
        audit_store=None,
        authority_resolver=None,
        classification_resolver=None,
    ):
        self.settings = settings or Settings()
        self.gateway = gateway or TrinoGateway(self.settings)
        self.audit_store = audit_store

        self.governance_engine = (
            governance_engine
            if governance_engine is not None
            else GovernancePolicyEngine()
        )

        self.discovery = DataDiscoveryAgent()

        self.analytics = AnalyticsAgent(
            self.discovery,
            self.settings,
        )

        self.visualization = VisualizationAgent(
            self.analytics
        )

        self.dashboard = DashboardAgent(
            self.visualization
        )

        self.data_quality = DataQualityAgent(
            self.discovery,
            self.settings,
        )

        self.insight = InsightAgent(
            self.discovery,
            self.analytics,
            self.settings,
        )

        #
        # Single server-side authority resolver.  Caller-supplied permissions
        # are narrowed against this; they never widen it (A2.2/A2.3).
        #
        self.authority_resolver = authority_resolver or ServerAuthorityResolver(
            max_rows=self.settings.max_rows,
        )

        #
        # Platform-owned trusted classification.  Injectable so a deployment
        # can supply its own metadata source; it is never derived from a
        # caller-supplied GovernanceRequest.
        #
        self.classification_resolver = classification_resolver

    def _authoritative_governance(
        self,
        governance_request: GovernanceRequest | None,
    ) -> GovernanceRequest | None:
        """Replace a claimed identity with the server-authorised one.

        A request may CLAIM roles, but only the intersection of the claim and
        the server-side role registry for that subject survives.  Raw
        ``identity.permissions`` are discarded outright, and an approval in the
        payload is only ever a reference id.

        With no configured role registry the authorised role set is empty, so a
        request that merely claims a privileged role gains nothing.
        """

        if governance_request is None:
            return None

        identity = getattr(governance_request, "identity", None)

        roles = self.authority_resolver.authorised_roles(identity)

        authoritative_identity = (
            identity.model_copy(
                update={
                    "roles": list(roles),
                    "permissions": [],
                },
            )
            if identity is not None
            else None
        )

        return governance_request.model_copy(
            update={
                "identity": authoritative_identity,
            }
        )

    def effective_permissions(
        self,
        request: AgentRequest,
        governance_request: GovernanceRequest | None = None,
    ):
        """Server-authorised capabilities for a request.

        The returned ``Permissions`` is the intersection of what the caller
        REQUESTED and what the server AUTHORISED, so

            effective_capability is a subset of server authority
        """

        identity = (
            getattr(governance_request, "identity", None)
            if governance_request is not None
            else None
        )

        authority = self.authority_resolver.resolve(identity)

        return narrow_permissions(
            request.permissions,
            authority,
            server_max_rows=self.settings.max_rows,
        )

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def route(
        self,
        request: AgentRequest,
    ) -> str:
        if request.agent:
            return request.agent

        objective = request.objective.lower()

        analytical = (
            "performance",
            "worst",
            "best",
            "average",
            "total",
            "rate",
            "trend",
            " by ",
        )

        if any(
            token in objective
            for token in (
                "what changed",
                "previous period",
                "deteriorat",
                "unusual",
                "anomaly",
                "difference across",
            )
        ):
            return "insight"

        if any(
            token in objective
            for token in (
                "data quality",
                "complete",
                "completeness",
                "missing",
                "duplicate",
                "unique",
                "freshness",
                "stale",
            )
        ):
            return "data_quality"

        if any(
            token in objective
            for token in (
                "dashboard",
                "scorecard",
            )
        ):
            return "dashboard"

        if any(
            token in objective
            for token in (
                "visualize",
                "visualise",
                "chart",
                "graph",
            )
        ):
            return "visualization"

        discovery = (
            "what table",
            "which dataset",
            "where is",
            "what data",
            "columns",
            "schema",
        )

        if any(
            token in objective
            for token in discovery
        ):
            return "data_discovery"

        if any(
            token in objective
            for token in analytical
        ):
            return "analytics"

        raise ValueError(
            "Objective is outside the supported bounded agent scope"
        )

    # ------------------------------------------------------------------
    # Phase 5 governance context
    # ------------------------------------------------------------------

    @staticmethod
    def _context_as_dict(
        context: Any,
    ) -> dict[str, Any]:
        """
        Convert AgentRequest.context into a dictionary without requiring the
        rest of the agent stack to know whether context is represented as a
        plain dictionary or a Pydantic model.
        """

        if context is None:
            return {}

        if isinstance(
            context,
            dict,
        ):
            return context

        if hasattr(
            context,
            "model_dump",
        ):
            value = context.model_dump()

            if isinstance(
                value,
                dict,
            ):
                return value

        return {}

    def _governance_request(
        self,
        request: AgentRequest,
    ) -> GovernanceRequest | None:
        """
        Resolve optional request-scoped governance information.

        Expected AgentRequest context shape:

        {
            "governance": {
                "identity": {
                    "subject_id": "analyst-1",
                    "roles": ["programme_analyst"],
                    "purpose": "programme_monitoring"
                },
                "action": "read",
                "resource": {
                    "dataset": "...",
                    "classification": "INTERNAL"
                }
            }
        }

        Requests without governance context remain backward compatible with
        Phases 1-4.

        IMPORTANT:
        Resource classification supplied here is only an interim Phase 5
        integration mechanism. Before Phase 5 is frozen, dataset and field
        classifications must be resolved from trusted platform metadata rather
        than accepted from an API caller.
        """

        context = self._context_as_dict(
            request.context
        )

        payload = context.get(
            "governance"
        )

        if payload is None:
            return None

        if isinstance(
            payload,
            GovernanceRequest,
        ):
            return payload

        if hasattr(
            payload,
            "model_dump",
        ):
            payload = payload.model_dump()

        if not isinstance(
            payload,
            dict,
        ):
            raise ValueError(
                "context.governance must be an object"
            )

        try:
            return GovernanceRequest.model_validate(
                payload
            )

        except ValidationError as exc:
            raise ValueError(
                "Invalid governance context: "
                f"{exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Agent execution
    # ------------------------------------------------------------------

    def execute(
        self,
        request: AgentRequest,
    ) -> AgentResponse:
        try:
            identity = self.route(
                request
            )

        except ValueError as exc:
            return AgentResponse(
                agent="orchestrator",
                objective=request.objective,
                context=request.context,
                permissions=request.permissions,
                status="unsupported",
                error=StructuredError(
                    category="unsupported_objective",
                    message=str(exc),
                ),
                completed_at=datetime.now(
                    timezone.utc
                ),
            )

        response = AgentResponse(
            agent=identity,
            objective=request.objective,
            context=request.context,
            permissions=request.permissions,
        )

        #
        # Resolve governance before creating ToolRegistry.
        #
        # A malformed governance context must fail closed instead of silently
        # removing policy enforcement.
        #
        try:
            governance_request = (
                self._governance_request(
                    request
                )
            )

        except Exception as exc:
            response.status = "failed"

            response.error = StructuredError(
                category="GovernanceContextError",
                message=str(exc),
            )

            response.completed_at = (
                datetime.now(
                    timezone.utc
                )
            )

            logger.exception(
                "governance_context_failed",
                extra={
                    "request_id": (
                        response.request_id
                    ),
                    "agent": identity,
                    "error_category": (
                        type(exc).__name__
                    ),
                },
            )

            return response

        #
        # A2.1/A2.3: replace the claimed identity with the server-authorised
        # one before any policy decision is made.
        #
        governance_request = self._authoritative_governance(
            governance_request
        )

        #
        # A2.1: governance is mandatory.  Agents that read governed data are
        # refused outright when no governance authority can be resolved, so
        # the request fails before any tool is constructed.  Metadata-only
        # discovery still runs, because it reads no governed platform data.
        #
        if (
            identity in GOVERNED_DATA_AGENTS
            and governance_request is None
        ):
            response.status = "failed"

            response.permissions = (
                self.effective_permissions(
                    request,
                    None,
                )
            )

            response.error = StructuredError(
                category="GovernanceContextRequired",
                message=(
                    "Governance context is required for "
                    f"'{identity}' requests and was not supplied."
                ),
            )

            response.completed_at = (
                datetime.now(
                    timezone.utc
                )
            )

            return response

        #
        # A2.2/A2.3: caller permissions are narrowed against server authority.
        # The response reports the EFFECTIVE permissions actually applied.
        #
        response.permissions = (
            self.effective_permissions(
                request,
                governance_request,
            )
        )

        tools = ToolRegistry(
            self.gateway,
            self.settings,
            governance_engine=(
                self.governance_engine
            ),
            governance_request=(
                governance_request
            ),
            audit_store=self.audit_store,
            request_id=response.request_id,
            classification_resolver=self.classification_resolver,
        )

        emit("orchestration_started", request_id=response.request_id,
             agent=identity, objective=brief(request.objective))

        try:
            agents = {
                "data_discovery": (
                    self.discovery
                ),
                "analytics": (
                    self.analytics
                ),
                "visualization": (
                    self.visualization
                ),
                "dashboard": (
                    self.dashboard
                ),
                "data_quality": (
                    self.data_quality
                ),
                "insight": (
                    self.insight
                ),
            }

            agent = agents[
                identity
            ]

            if identity in {
                "data_quality",
                "insight",
            }:
                (
                    response.result,
                    response.evidence,
                    response.warnings,
                ) = agent.run(
                    request.objective,
                    response.permissions,
                    tools,
                    request.context,
                )

            else:
                (
                    response.result,
                    response.evidence,
                    response.warnings,
                ) = agent.run(
                    request.objective,
                    response.permissions,
                    tools,
                )

        except Exception as exc:
            response.status = "failed"

            response.error = StructuredError(
                category=type(
                    exc
                ).__name__,
                message=str(exc),
                tool=(
                    tools.calls[-1].tool
                    if tools.calls
                    else None
                ),
            )

            logger.exception(
                "agent_request_failed",
                extra={
                    "request_id": (
                        response.request_id
                    ),
                    "agent": identity,
                    "error_category": (
                        type(exc).__name__
                    ),
                },
            )

        response.tool_calls = (
            tools.calls
        )

        response.completed_at = (
            datetime.now(
                timezone.utc
            )
        )

        logger.info(
            "agent_request_completed",
            extra={
                "request_id": (
                    response.request_id
                ),
                "agent": identity,
                "status": (
                    response.status
                ),
                "tool_calls": len(
                    tools.calls
                ),
            },
        )

        return response