import logging
from datetime import datetime, timezone

from agents import AnalyticsAgent, DataDiscoveryAgent
from config import Settings
from models import AgentRequest, AgentResponse, StructuredError
from tools import ToolError, ToolRegistry
from trino_gateway import TrinoGateway

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, settings: Settings | None = None, gateway=None):
        self.settings = settings or Settings()
        self.gateway = gateway or TrinoGateway(self.settings)
        self.discovery = DataDiscoveryAgent()
        self.analytics = AnalyticsAgent(self.discovery, self.settings)

    def route(self, request: AgentRequest) -> str:
        if request.agent:
            return request.agent
        objective = request.objective.lower()
        analytical = ("performance", "worst", "best", "average", "total", "rate", "trend", " by ")
        discovery = ("what table", "which dataset", "where is", "what data", "columns", "schema")
        if any(token in objective for token in discovery):
            return "data_discovery"
        if any(token in objective for token in analytical):
            return "analytics"
        raise ValueError("Objective is outside the supported Phase 1 discovery/analytics scope")

    def execute(self, request: AgentRequest) -> AgentResponse:
        try:
            identity = self.route(request)
        except ValueError as exc:
            return AgentResponse(agent="orchestrator", objective=request.objective,
                context=request.context, permissions=request.permissions, status="unsupported",
                error=StructuredError(category="unsupported_objective", message=str(exc)),
                completed_at=datetime.now(timezone.utc))
        response = AgentResponse(agent=identity, objective=request.objective,
            context=request.context, permissions=request.permissions)
        tools = ToolRegistry(self.gateway, self.settings)
        try:
            agent = self.discovery if identity == "data_discovery" else self.analytics
            response.result, response.evidence, response.warnings = agent.run(
                request.objective, request.permissions, tools)
        except (ToolError, Exception) as exc:
            response.status = "failed"
            response.error = StructuredError(category=type(exc).__name__, message=str(exc),
                tool=tools.calls[-1].tool if tools.calls else None)
            logger.exception("agent_request_failed", extra={"request_id": response.request_id,
                "agent": identity, "error_category": type(exc).__name__})
        response.tool_calls = tools.calls
        response.completed_at = datetime.now(timezone.utc)
        logger.info("agent_request_completed", extra={"request_id": response.request_id,
            "agent": identity, "status": response.status, "tool_calls": len(tools.calls)})
        return response

