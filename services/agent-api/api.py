import logging
import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from config import Settings
from builder_models import (BuildWorkflowRequest, DashboardBuildRequest,
                            DataSourceBuildRequest, VisualizationBuildRequest)
from builders import recommend_visualizations
from bi_adapter import DashboardAgentRequest, PublicationRequest, SupersetAdapter
from models import AgentRequest
from orchestrator import Orchestrator
from tools import ToolRegistry

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
settings = Settings()
orchestrator = Orchestrator(settings)
app = FastAPI(title="DP AI Agent API", version="1.0.0")


@app.get("/agents")
def agents():
    return {"agents": [
        {"id": "data_discovery", "capabilities": ["metadata search", "schema inspection", "join inference"]},
        {"id": "analytics", "capabilities": ["semantic analytical request", "bounded read-only query"]},
        {"id": "visualization", "capabilities": ["bounded visualization recommendation"]},
        {"id": "dashboard", "capabilities": ["bounded dashboard composition"]},
    ]}


@app.get("/agents/health")
def health():
    try:
        platform = orchestrator.gateway.health()
        return {"status": "healthy", "trino": platform, "model_provider": settings.model_provider,
                "rag": "enabled" if settings.rag_enabled else "not_configured"}
    except Exception as exc:
        return JSONResponse(status_code=503, content={"status": "unhealthy", "trino": {
            "status": "unavailable", "error_category": type(exc).__name__}})


@app.post("/agents/query")
def query(request: AgentRequest):
    response = orchestrator.execute(request)
    code = 200 if response.status == "completed" else 422 if response.status == "unsupported" else 503
    return JSONResponse(status_code=code, content=response.model_dump(mode="json"))


def _builder_response(operation):
    tools = ToolRegistry(orchestrator.gateway, settings)
    try:
        artifact = operation(tools)
        return {"status": "completed", "artifact": artifact.model_dump(mode="json"),
                "tool_calls": [call.model_dump(mode="json") for call in tools.calls]}
    except Exception as exc:
        return JSONResponse(status_code=422, content={"status": "failed", "error": {
            "category": type(exc).__name__, "message": str(exc)},
            "tool_calls": [call.model_dump(mode="json") for call in tools.calls]})


@app.post("/builders/data-source")
def build_data_source(request: DataSourceBuildRequest):
    return _builder_response(lambda tools: tools.build_data_source(request))


@app.post("/builders/visualization")
def build_visualization(request: VisualizationBuildRequest):
    return _builder_response(lambda tools: tools.build_visualization(request))


@app.post("/builders/dashboard")
def build_dashboard(request: DashboardBuildRequest):
    return _builder_response(lambda tools: tools.build_dashboard(request))


@app.post("/agents/build")
def build_workflow(request: BuildWorkflowRequest):
    analytical = orchestrator.execute(AgentRequest(agent="analytics", objective=request.objective,
                                                   permissions=request.permissions))
    if analytical.status != "completed" or not analytical.result.get("data_source"):
        return JSONResponse(status_code=422, content=analytical.model_dump(mode="json"))
    from builder_models import DataSourceSpec, LayoutItem
    data_source = DataSourceSpec.model_validate(analytical.result["data_source"])
    tools = ToolRegistry(orchestrator.gateway, settings)
    try:
        visualizations = [tools.build_visualization(item) for item in
                          recommend_visualizations(data_source, request.objective, request.permissions)]
        layout = [LayoutItem(visualization_id=item.id, x=0, y=index * 4,
                             width=12, height=4) for index, item in enumerate(visualizations)]
        dashboard = tools.build_dashboard(DashboardBuildRequest(
            title=request.objective, description="Deterministically composed Phase 2 definition",
            visualizations=visualizations, layout=layout, permissions=request.permissions))
        return {"status": "completed", "analytical_result": analytical.result,
                "data_source": data_source.model_dump(mode="json"),
                "visualizations": [item.model_dump(mode="json") for item in visualizations],
                "dashboard": dashboard.model_dump(mode="json"),
                "tool_calls": [call.model_dump(mode="json") for call in
                               analytical.tool_calls + tools.calls]}
    except Exception as exc:
        return JSONResponse(status_code=422, content={"status": "failed",
            "error": {"category": type(exc).__name__, "message": str(exc)}})


@app.post("/agents/visualize")
def visualize(request: BuildWorkflowRequest):
    response = orchestrator.execute(AgentRequest(agent="visualization", objective=request.objective,
                                                 permissions=request.permissions))
    return JSONResponse(status_code=200 if response.status == "completed" else 422,
                        content=response.model_dump(mode="json"))


@app.post("/agents/dashboard")
def dashboard(request: DashboardAgentRequest):
    response = orchestrator.execute(AgentRequest(agent="dashboard", objective=request.objective,
                                                 permissions=request.permissions))
    if response.status != "completed" or not response.result.get("dashboard"):
        return JSONResponse(status_code=422, content=response.model_dump(mode="json"))
    from builder_models import DashboardSpec
    artifact = DashboardSpec.model_validate(response.result["dashboard"])
    adapter = SupersetAdapter(settings)
    try:
        publication = (adapter.publish(artifact, request.permissions) if request.publish
                       else adapter.plan(artifact))
    except Exception as exc:
        return JSONResponse(status_code=502, content={"status": "failed",
            "error": {"category": type(exc).__name__, "message": str(exc)},
            "dashboard": artifact.model_dump(mode="json")})
    return {"status": "completed", "agent_response": response.model_dump(mode="json"),
            "dashboard": artifact.model_dump(mode="json"), "superset": publication}


@app.post("/publish/superset")
def publish_superset(request: PublicationRequest):
    adapter = SupersetAdapter(settings)
    try:
        result = (adapter.publish(request.dashboard, request.permissions) if request.publish
                  else adapter.plan(request.dashboard))
        return {"status": "completed", "superset": result}
    except Exception as exc:
        return JSONResponse(status_code=502, content={"status": "failed",
            "error": {"category": type(exc).__name__, "message": str(exc)}})
