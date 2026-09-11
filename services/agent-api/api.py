import logging
import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from config import Settings
from models import AgentRequest
from orchestrator import Orchestrator

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

