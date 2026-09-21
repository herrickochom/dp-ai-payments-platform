"""Authenticated, fail-closed transform admission and status API."""
import hashlib
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from fastapi import Depends, FastAPI, HTTPException
from .auth import require_airflow_identity, runner_headers
from .contract import ContractError, load_contract
from .execution_plan import EXECUTION_BATCHES
from .ledger import LedgerError, create_ledger_from_environment, model_fingerprint
from .models import CreateRunRequest, SubmitBatchRequest, TransformResponse

CONTRACT_PATH = Path(os.getenv("LAKEHOUSE_TRANSFORM_CONTRACT", "/app/contracts/lakehouse_transform.json"))
EXECUTION_ENABLED = os.getenv("LAKEHOUSE_TRANSFORM_EXECUTION_ENABLED", "false").lower() == "true"
ledger = create_ledger_from_environment()
app = FastAPI(title="PDM Lakehouse Transform Runtime", version="2.0.0")

def plan_digest() -> str:
    value = {key: {"authority": batch.authority, "models": sorted(batch.model_allowlist), "prerequisites": sorted(batch.prerequisite_batches)} for key, batch in sorted(EXECUTION_BATCHES.items())}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

def response(row: dict) -> TransformResponse:
    return TransformResponse(transform_run_id=row["transform_run_id"], batch_execution_id=row.get("batch_execution_id"), batch_id=row.get("batch_id"), status=row["status"], accepted_at=row["accepted_at"], started_at=row.get("started_at"), finished_at=row.get("finished_at"), attempt=row.get("attempt"), test_status=row.get("test_status"), failure_class=row.get("failure_class"))



@app.get("/health")
def health() -> dict:
    return {"status": "healthy", "execution_enabled": EXECUTION_ENABLED}

@app.get("/ready")
def ready() -> dict:
    try:
        payload, digest = load_contract(CONTRACT_PATH)
        ledger.ping() if hasattr(ledger, "ping") else ledger.connection.execute("SELECT 1").fetchone()
        if ledger.schema_version() < 2:
            raise LedgerError("transform ledger schema is not current")
    except (OSError, ValueError, ContractError, LedgerError) as exc:
        raise HTTPException(status_code=503, detail="transform runtime not ready") from exc
    return {"status": "ready", "contract_digest": digest, "plan_digest": plan_digest(), "contract_version": payload["contract_version"], "execution_enabled": EXECUTION_ENABLED}

@app.get("/v1/contract")
def contract_metadata(_: str = Depends(require_airflow_identity)) -> dict:
    try:
        payload, digest = load_contract(CONTRACT_PATH)
    except (OSError, ValueError, ContractError) as exc:
        raise HTTPException(status_code=503, detail="transform contract unavailable") from exc
    return {"name": payload["contract_name"], "version": payload["contract_version"], "mode": payload["mode"], "sha256": digest, "model_count": len(payload["models"]), "wave_count": len(payload["topological_waves"])}

@app.post("/v1/runs", response_model=TransformResponse, status_code=202)
def create_run(request: CreateRunRequest, caller: str = Depends(require_airflow_identity)) -> TransformResponse:
    try:
        payload, digest = load_contract(CONTRACT_PATH)
        if request.contract_version != payload["contract_version"]:
            raise ContractError("contract version mismatch")
        return response(ledger.create_run(caller, request.idempotency_key, request.execution_mode, digest, plan_digest()))
    except (OSError, ValueError, ContractError, LedgerError) as exc:
        raise HTTPException(status_code=409, detail="transform run rejected") from exc

@app.post("/v1/runs/{run_id}/batches", response_model=TransformResponse, status_code=202)
def submit_batch(run_id: str, request: SubmitBatchRequest, caller: str = Depends(require_airflow_identity)) -> TransformResponse:
    batch = EXECUTION_BATCHES.get(request.batch_id)
    if batch is None:
        raise HTTPException(status_code=409, detail="transform batch rejected")
    try:
        row = ledger.submit_batch(run_id, caller, request.idempotency_key, batch, model_fingerprint(batch.model_allowlist))
    except LedgerError as exc:
        raise HTTPException(status_code=409, detail="transform batch rejected") from exc
    if not EXECUTION_ENABLED:
        return response(row)

    try:
        queued = ledger.transition(
            row["batch_execution_id"],
            "QUEUED",
        )
        return response(queued)
    except LedgerError as exc:
        raise HTTPException(
            status_code=503,
            detail="transform execution unavailable",
        ) from exc

@app.get("/v1/batches/{execution_id}", response_model=TransformResponse)
def batch_status(execution_id: str, caller: str = Depends(require_airflow_identity)) -> TransformResponse:
    try:
        return response(ledger.get_batch(execution_id, caller))
    except LedgerError as exc:
        raise HTTPException(status_code=404, detail="batch execution not found") from exc

@app.get("/v1/batches/{execution_id}/result", response_model=TransformResponse)
def batch_result(execution_id: str, caller: str = Depends(require_airflow_identity)) -> TransformResponse:
    return batch_status(execution_id, caller)

@app.post("/v1/batches/{execution_id}/cancel", response_model=TransformResponse)
def cancel_batch(execution_id: str, caller: str = Depends(require_airflow_identity)) -> TransformResponse:
    try:
        ledger.get_batch(execution_id, caller)

        if not EXECUTION_ENABLED:
            return response(
                ledger.transition(
                    execution_id,
                    "CANCELLED",
                    failure_class="OPERATOR_CANCELLED",
                )
            )

        from .durable_queue import DurableQueueError, PostgresDurableQueue
        return response(PostgresDurableQueue().request_cancel(execution_id))
    except (LedgerError, DurableQueueError) as exc:
        raise HTTPException(
            status_code=409,
            detail="cancellation rejected",
        ) from exc
