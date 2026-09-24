"""Metadata-only Airflow client for authenticated transform admission."""
from __future__ import annotations
import json
import os
import time
import urllib.request

from services.shared.security.secret_provider import require_secret

def _request(method: str, path: str, payload: dict | None = None) -> dict:
    token = require_secret("TRANSFORM_RUNTIME_AIRFLOW_TOKEN")
    body = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(os.getenv("TRANSFORM_RUNTIME_URL", "http://transform-runtime:8089").rstrip("/") + path, data=body, method=method, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=10) as response:
        value = json.load(response)
    if not isinstance(value, dict):
        raise RuntimeError("transform runtime returned invalid metadata")
    return value

def create_transform_run(idempotency_key: str) -> str:
    result = _request("POST", "/v1/runs", {"contract_name": "lakehouse_transform", "contract_version": 1, "execution_mode": "snapshot", "idempotency_key": idempotency_key})
    return result["transform_run_id"]

def submit_and_wait_batch(transform_run_id: str, batch_id: str, idempotency_key: str) -> dict:
    result = _request("POST", f"/v1/runs/{transform_run_id}/batches", {"batch_id": batch_id, "idempotency_key": idempotency_key})
    execution_id = result["batch_execution_id"]
    deadline = time.monotonic() + int(os.getenv("TRANSFORM_AIRFLOW_POLL_TIMEOUT_SECONDS", "3600"))
    while result["status"] not in {"SUCCEEDED", "FAILED", "CANCELLED", "ORPHANED"}:
        if time.monotonic() >= deadline:
            raise TimeoutError("transform batch status polling timed out")
        time.sleep(min(10, int(os.getenv("TRANSFORM_AIRFLOW_POLL_SECONDS", "5"))))
        result = _request("GET", f"/v1/batches/{execution_id}")
    if result["status"] != "SUCCEEDED" or result.get("test_status") != "PASSED":
        raise RuntimeError(f"transform batch failed: {batch_id}; class={result.get('failure_class')}")
    return {key: result.get(key) for key in ("transform_run_id", "batch_execution_id", "batch_id", "status", "attempt", "test_status", "failure_class")}
