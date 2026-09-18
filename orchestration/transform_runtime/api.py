import os
from pathlib import Path

from fastapi import FastAPI, HTTPException

from .contract import ContractError, load_contract
from .execution_policy import (
    ExecutionPolicyError,
    validate_execution_unit,
)
from .models import TransformRequest, TransformResponse


CONTRACT_PATH = Path(
    os.getenv(
        "LAKEHOUSE_TRANSFORM_CONTRACT",
        "/app/contracts/lakehouse_transform.json",
    )
)

EXECUTION_ENABLED = (
    os.getenv(
        "LAKEHOUSE_TRANSFORM_EXECUTION_ENABLED",
        "false",
    ).lower()
    == "true"
)

app = FastAPI(
    title="PDM Lakehouse Transform Runtime",
    version="1.0.0",
)


@app.get("/health")
def health() -> dict:
    return {
        "status": "healthy",
        "execution_enabled": EXECUTION_ENABLED,
    }


@app.get("/v1/contract")
def contract_metadata() -> dict:
    try:
        payload, digest = load_contract(CONTRACT_PATH)
    except (
        OSError,
        ValueError,
        ContractError,
    ) as exc:
        raise HTTPException(
            status_code=503,
            detail="transform contract unavailable",
        ) from exc

    return {
        "name": payload["name"],
        "version": payload["contract_version"],
        "mode": payload["mode"],
        "sha256": digest,
        "model_count": len(payload["models"]),
        "wave_count": len(payload["topological_waves"]),
    }


@app.post(
    "/v1/transform",
    response_model=TransformResponse,
)
def transform(
    request: TransformRequest,
) -> TransformResponse:

    try:
        payload, _ = load_contract(CONTRACT_PATH)

        if (
            request.contract_version
            != payload["contract_version"]
        ):
            raise ContractError(
                "contract version mismatch"
            )

        validate_execution_unit(
            request.execution_unit,
            request.execution_mode,
        )

    except (
        OSError,
        ValueError,
        ContractError,
        ExecutionPolicyError,
    ) as exc:
        raise HTTPException(
            status_code=409,
            detail="transform request rejected",
        ) from exc

    if not EXECUTION_ENABLED:
        return TransformResponse(
            accepted=False,
            contract_name=request.contract_name,
            contract_version=request.contract_version,
            execution_unit=request.execution_unit,
            execution_mode=request.execution_mode,
            execution_enabled=False,
            detail=(
                "execution disabled; "
                "production runtime is fail-closed"
            ),
        )

    # Deliberately no transformation executor implementation yet.
    raise HTTPException(
        status_code=503,
        detail="transform executor not implemented",
    )
