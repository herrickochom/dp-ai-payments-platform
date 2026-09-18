"""
Bounded control-plane API for DP-AI platform jobs.

This service deliberately exposes only repository-defined job names.
It does not accept arbitrary argv, shell commands, Docker commands,
replay operations, model training, lifecycle execution, recovery,
offset reset, or token-link materialisation.
"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from contracts import JOBS
from planner import plan
from platform_preflight import run_preflight
from raw_readiness import run_raw_readiness
from request import JobRequest


app = FastAPI(
    title="DP-AI Platform Job Runner",
    version="1.0.0",
)


class ScheduledJobRequest(BaseModel):
    job_name: str
    request_id: str = Field(min_length=1, max_length=128)
    requested_by: str = Field(
        default="airflow",
        min_length=1,
        max_length=128,
    )


class GeneratedSourceRequest(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    requested_by: str = Field(
        default="airflow-manual-trigger",
        min_length=1,
        max_length=128,
    )
    record_count: int = Field(ge=1, le=10000)


def execution_enabled() -> bool:
    return (
        os.getenv(
            "PLATFORM_RUNNER_EXECUTION_ENABLED",
            "false",
        ).strip().lower()
        == "true"
    )


@app.get("/health")
def health() -> dict:
    return {
        "status": "healthy",
        "execution_enabled": execution_enabled(),
    }


@app.get("/v1/contracts")
def contracts() -> dict:
    return {
        "scheduled": sorted(
            name
            for name, contract in JOBS.items()
            if contract.scheduled
        ),
        "on_demand": sorted(
            name
            for name, contract in JOBS.items()
            if not contract.scheduled
        ),
        "execution_enabled": execution_enabled(),
    }


@app.post("/v1/jobs/scheduled")
def scheduled_job(
    payload: ScheduledJobRequest,
) -> dict:
    request = JobRequest(
        job_name=payload.job_name,
        request_id=payload.request_id,
        requested_by=payload.requested_by,
        on_demand=False,
    )

    try:
        command = plan(request)
    except (
        PermissionError,
        ValueError,
        KeyError,
    ) as exc:
        raise HTTPException(
            status_code=403,
            detail=str(exc),
        ) from None

    if (
        command.job_name == "platform_preflight"
        and execution_enabled()
    ):
        result = run_preflight()

        if result["status"] != "SUCCESS":
            raise HTTPException(
                status_code=503,
                detail={
                    "message": (
                        "Platform preflight failed"
                    ),
                    "preflight": result,
                },
            )

        return {
            "job_name": command.job_name,
            "status": "SUCCESS",
            "request_id": request.request_id,
            "mutating": False,
            "preflight": result,
        }

    if command.job_name == "raw_readiness":
        result = run_raw_readiness()
        return {
            "job_name": command.job_name,
            "status": result["status"],
            "request_id": request.request_id,
            "mutating": False,
            "raw_readiness": result,
        }

    if execution_enabled():
        raise HTTPException(
            status_code=503,
            detail=(
                "Real execution is not enabled "
                "for this platform job"
            ),
        )

    return {
        "job_name": command.job_name,
        "status": "DRY_RUN",
        "request_id": request.request_id,
        "mutating": command.mutating,
    }


@app.post("/v1/jobs/generate-source-data")
def generate_source_data(
    payload: GeneratedSourceRequest,
) -> dict:
    request = JobRequest(
        job_name="generate_payment_source_data",
        request_id=payload.request_id,
        requested_by=payload.requested_by,
        on_demand=True,
        record_count=payload.record_count,
    )

    try:
        command = plan(request)
    except (
        PermissionError,
        ValueError,
        KeyError,
    ) as exc:
        raise HTTPException(
            status_code=403,
            detail=str(exc),
        ) from None

    if execution_enabled():
        raise HTTPException(
            status_code=503,
            detail=(
                "Real source generation is not wired yet; "
                "runner remains fail-closed"
            ),
        )

    return {
        "job_name": command.job_name,
        "status": "DRY_RUN",
        "request_id": request.request_id,
        "record_count": payload.record_count,
        "mutating": command.mutating,
    }
