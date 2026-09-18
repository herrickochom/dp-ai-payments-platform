"""Allowlisted orchestration contracts for the DP-AI Payments Platform."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class JobRisk(str, Enum):
    READ_ONLY = "read_only"
    BOUNDED_WRITE = "bounded_write"
    ON_DEMAND_WRITE = "on_demand_write"


@dataclass(frozen=True)
class JobContract:
    name: str
    risk: JobRisk
    scheduled: bool
    description: str


JOBS = {
    "platform_preflight": JobContract(
        name="platform_preflight",
        risk=JobRisk.READ_ONLY,
        scheduled=True,
        description="Verify required platform services are available.",
    ),
    "raw_readiness": JobContract(
        name="raw_readiness",
        risk=JobRisk.READ_ONLY,
        scheduled=True,
        description="Verify expected Raw inputs are available.",
    ),
    "lakehouse_transform": JobContract(
        name="lakehouse_transform",
        risk=JobRisk.BOUNDED_WRITE,
        scheduled=True,
        description="Run the approved lakehouse transformation contract.",
    ),
    "data_quality": JobContract(
        name="data_quality",
        risk=JobRisk.READ_ONLY,
        scheduled=True,
        description="Run approved data-quality validation.",
    ),
    "ml_feature_generation": JobContract(
        name="ml_feature_generation",
        risk=JobRisk.BOUNDED_WRITE,
        scheduled=True,
        description="Generate approved default-risk model features.",
    ),
    "ml_default_risk_scoring": JobContract(
        name="ml_default_risk_scoring",
        risk=JobRisk.BOUNDED_WRITE,
        scheduled=True,
        description="Score using the frozen approved model.",
    ),
    "reconciliation": JobContract(
        name="reconciliation",
        risk=JobRisk.READ_ONLY,
        scheduled=True,
        description="Validate payment/lifecycle reconciliation.",
    ),
    "acceptance": JobContract(
        name="acceptance",
        risk=JobRisk.READ_ONLY,
        scheduled=True,
        description="Run bounded pipeline acceptance checks.",
    ),
    "generate_payment_source_data": JobContract(
        name="generate_payment_source_data",
        risk=JobRisk.ON_DEMAND_WRITE,
        scheduled=False,
        description="Generate synthetic payment source data on explicit request.",
    ),
}


FORBIDDEN_JOBS = frozenset(
    {
        "live_dlq_replay",
        "kafka_offset_reset",
        "destructive_lifecycle_execution",
        "recovery_restore_execution",
        "token_link_rematerialisation",
        "ml_model_training",
    }
)


def get_job(name: str) -> JobContract:
    if name in FORBIDDEN_JOBS:
        raise PermissionError(f"Job is forbidden from Airflow: {name}")

    try:
        return JOBS[name]
    except KeyError as exc:
        raise KeyError(f"Unknown platform job: {name}") from exc


def require_scheduled(name: str) -> JobContract:
    job = get_job(name)
    if not job.scheduled:
        raise PermissionError(
            f"Job requires explicit on-demand invocation: {name}"
        )
    return job


def require_on_demand(name: str) -> JobContract:
    job = get_job(name)
    if job.scheduled:
        raise PermissionError(
            f"Job is not an on-demand operation: {name}"
        )
    return job
