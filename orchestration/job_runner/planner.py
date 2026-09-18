"""
Deterministic platform job planner.

This module returns an allowlisted command specification.
It does not execute subprocesses.
"""

from __future__ import annotations

from dataclasses import dataclass

from contracts import (
    require_on_demand,
    require_scheduled,
)
from request import JobRequest


@dataclass(frozen=True)
class CommandPlan:
    job_name: str
    argv: tuple[str, ...]
    mutating: bool


SCHEDULED_COMMANDS = {
    "platform_preflight": (
        "platform-job",
        "preflight",
    ),
    "raw_readiness": (
        "platform-job",
        "raw-readiness",
    ),
    "lakehouse_transform": (
        "platform-job",
        "lakehouse-transform",
    ),
    "data_quality": (
        "platform-job",
        "data-quality",
    ),
    "ml_feature_generation": (
        "platform-job",
        "ml-feature-generation",
    ),
    "ml_default_risk_scoring": (
        "platform-job",
        "ml-default-risk-scoring",
    ),
    "reconciliation": (
        "platform-job",
        "reconciliation",
    ),
    "acceptance": (
        "platform-job",
        "acceptance",
    ),
}


def plan(request: JobRequest) -> CommandPlan:
    request.validate()

    if request.on_demand:
        require_on_demand(request.job_name)

        if request.job_name != "generate_payment_source_data":
            raise PermissionError("Unsupported on-demand operation")

        if request.record_count is None:
            raise ValueError(
                "record_count is required for source generation"
            )

        return CommandPlan(
            job_name=request.job_name,
            argv=(
                "platform-job",
                "generate-payment-source-data",
                "--count",
                str(request.record_count),
            ),
            mutating=True,
        )

    require_scheduled(request.job_name)

    try:
        argv = SCHEDULED_COMMANDS[request.job_name]
    except KeyError as exc:
        raise PermissionError(
            f"No scheduled command contract: {request.job_name}"
        ) from exc

    return CommandPlan(
        job_name=request.job_name,
        argv=argv,
        mutating=request.job_name in {
            "lakehouse_transform",
            "ml_feature_generation",
            "ml_default_risk_scoring",
        },
    )
