from __future__ import annotations

import sys
from pathlib import Path
from uuid import uuid4

import pytest


ROOT = Path(__file__).resolve().parents[3]
JOB_RUNNER = ROOT / "orchestration" / "job_runner"

sys.path.insert(0, str(JOB_RUNNER))

from contracts import FORBIDDEN_JOBS, get_job
from planner import plan
from request import JobRequest


def request(name: str, **kwargs) -> JobRequest:
    return JobRequest(
        job_name=name,
        request_id=str(uuid4()),
        requested_by="airflow-test",
        **kwargs,
    )


def test_known_scheduled_job_is_allowed():
    result = plan(request("platform_preflight"))
    assert result.job_name == "platform_preflight"
    assert result.mutating is False


@pytest.mark.parametrize("job_name", sorted(FORBIDDEN_JOBS))
def test_forbidden_jobs_fail_closed(job_name):
    with pytest.raises(PermissionError):
        get_job(job_name)


def test_unknown_job_fails_closed():
    with pytest.raises(KeyError):
        get_job("arbitrary_shell_command")


def test_generator_cannot_be_scheduled():
    with pytest.raises(PermissionError):
        plan(request("generate_payment_source_data"))


def test_generator_requires_explicit_on_demand_flag():
    result = plan(
        request(
            "generate_payment_source_data",
            on_demand=True,
            record_count=250,
        )
    )
    assert result.mutating is True
    assert result.argv[-2:] == ("--count", "250")


@pytest.mark.parametrize("count", [0, -1, 10001])
def test_generator_rejects_unbounded_counts(count):
    with pytest.raises(ValueError):
        plan(
            request(
                "generate_payment_source_data",
                on_demand=True,
                record_count=count,
            )
        )


def test_generator_does_not_add_clean():
    result = plan(
        request(
            "generate_payment_source_data",
            on_demand=True,
            record_count=250,
        )
    )
    assert "--clean" not in result.argv


def test_no_command_contract_exposes_shell():
    for name in (
        "platform_preflight",
        "raw_readiness",
        "lakehouse_transform",
        "data_quality",
        "ml_feature_generation",
        "ml_default_risk_scoring",
        "reconciliation",
        "acceptance",
    ):
        result = plan(request(name))
        joined = " ".join(result.argv)
        assert "bash -c" not in joined
        assert "sh -c" not in joined
        assert ";" not in joined
        assert "&&" not in joined
        assert "||" not in joined
