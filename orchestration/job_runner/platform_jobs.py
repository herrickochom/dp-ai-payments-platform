"""Airflow adapter for the bounded DP-AI platform job runner."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from uuid import uuid4


DEFAULT_RUNNER_URL = "http://platform-job-runner:8090"


def _runner_url() -> str:
    value = os.getenv(
        "PLATFORM_JOB_RUNNER_URL",
        DEFAULT_RUNNER_URL,
    ).strip()

    if not value:
        raise RuntimeError(
            "PLATFORM_JOB_RUNNER_URL must not be empty"
        )

    return value.rstrip("/")


def _post_json(
    *,
    path: str,
    payload: dict,
) -> dict:
    body = json.dumps(
        payload,
        separators=(",", ":"),
    ).encode("utf-8")

    request = urllib.request.Request(
        _runner_url() + path,
        data=body,
        headers={
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=10,
        ) as response:
            result = json.load(response)

    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(
            "utf-8",
            errors="replace",
        )
        raise RuntimeError(
            "Platform job runner rejected request: "
            f"http_status={exc.code}; "
            f"detail={detail}"
        ) from None

    except urllib.error.URLError as exc:
        raise RuntimeError(
            "Platform job runner unavailable: "
            f"{exc.reason}"
        ) from None

    if not isinstance(result, dict):
        raise RuntimeError(
            "Platform job runner returned invalid response"
        )

    return result


def run_scheduled_job(
    job_name: str,
    *,
    request_id: str | None = None,
    requested_by: str = "airflow",
) -> dict:
    resolved_request_id = (
        request_id or str(uuid4())
    )

    result = _post_json(
        path="/v1/jobs/scheduled",
        payload={
            "job_name": job_name,
            "request_id": resolved_request_id,
            "requested_by": requested_by,
        },
    )

    if result.get("request_id") != resolved_request_id:
        raise RuntimeError(
            "Platform job runner request_id mismatch"
        )

    if result.get("job_name") != job_name:
        raise RuntimeError(
            "Platform job runner job_name mismatch"
        )

    return result


def generate_source_data(
    *,
    record_count: int,
    request_id: str | None = None,
    requested_by: str = "airflow",
) -> dict:
    resolved_request_id = (
        request_id or str(uuid4())
    )

    result = _post_json(
        path="/v1/jobs/generate-source-data",
        payload={
            "request_id": resolved_request_id,
            "requested_by": requested_by,
            "record_count": record_count,
        },
    )

    if (
        result.get("request_id")
        != resolved_request_id
    ):
        raise RuntimeError(
            "Platform job runner request_id mismatch"
        )

    if (
        result.get("job_name")
        != "generate_payment_source_data"
    ):
        raise RuntimeError(
            "Platform job runner job_name mismatch"
        )

    if result.get("record_count") != record_count:
        raise RuntimeError(
            "Platform job runner record_count mismatch"
        )

    return result
