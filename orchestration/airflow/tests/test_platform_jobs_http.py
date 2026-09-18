from __future__ import annotations

import io
import json
from urllib.error import HTTPError

import pytest

import platform_jobs


class FakeResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps(
            self.payload
        ).encode("utf-8")


def test_scheduled_job_posts_to_runner(
    monkeypatch,
):
    seen = {}

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        seen["timeout"] = timeout
        seen["payload"] = json.loads(
            request.data.decode("utf-8")
        )

        return FakeResponse({
            "job_name": "platform_preflight",
            "status": "DRY_RUN",
            "request_id": (
                "11111111-1111-4111-8111-"
                "111111111111"
            ),
            "mutating": False,
        })

    monkeypatch.setenv(
        "PLATFORM_JOB_RUNNER_URL",
        "http://runner:8090",
    )
    monkeypatch.setattr(
        platform_jobs.urllib.request,
        "urlopen",
        fake_urlopen,
    )

    result = platform_jobs.run_scheduled_job(
        "platform_preflight",
        request_id=(
            "11111111-1111-4111-8111-"
            "111111111111"
        ),
    )

    assert (
        seen["url"]
        == "http://runner:8090/v1/jobs/scheduled"
    )
    assert seen["timeout"] == 10
    assert seen["payload"]["job_name"] == (
        "platform_preflight"
    )
    assert result["status"] == "DRY_RUN"


def test_generator_posts_only_bounded_fields(
    monkeypatch,
):
    seen = {}

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        seen["payload"] = json.loads(
            request.data.decode("utf-8")
        )

        return FakeResponse({
            "job_name": (
                "generate_payment_source_data"
            ),
            "status": "DRY_RUN",
            "request_id": (
                "22222222-2222-4222-8222-"
                "222222222222"
            ),
            "record_count": 7,
            "mutating": True,
        })

    monkeypatch.setattr(
        platform_jobs.urllib.request,
        "urlopen",
        fake_urlopen,
    )

    result = platform_jobs.generate_source_data(
        record_count=7,
        request_id=(
            "22222222-2222-4222-8222-"
            "222222222222"
        ),
    )

    assert seen["url"].endswith(
        "/v1/jobs/generate-source-data"
    )
    assert set(seen["payload"]) == {
        "request_id",
        "requested_by",
        "record_count",
    }
    assert "argv" not in seen["payload"]
    assert "command" not in seen["payload"]
    assert result["record_count"] == 7


def test_http_rejection_fails_closed(
    monkeypatch,
):
    error = HTTPError(
        url="http://runner/v1/jobs/scheduled",
        code=403,
        msg="Forbidden",
        hdrs=None,
        fp=io.BytesIO(
            b'{"detail":"forbidden"}'
        ),
    )

    def fake_urlopen(request, timeout):
        raise error

    monkeypatch.setattr(
        platform_jobs.urllib.request,
        "urlopen",
        fake_urlopen,
    )

    with pytest.raises(
        RuntimeError,
        match="http_status=403",
    ):
        platform_jobs.run_scheduled_job(
            "ml_model_training",
            request_id=(
                "33333333-3333-4333-8333-"
                "333333333333"
            ),
        )


def test_request_id_mismatch_fails_closed(
    monkeypatch,
):
    def fake_urlopen(request, timeout):
        return FakeResponse({
            "job_name": "platform_preflight",
            "status": "DRY_RUN",
            "request_id": (
                "aaaaaaaa-aaaa-4aaa-8aaa-"
                "aaaaaaaaaaaa"
            ),
            "mutating": False,
        })

    monkeypatch.setattr(
        platform_jobs.urllib.request,
        "urlopen",
        fake_urlopen,
    )

    with pytest.raises(
        RuntimeError,
        match="request_id mismatch",
    ):
        platform_jobs.run_scheduled_job(
            "platform_preflight",
            request_id=(
                "bbbbbbbb-bbbb-4bbb-8bbb-"
                "bbbbbbbbbbbb"
            ),
        )


def test_empty_runner_url_fails_closed(
    monkeypatch,
):
    monkeypatch.setenv(
        "PLATFORM_JOB_RUNNER_URL",
        "   ",
    )

    with pytest.raises(
        RuntimeError,
        match="must not be empty",
    ):
        platform_jobs.run_scheduled_job(
            "platform_preflight",
            request_id=(
                "44444444-4444-4444-8444-"
                "444444444444"
            ),
        )
