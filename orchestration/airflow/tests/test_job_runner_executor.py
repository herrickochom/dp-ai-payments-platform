from __future__ import annotations

import json
import sys
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[3]
RUNNER = ROOT / "orchestration" / "job_runner"

sys.path.insert(0, str(RUNNER))

from executor import PlatformExecutor
from planner import plan
from request import JobRequest


def make_request(
    job_name: str,
    *,
    on_demand: bool = False,
    record_count: int | None = None,
) -> JobRequest:
    return JobRequest(
        job_name=job_name,
        request_id=str(uuid4()),
        requested_by="pytest",
        on_demand=on_demand,
        record_count=record_count,
    )


def test_executor_defaults_to_dry_run(
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv(
        "PLATFORM_JOB_EXECUTION_ENABLED",
        raising=False,
    )

    request = make_request("platform_preflight")
    command = plan(request)

    result = PlatformExecutor(
        audit_path=tmp_path / "audit.jsonl"
    ).execute(
        plan=command,
        request_id=request.request_id,
    )

    assert result.status == "DRY_RUN"
    assert result.return_code is None


def test_dry_run_writes_audit_record(tmp_path):
    request = make_request("platform_preflight")
    command = plan(request)
    path = tmp_path / "audit.jsonl"

    PlatformExecutor(
        enabled=False,
        audit_path=path,
    ).execute(
        plan=command,
        request_id=request.request_id,
    )

    rows = path.read_text().splitlines()

    assert len(rows) == 1

    payload = json.loads(rows[0])

    assert payload["job_name"] == "platform_preflight"
    assert payload["status"] == "DRY_RUN"
    assert payload["request_id"] == request.request_id


def test_generator_plan_has_no_clean():
    request = make_request(
        "generate_payment_source_data",
        on_demand=True,
        record_count=250,
    )

    command = plan(request)

    assert "--clean" not in command.argv


def test_executor_never_uses_shell(
    tmp_path,
    monkeypatch,
):
    request = make_request("platform_preflight")
    command = plan(request)

    captured = {}

    class Completed:
        returncode = 0

    def fake_run(argv, **kwargs):
        captured["argv"] = argv
        captured["kwargs"] = kwargs
        return Completed()

    monkeypatch.setattr(
        "executor.subprocess.run",
        fake_run,
    )

    PlatformExecutor(
        enabled=True,
        audit_path=tmp_path / "audit.jsonl",
    ).execute(
        plan=command,
        request_id=request.request_id,
    )

    assert captured["kwargs"]["shell"] is False
    assert isinstance(captured["argv"], list)
