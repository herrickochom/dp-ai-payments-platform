"""
Bounded execution layer for Airflow platform jobs.

Execution is disabled by default.
No arbitrary shell input is accepted.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from planner import CommandPlan


@dataclass(frozen=True)
class ExecutionResult:
    job_name: str
    status: str
    return_code: int | None
    argv: tuple[str, ...]
    started_at: str
    finished_at: str
    request_id: str


class PlatformExecutor:
    def __init__(
        self,
        *,
        enabled: bool | None = None,
        audit_path: str | Path | None = None,
    ) -> None:
        if enabled is None:
            enabled = (
                os.getenv(
                    "PLATFORM_JOB_EXECUTION_ENABLED",
                    "false",
                ).strip().lower()
                == "true"
            )

        self.enabled = enabled
        self.audit_path = Path(
            audit_path
            or os.getenv(
                "PLATFORM_JOB_AUDIT_PATH",
                "/var/lib/platform-job-runner/audit.jsonl",
            )
        )

    def execute(
        self,
        *,
        plan: CommandPlan,
        request_id: str,
        environment: Mapping[str, str] | None = None,
    ) -> ExecutionResult:
        started = datetime.now(timezone.utc).isoformat()

        if not self.enabled:
            result = ExecutionResult(
                job_name=plan.job_name,
                status="DRY_RUN",
                return_code=None,
                argv=plan.argv,
                started_at=started,
                finished_at=datetime.now(
                    timezone.utc
                ).isoformat(),
                request_id=request_id,
            )
            self._audit(result)
            return result

        completed = subprocess.run(
            list(plan.argv),
            check=False,
            shell=False,
            env=(
                dict(environment)
                if environment is not None
                else None
            ),
        )

        result = ExecutionResult(
            job_name=plan.job_name,
            status=(
                "SUCCESS"
                if completed.returncode == 0
                else "FAILED"
            ),
            return_code=completed.returncode,
            argv=plan.argv,
            started_at=started,
            finished_at=datetime.now(
                timezone.utc
            ).isoformat(),
            request_id=request_id,
        )

        self._audit(result)

        if completed.returncode != 0:
            raise RuntimeError(
                f"Platform job failed: {plan.job_name}; "
                f"rc={completed.returncode}"
            )

        return result

    def _audit(
        self,
        result: ExecutionResult,
    ) -> None:
        self.audit_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        with self.audit_path.open(
            "a",
            encoding="utf-8",
        ) as handle:
            handle.write(
                json.dumps(
                    asdict(result),
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )
            handle.write("\n")
