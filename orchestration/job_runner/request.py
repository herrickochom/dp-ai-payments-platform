"""Validated job-runner requests."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class JobRequest:
    job_name: str
    request_id: str
    requested_by: str
    on_demand: bool = False
    record_count: int | None = None

    def validate(self) -> None:
        UUID(self.request_id)

        if not self.requested_by.strip():
            raise ValueError("requested_by is required")

        if self.record_count is not None:
            if self.record_count < 1:
                raise ValueError("record_count must be >= 1")
            if self.record_count > 10_000:
                raise ValueError("record_count exceeds bounded maximum")
