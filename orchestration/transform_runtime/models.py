from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


ExecutionMode = Literal[
    "snapshot",
    "incremental",
    "cdc_incremental",
]


class CreateRunRequest(BaseModel):
    """
    A caller selects a version-controlled execution unit.

    Arbitrary dbt selectors, model names, commands and shell arguments
    are intentionally absent from this API.
    """

    model_config = ConfigDict(extra="forbid")

    contract_name: Literal["lakehouse_transform"]
    contract_version: int = Field(ge=1)
    execution_mode: ExecutionMode = "snapshot"
    idempotency_key: str = Field(min_length=16, max_length=128)


class SubmitBatchRequest(BaseModel):
    """Only a version-controlled batch identity crosses the trust boundary."""

    model_config = ConfigDict(extra="forbid")

    batch_id: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^C4_[A-Z0-9_]+$",
    )
    idempotency_key: str = Field(min_length=16, max_length=128)


class TransformResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    accepted: bool
    transform_run_id: str
    batch_execution_id: str | None = None
    batch_id: str | None = None
    status: str
    accepted_at: str
    started_at: str | None = None
    finished_at: str | None = None
    attempt: int | None = None
    test_status: str | None = None
    failure_class: str | None = None
