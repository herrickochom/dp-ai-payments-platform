from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


ExecutionMode = Literal[
    "snapshot",
    "incremental",
    "cdc_incremental",
]


class TransformRequest(BaseModel):
    """
    A caller selects a version-controlled execution unit.

    Arbitrary dbt selectors, model names, commands and shell arguments
    are intentionally absent from this API.
    """

    model_config = ConfigDict(extra="forbid")

    contract_name: Literal["lakehouse_transform"]
    contract_version: int = Field(ge=1)
    execution_unit: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^[a-z0-9_]+$",
    )
    execution_mode: ExecutionMode = "snapshot"


class TransformResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    accepted: bool
    contract_name: str
    contract_version: int
    execution_unit: str
    execution_mode: ExecutionMode
    execution_enabled: bool
    detail: str
