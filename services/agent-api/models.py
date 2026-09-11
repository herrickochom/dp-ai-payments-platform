from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class Permissions(BaseModel):
    can_discover_metadata: bool = True
    can_execute_read_queries: bool = True
    max_rows: int = Field(default=1000, ge=1)


class AgentRequest(BaseModel):
    objective: str = Field(min_length=3, max_length=4000)
    agent: Literal["data_discovery", "analytics"] | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    permissions: Permissions = Field(default_factory=Permissions)


class Evidence(BaseModel):
    kind: str
    reference: str
    details: dict[str, Any] = Field(default_factory=dict)


class ToolCall(BaseModel):
    tool: str
    status: Literal["completed", "failed"]
    duration_ms: float
    metadata: dict[str, Any] = Field(default_factory=dict)


class StructuredError(BaseModel):
    category: str
    message: str
    tool: str | None = None


class AgentResponse(BaseModel):
    request_id: str = Field(default_factory=lambda: str(uuid4()))
    agent: str
    objective: str
    context: dict[str, Any] = Field(default_factory=dict)
    permissions: Permissions
    tool_calls: list[ToolCall] = Field(default_factory=list)
    result: dict[str, Any] = Field(default_factory=dict)
    evidence: list[Evidence] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    status: Literal["completed", "failed", "unsupported"] = "completed"
    error: StructuredError | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None


class Measure(BaseModel):
    name: str
    expression: str
    aggregation: str


class OrderBy(BaseModel):
    field: str
    direction: Literal["asc", "desc"] = "asc"


class AnalyticalRequest(BaseModel):
    datasets: list[str]
    dimensions: list[str]
    measures: list[Measure]
    filters: list[dict[str, Any]] = Field(default_factory=list)
    group_by: list[str] = Field(default_factory=list)
    order_by: list[OrderBy] = Field(default_factory=list)
    limit: int = Field(default=20, ge=1)

