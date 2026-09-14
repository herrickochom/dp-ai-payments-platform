from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field

from governance_models import IdentityContext


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    DENIED = "DENIED"
    REQUIRES_APPROVAL = "REQUIRES_APPROVAL"
    COMPLETED = "COMPLETED"
    SKIPPED = "SKIPPED"


class OrchestrationStatus(str, Enum):
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    DENIED = "DENIED"
    REQUIRES_APPROVAL = "REQUIRES_APPROVAL"
    COMPLETED = "COMPLETED"


class AgentTask(BaseModel):
    objective: str = Field(min_length=3, max_length=4000)
    identity: IdentityContext
    requested_agent: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)


class AgentEvidence(BaseModel):
    agent: str
    kind: str
    reference: str
    tool: str | None = None
    request: dict[str, Any] = Field(default_factory=dict)
    details: dict[str, Any] = Field(default_factory=dict)


class AgentFailure(BaseModel):
    category: str
    message: str
    retryable: bool = False


class AgentStep(BaseModel):
    step_id: str
    agent: str
    objective: str
    dependencies: list[str] = Field(default_factory=list)
    required_tools: list[str] = Field(default_factory=list)
    permission_requirements: list[str] = Field(default_factory=list)
    status: StepStatus = StepStatus.PENDING
    evidence: list[AgentEvidence] = Field(default_factory=list)
    failure: AgentFailure | None = None


class AgentPlan(BaseModel):
    plan_id: str
    steps: list[AgentStep]


class AgentResult(BaseModel):
    status: StepStatus = StepStatus.COMPLETED
    output: dict[str, Any] = Field(default_factory=dict)
    evidence: list[AgentEvidence] = Field(default_factory=list)
    tool_calls: int = Field(default=0, ge=0)
    failure: AgentFailure | None = None


class ExecutionContext(BaseModel):
    request_id: str
    identity: IdentityContext
    max_steps: int = Field(default=8, ge=1, le=20)
    max_tool_calls: int = Field(default=20, ge=1, le=100)
    max_depth: int = Field(default=6, ge=1, le=10)
    max_retries: int = Field(default=1, ge=0, le=3)
    max_elapsed_seconds: float = Field(default=30.0, gt=0, le=300)
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class OrchestrationResult(BaseModel):
    request_id: str
    plan: AgentPlan
    status: OrchestrationStatus
    outputs: dict[str, dict[str, Any]] = Field(default_factory=dict)
    evidence: list[AgentEvidence] = Field(default_factory=list)
    failures: list[AgentFailure] = Field(default_factory=list)
    completed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

