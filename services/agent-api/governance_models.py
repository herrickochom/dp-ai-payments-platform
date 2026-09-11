from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class DataClassification(str, Enum):
    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    RESTRICTED = "RESTRICTED"


class PolicyDecisionType(str, Enum):
    ALLOW = "ALLOW"
    DENY = "DENY"
    MASK = "MASK"
    REQUIRE_APPROVAL = "REQUIRE_APPROVAL"


class SubjectType(str, Enum):
    HUMAN = "human"
    SERVICE = "service"
    AGENT = "agent"
    SYSTEM = "system"


class IdentityContext(BaseModel):
    subject_id: str
    subject_type: SubjectType = SubjectType.HUMAN

    roles: list[str] = Field(default_factory=list)
    permissions: list[str] = Field(default_factory=list)

    organisation: str | None = None
    jurisdiction: str | None = None
    district: str | None = None
    parish: str | None = None

    purpose: str | None = None


class FieldClassification(BaseModel):
    dataset: str
    field: str

    classification: DataClassification
    semantic_type: str | None = None

    masking_policy: str | None = None

    source: Literal[
        "explicit",
        "contract",
        "schema",
        "repository",
        "inferred",
    ] = "explicit"

    confidence: float = Field(default=1.0, ge=0.0, le=1.0)


class ResourceContext(BaseModel):
    dataset: str

    fields: list[str] = Field(default_factory=list)

    classification: DataClassification = DataClassification.INTERNAL

    field_classifications: list[FieldClassification] = Field(
        default_factory=list
    )

    district: str | None = None
    parish: str | None = None


class ApprovalContext(BaseModel):
    approval_id: str | None = None

    status: Literal[
        "not_required",
        "required",
        "pending",
        "approved",
        "denied",
        "expired",
    ] = "not_required"

    approver: str | None = None
    reason: str | None = None
    expires_at: datetime | None = None


class GovernanceRequest(BaseModel):
    identity: IdentityContext

    action: str
    resource: ResourceContext

    agent: str | None = None
    tool: str | None = None

    approval: ApprovalContext | None = None


class PolicyDecision(BaseModel):
    decision_id: str

    subject: str
    action: str
    resource: str

    decision: PolicyDecisionType

    reasons: list[str] = Field(default_factory=list)
    matched_policies: list[str] = Field(default_factory=list)

    masked_fields: list[str] = Field(default_factory=list)
    required_permissions: list[str] = Field(default_factory=list)

    approval_required: bool = False

    evidence: dict[str, Any] = Field(default_factory=dict)

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


class GovernanceAuditEvent(BaseModel):
    audit_id: str

    subject: str
    agent: str | None = None
    tool: str | None = None

    action: str
    resource: str

    fields: list[str] = Field(default_factory=list)

    classification: DataClassification

    decision: PolicyDecisionType
    matched_policies: list[str] = Field(default_factory=list)
    masked_fields: list[str] = Field(default_factory=list)

    purpose: str | None = None
    query_id: str | None = None

    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
