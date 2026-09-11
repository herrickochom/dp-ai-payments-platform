from datetime import date, datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from models import Evidence, Permissions


RuleType = Literal["not_null", "unique", "accepted_values", "range",
                   "referential_integrity", "freshness", "volume",
                   "custom_read_only_check"]
RuleSource = Literal["dbt_test", "phase4_config", "inferred", "platform_health",
                     "existing_dbt_model"]


class DQRule(BaseModel):
    rule_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]+$")
    dataset: str
    rule_type: RuleType
    field: str | None = None
    severity: Literal["low", "medium", "high", "critical"] = "medium"
    parameters: dict[str, Any] = Field(default_factory=dict)
    source: RuleSource
    provenance: str | None = None

    @model_validator(mode="after")
    def validate_shape(self):
        if self.rule_type not in {"volume", "custom_read_only_check"} and not self.field:
            raise ValueError(f"{self.rule_type} requires a field")
        if self.rule_type == "custom_read_only_check" and "sql" in self.parameters:
            raise ValueError("arbitrary SQL is not accepted in DQ rules")
        return self


class DQFinding(BaseModel):
    finding_id: str = Field(default_factory=lambda: f"dq_{uuid4().hex}")
    rule_id: str
    dataset: str
    fields: list[str] = Field(default_factory=list)
    rule_type: RuleType
    rule_source: RuleSource
    severity: Literal["low", "medium", "high", "critical"]
    status: Literal["passed", "failed", "error", "unsupported"]
    checked_rows: int = 0
    failed_rows: int = 0
    failure_rate: float = 0.0
    evidence: list[Evidence] = Field(default_factory=list)
    query_id: str | None = None
    sample: list[dict[str, Any]] = Field(default_factory=list)
    observed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    recommendation: str
    confidence: float = Field(default=1.0, ge=0, le=1)
    warnings: list[str] = Field(default_factory=list)


class DQCheckRequest(BaseModel):
    rule: DQRule
    permissions: Permissions = Field(default_factory=Permissions)


class DQProfileRequest(BaseModel):
    dataset: str
    fields: list[str] = Field(min_length=1, max_length=5)
    permissions: Permissions = Field(default_factory=Permissions)


class TimePeriod(BaseModel):
    start: date | datetime | str
    end: date | datetime | str


class ComparisonWindow(BaseModel):
    current_period: TimePeriod | None = None
    comparison_period: TimePeriod | None = None
    time_field: str | None = None


class InsightRequest(BaseModel):
    objective: str = Field(min_length=3, max_length=4000)
    dataset: str | None = None
    metric: str | None = None
    dimension: str | None = None
    comparison: ComparisonWindow | None = None
    threshold: float | None = Field(default=None, ge=0)
    permissions: Permissions = Field(default_factory=Permissions)


class InsightFinding(BaseModel):
    insight_id: str = Field(default_factory=lambda: f"insight_{uuid4().hex}")
    type: Literal["metric_change", "ranking", "ranking_movement", "threshold_breach",
                  "anomaly", "limitation", "platform_health", "data_quality"]
    metric: str | None = None
    dimension: str | None = None
    summary: str
    current_value: float | int | None = None
    previous_value: float | int | None = None
    absolute_change: float | None = None
    relative_change: float | None = None
    evidence: list[Evidence] = Field(default_factory=list)
    confidence: float = Field(default=1.0, ge=0, le=1)
    warnings: list[str] = Field(default_factory=list)
    classification: Literal["observed_change", "contribution", "correlation",
                            "hypothesis", "causal_evidence", "limitation"] = "observed_change"
    details: dict[str, Any] = Field(default_factory=dict)
