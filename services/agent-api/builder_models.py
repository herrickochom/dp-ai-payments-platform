from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from models import AnalyticalRequest, Evidence, Permissions


class JoinSpec(BaseModel):
    left_dataset: str
    right_dataset: str
    left_field: str
    right_field: str
    relationship: Literal["confirmed", "inferred"] = "inferred"


class DataSourceBuildRequest(BaseModel):
    semantic_request: AnalyticalRequest
    joins: list[JoinSpec] = Field(default_factory=list)
    permissions: Permissions = Field(default_factory=Permissions)
    semantic_request_id: str | None = None
    query_id: str | None = None


class ResolvedField(BaseModel):
    name: str
    data_type: str
    role: Literal["dimension", "measure"]
    aggregation: str | None = None


class DataSourceSpec(BaseModel):
    id: str
    dataset: str
    fields: list[ResolvedField]
    joins: list[JoinSpec] = Field(default_factory=list)
    read_only: bool = True
    semantic_request_id: str | None = None
    query_id: str | None = None
    evidence: list[Evidence] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    status: Literal["validated"] = "validated"


class Encoding(BaseModel):
    channel: Literal["x", "y", "value", "detail", "column"]
    field: str
    role: Literal["dimension", "measure"]
    aggregation: str | None = None


class SortSpec(BaseModel):
    field: str
    direction: Literal["asc", "desc"] = "asc"


class VisualizationBuildRequest(BaseModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    type: Literal["kpi", "table", "bar", "line", "scatter"]
    title: str = Field(min_length=1, max_length=200)
    data_source: DataSourceSpec
    encoding: list[Encoding]
    sort: SortSpec | None = None
    limit: int = Field(default=20, ge=1, le=1000)
    permissions: Permissions = Field(default_factory=Permissions)


class VisualizationSpec(VisualizationBuildRequest):
    renderer: str = "vendor_neutral"
    evidence: list[Evidence] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    status: Literal["validated"] = "validated"


class DashboardFilter(BaseModel):
    field: str
    data_source_id: str


class LayoutItem(BaseModel):
    visualization_id: str
    x: int = Field(ge=0, le=11)
    y: int = Field(ge=0)
    width: int = Field(ge=1, le=12)
    height: int = Field(ge=1, le=100)

    @model_validator(mode="after")
    def within_grid(self):
        if self.x + self.width > 12:
            raise ValueError("layout item exceeds the 12-column grid")
        return self


class DashboardBuildRequest(BaseModel):
    title: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=1000)
    visualizations: list[VisualizationSpec] = Field(min_length=1)
    filters: list[DashboardFilter] = Field(default_factory=list)
    layout: list[LayoutItem] = Field(min_length=1)
    permissions: Permissions = Field(default_factory=Permissions)


class DashboardSpec(DashboardBuildRequest):
    id: str
    persistence: Literal["returned_only"] = "returned_only"
    evidence: list[Evidence] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    status: Literal["validated"] = "validated"


class BuildWorkflowRequest(BaseModel):
    objective: str = Field(min_length=3, max_length=4000)
    permissions: Permissions = Field(default_factory=Permissions)


class BuildWorkflowResult(BaseModel):
    analytical_result: dict[str, Any]
    data_source: DataSourceSpec
    visualizations: list[VisualizationSpec]
    dashboard: DashboardSpec
