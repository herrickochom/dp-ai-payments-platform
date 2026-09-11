import hashlib
import re
from typing import Any

from builder_models import (
    DashboardBuildRequest, DashboardSpec, DataSourceBuildRequest, DataSourceSpec,
    Encoding, ResolvedField, VisualizationBuildRequest, VisualizationSpec,
)
from models import Evidence
from tools import PermissionDenied, ToolError, ToolRegistry


class BuilderValidationError(ToolError):
    pass


NUMERIC_TYPES = ("tinyint", "smallint", "integer", "bigint", "real", "double", "decimal")
AGGREGATIONS = {"sum", "avg", "min", "max", "count", "count_distinct"}


def stable_id(prefix: str, value: str) -> str:
    return f"{prefix}_{hashlib.sha256(value.encode()).hexdigest()[:12]}"


class DataSourceBuilder:
    def build(self, request: DataSourceBuildRequest, tools: ToolRegistry) -> DataSourceSpec:
        if not request.permissions.can_discover_metadata:
            raise PermissionDenied("metadata discovery permission is required to build a data source")
        if not request.permissions.can_execute_read_queries:
            raise PermissionDenied("read-query permission is required to expose a data source")
        if len(request.semantic_request.datasets) != 1:
            raise BuilderValidationError("Phase 2 data sources require exactly one primary dataset")
        dataset = request.semantic_request.datasets[0]
        parts = dataset.split(".")
        if len(parts) != 3 or not all(parts):
            raise BuilderValidationError("dataset must be catalog.schema.table")
        catalog, schema, table = parts
        columns = tools.describe_table(catalog, schema, table)
        if not columns:
            raise BuilderValidationError(f"dataset does not exist or has no visible columns: {dataset}")
        types = {column["column_name"]: column["data_type"] for column in columns}
        fields: list[ResolvedField] = []
        for dimension in request.semantic_request.dimensions:
            if dimension not in types:
                raise BuilderValidationError(f"dimension does not exist: {dimension}")
            fields.append(ResolvedField(name=dimension, data_type=types[dimension], role="dimension"))
        for measure in request.semantic_request.measures:
            if measure.aggregation.lower() not in AGGREGATIONS:
                raise BuilderValidationError(f"unsupported aggregation: {measure.aggregation}")
            # Phase 2 only accepts direct discovered fields; expression languages
            # remain outside the deterministic builder boundary.
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", measure.expression):
                raise BuilderValidationError("calculated measure expressions are not supported in Phase 2")
            if measure.expression not in types:
                raise BuilderValidationError(f"measure field does not exist: {measure.expression}")
            if measure.aggregation.lower() not in {"count", "count_distinct"} and not any(
                    token in types[measure.expression].lower() for token in NUMERIC_TYPES):
                raise BuilderValidationError(f"aggregation requires a numeric field: {measure.expression}")
            fields.append(ResolvedField(name=measure.expression, data_type=types[measure.expression],
                                        role="measure", aggregation=measure.aggregation.lower()))
        warnings = []
        for join in request.joins:
            if join.relationship == "inferred":
                warnings.append(f"Join {join.left_dataset} -> {join.right_dataset} is inferred")
            self._validate_join(join, dataset, types, tools)
        evidence = [Evidence(kind="builder_source", reference=dataset,
            details={"fields": [field.name for field in fields],
                     "semantic_request_id": request.semantic_request_id,
                     "query_id": request.query_id})]
        return DataSourceSpec(id=stable_id("ds", dataset), dataset=dataset, fields=fields,
            joins=request.joins, semantic_request_id=request.semantic_request_id,
            query_id=request.query_id, evidence=evidence, warnings=warnings)

    @staticmethod
    def _validate_join(join, primary, primary_types, tools):
        if join.left_dataset != primary:
            raise BuilderValidationError("join left_dataset must be the primary dataset")
        if join.left_field not in primary_types:
            raise BuilderValidationError(f"join field does not exist: {join.left_field}")
        parts = join.right_dataset.split(".")
        if len(parts) != 3:
            raise BuilderValidationError("joined dataset must be catalog.schema.table")
        columns = tools.describe_table(*parts)
        if join.right_field not in {column["column_name"] for column in columns}:
            raise BuilderValidationError(f"joined field does not exist: {join.right_field}")


class VisualizationBuilder:
    def build(self, request: VisualizationBuildRequest) -> VisualizationSpec:
        if not request.permissions.can_execute_read_queries:
            raise PermissionDenied("read-query permission is required to build a visualization")
        if not request.permissions.can_build_visualizations:
            raise PermissionDenied("visualization build permission is required")
        fields = {field.name: field for field in request.data_source.fields}
        for encoding in request.encoding:
            field = fields.get(encoding.field)
            if not field:
                raise BuilderValidationError(f"visualization field does not exist: {encoding.field}")
            if field.role != encoding.role:
                raise BuilderValidationError(f"field role mismatch for {encoding.field}")
            if encoding.aggregation and encoding.aggregation.lower() not in AGGREGATIONS:
                raise BuilderValidationError(f"unsupported aggregation: {encoding.aggregation}")
        self._validate_shape(request)
        if request.sort and request.sort.field not in fields:
            raise BuilderValidationError(f"sort field does not exist: {request.sort.field}")
        evidence = list(request.data_source.evidence) + [Evidence(kind="visualization",
            reference=request.id, details={"type": request.type,
                                           "data_source_id": request.data_source.id})]
        return VisualizationSpec(**request.model_dump(), evidence=evidence)

    @staticmethod
    def _validate_shape(request):
        dimensions = [e for e in request.encoding if e.role == "dimension"]
        measures = [e for e in request.encoding if e.role == "measure"]
        if request.type == "kpi" and (dimensions or len(measures) != 1):
            raise BuilderValidationError("KPI requires exactly one measure and no dimensions")
        if request.type in {"bar", "line"} and (len(dimensions) != 1 or len(measures) < 1):
            raise BuilderValidationError(f"{request.type} requires one dimension and at least one measure")
        if request.type == "scatter" and len(measures) != 2:
            raise BuilderValidationError("scatter requires exactly two measures")
        if request.type == "table" and not request.encoding:
            raise BuilderValidationError("table requires at least one field")


class DashboardBuilder:
    def build(self, request: DashboardBuildRequest) -> DashboardSpec:
        if not request.permissions.can_execute_read_queries:
            raise PermissionDenied("read-query permission is required to build a dashboard")
        if not request.permissions.can_build_dashboards:
            raise PermissionDenied("dashboard build permission is required")
        identifiers = [visualization.id for visualization in request.visualizations]
        if len(identifiers) != len(set(identifiers)):
            raise BuilderValidationError("visualization IDs must be unique")
        known = set(identifiers)
        laid_out = [item.visualization_id for item in request.layout]
        missing = set(laid_out) - known
        if missing:
            raise BuilderValidationError(f"layout references missing visualizations: {sorted(missing)}")
        if set(identifiers) - set(laid_out):
            raise BuilderValidationError("every visualization must have a layout item")
        sources = {v.data_source.id: v.data_source for v in request.visualizations}
        for dashboard_filter in request.filters:
            source = sources.get(dashboard_filter.data_source_id)
            if not source:
                raise BuilderValidationError(f"filter data source does not exist: {dashboard_filter.data_source_id}")
            if dashboard_filter.field not in {field.name for field in source.fields}:
                raise BuilderValidationError(f"filter field does not exist: {dashboard_filter.field}")
        evidence = [item for visualization in request.visualizations for item in visualization.evidence]
        evidence.append(Evidence(kind="dashboard", reference=request.title,
            details={"visualizations": identifiers, "persistence": "returned_only"}))
        return DashboardSpec(**request.model_dump(), id=stable_id("dashboard", request.title),
                             evidence=evidence)


def recommend_visualizations(data_source: DataSourceSpec, title: str,
                             permissions=None) -> list[VisualizationBuildRequest]:
    from models import Permissions
    permissions = permissions or Permissions()
    dimensions = [field for field in data_source.fields if field.role == "dimension"]
    measures = [field for field in data_source.fields if field.role == "measure"]
    recommendations = []
    if measures:
        recommendations.append(VisualizationBuildRequest(id="repayment_rate_kpi", type="kpi",
            title="Average repayment performance", data_source=data_source,
            encoding=[Encoding(channel="value", field=measures[0].name, role="measure",
                               aggregation=measures[0].aggregation)], limit=1,
            permissions=permissions))
    if dimensions and measures:
        chart_type = "line" if any(token in dimensions[0].name.lower()
                                    for token in ("date", "month", "year", "time")) else "bar"
        recommendations.append(VisualizationBuildRequest(id="district_ranking", type=chart_type,
            title=title, data_source=data_source,
            encoding=[Encoding(channel="x", field=dimensions[0].name, role="dimension"),
                      Encoding(channel="y", field=measures[0].name, role="measure",
                               aggregation=measures[0].aggregation)], limit=20,
            permissions=permissions))
    recommendations.append(VisualizationBuildRequest(id="underlying_data", type="table",
        title="Underlying data", data_source=data_source,
        encoding=[Encoding(channel="column", field=field.name, role=field.role,
                           aggregation=field.aggregation) for field in data_source.fields], limit=100,
        permissions=permissions))
    return recommendations
