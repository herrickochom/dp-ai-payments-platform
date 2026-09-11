import hashlib
import re
from typing import Any, TYPE_CHECKING

from models import Evidence, Permissions
from quality_models import DQFinding, DQProfileRequest, DQRule
from tools import PermissionDenied, ToolError, quote_identifier

if TYPE_CHECKING:
    from tools import ToolRegistry


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _qualified(dataset: str) -> tuple[str, str, str, str]:
    parts = dataset.split(".")
    if len(parts) != 3 or any(not IDENTIFIER.fullmatch(part) for part in parts):
        raise ToolError("dataset must be a three-part catalog.schema.table identifier")
    return parts[0], parts[1], parts[2], ".".join(quote_identifier(part) for part in parts)


def _field(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ToolError(f"invalid field identifier: {value}")
    return quote_identifier(value)


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


class DataQualityEngine:
    def __init__(self, tools: "ToolRegistry"):
        self.tools = tools

    def run_rule(self, rule: DQRule, permissions: Permissions) -> DQFinding:
        if not permissions.can_run_data_quality_checks:
            raise PermissionDenied("data-quality permission is required")
        if not permissions.can_execute_read_queries:
            raise PermissionDenied("read-query permission is required")
        catalog, schema, table, dataset_sql = _qualified(rule.dataset)
        columns = {row["column_name"] for row in self.tools.describe_table(catalog, schema, table)}
        if rule.field and rule.field not in columns:
            raise ToolError(f"field {rule.field} is not present in {rule.dataset}")
        sql, sample_sql, extra_fields = self._compile(rule, dataset_sql, columns)
        result = self.tools.execute_query(sql, 1)
        if not result["rows"]:
            raise ToolError("DQ aggregate returned no result")
        row = dict(zip(result["columns"], result["rows"][0]))
        checked = int(row.get("checked_rows") or 0)
        failed = int(row.get("failed_rows") or 0)
        sample: list[dict[str, Any]] = []
        if (failed and sample_sql and permissions.can_view_data_quality_samples):
            limit = min(self.tools.settings.dq_max_sample_rows, permissions.max_rows, 5)
            sampled = self.tools.execute_query(sample_sql, limit)
            sample = [self._masked(dict(zip(sampled["columns"], values)))
                      for values in sampled["rows"][:limit]]
        evidence = [Evidence(kind="dq_rule", reference=rule.rule_id, details={
            "source": rule.source, "provenance": rule.provenance,
            "dataset": rule.dataset, "field": rule.field,
        }), Evidence(kind="query", reference=result.get("query_id") or "trino-query", details={
            "dataset": rule.dataset, "aggregate_only": True,
        })]
        warnings = []
        if failed and not permissions.can_view_data_quality_samples:
            warnings.append("Raw failure samples were withheld by default permission policy")
        return DQFinding(
            rule_id=rule.rule_id, dataset=rule.dataset,
            fields=[value for value in [rule.field, *extra_fields] if value],
            rule_type=rule.rule_type, rule_source=rule.source, severity=rule.severity,
            status="failed" if failed else "passed", checked_rows=checked,
            failed_rows=failed, failure_rate=(failed / checked if checked else 0.0),
            evidence=evidence, query_id=result.get("query_id"), sample=sample,
            recommendation=self._recommendation(rule, failed),
            confidence=0.7 if rule.source == "inferred" else 1.0, warnings=warnings,
        )

    def profile(self, request: DQProfileRequest) -> dict[str, Any]:
        permissions = request.permissions
        if not permissions.can_run_data_quality_checks:
            raise PermissionDenied("data-quality permission is required")
        if not permissions.can_execute_read_queries:
            raise PermissionDenied("read-query permission is required")
        catalog, schema, table, dataset_sql = _qualified(request.dataset)
        columns = {row["column_name"] for row in self.tools.describe_table(catalog, schema, table)}
        if missing := [field for field in request.fields if field not in columns]:
            raise ToolError(f"profile fields are not present: {', '.join(missing)}")
        expressions = ["COUNT(*) AS row_count"]
        for index, name in enumerate(request.fields):
            field = _field(name)
            expressions.extend([
                f"COUNT_IF({field} IS NULL) AS null_count_{index}",
                f"APPROX_DISTINCT({field}) AS distinct_count_{index}",
                f"MIN({field}) AS min_value_{index}",
                f"MAX({field}) AS max_value_{index}",
            ])
        result = self.tools.execute_query(f"SELECT {', '.join(expressions)} FROM {dataset_sql}", 1)
        row = dict(zip(result["columns"], result["rows"][0]))
        count = int(row["row_count"] or 0)
        profiles = []
        for index, name in enumerate(request.fields):
            nulls = int(row[f"null_count_{index}"] or 0)
            profiles.append({"field": name, "null_count": nulls,
                "null_rate": nulls / count if count else 0.0,
                "approximate_distinct_count": int(row[f"distinct_count_{index}"] or 0),
                "min": row[f"min_value_{index}"], "max": row[f"max_value_{index}"]})
        return {"dataset": request.dataset, "row_count": count, "fields": profiles,
                "query_id": result.get("query_id"), "aggregate_only": True,
                "evidence": [Evidence(kind="query", reference=result.get("query_id") or "trino-query",
                                      details={"dataset": request.dataset}).model_dump(mode="json")]}

    def existing_results_summary(self, permissions: Permissions,
                                 dataset: str = "iceberg.silver.slv_pdm_dq_results") -> dict[str, Any]:
        """Summarise repository-owned financial/reconciliation checks without reimplementing them."""
        if not permissions.can_run_data_quality_checks:
            raise PermissionDenied("data-quality permission is required")
        if not permissions.can_execute_read_queries:
            raise PermissionDenied("read-query permission is required")
        catalog, schema, table, dataset_sql = _qualified(dataset)
        columns = {row["column_name"] for row in self.tools.describe_table(catalog, schema, table)}
        required = {"entity_type", "rule_code", "rule_passed"}
        if not required.issubset(columns):
            raise ToolError(f"{dataset} is not the repository DQ results contract")
        result = self.tools.execute_query(
            f"SELECT entity_type, rule_code, COUNT(*) AS checked_rows, "
            f"COUNT_IF(NOT rule_passed) AS failed_rows FROM {dataset_sql} "
            "GROUP BY entity_type, rule_code ORDER BY failed_rows DESC, entity_type, rule_code", 100)
        rows = [dict(zip(result["columns"], row)) for row in result["rows"]]
        return {"dataset": dataset, "checks": rows, "query_id": result.get("query_id"),
                "source": "existing_dbt_model", "aggregate_only": True,
                "evidence": [Evidence(kind="dbt_model", reference="slv_pdm_dq_results",
                    details={"query_id": result.get("query_id"), "row_count": len(rows)}).model_dump(mode="json")]}

    def _compile(self, rule: DQRule, dataset_sql: str,
                 columns: set[str]) -> tuple[str, str | None, list[str]]:
        field = _field(rule.field) if rule.field else None
        sample_sql: str | None = None
        extra_fields: list[str] = []
        if rule.rule_type == "not_null":
            failure = f"{field} IS NULL"
            checked = "COUNT(*)"
        elif rule.rule_type == "unique":
            return (f"SELECT COUNT({field}) AS checked_rows, "
                    f"COUNT({field}) - COUNT(DISTINCT {field}) AS failed_rows FROM {dataset_sql}",
                    f"SELECT {field}, COUNT(*) AS duplicate_count FROM {dataset_sql} "
                    f"WHERE {field} IS NOT NULL GROUP BY {field} HAVING COUNT(*) > 1 "
                    "ORDER BY duplicate_count DESC", [])
        elif rule.rule_type == "accepted_values":
            values = rule.parameters.get("values", [])
            if not isinstance(values, list) or not values:
                raise ToolError("accepted_values requires a non-empty values list")
            failure = f"{field} IS NOT NULL AND {field} NOT IN ({', '.join(_literal(v) for v in values)})"
            checked = f"COUNT({field})"
        elif rule.rule_type == "range":
            clauses = []
            if "min" in rule.parameters:
                clauses.append(f"{field} < {_literal(rule.parameters['min'])}")
            if "max" in rule.parameters:
                clauses.append(f"{field} > {_literal(rule.parameters['max'])}")
            if not clauses:
                raise ToolError("range requires min and/or max")
            failure = f"{field} IS NOT NULL AND ({' OR '.join(clauses)})"
            checked = f"COUNT({field})"
        elif rule.rule_type == "referential_integrity":
            reference = rule.parameters.get("reference_dataset")
            reference_field = rule.parameters.get("reference_field")
            if not isinstance(reference, str) or not isinstance(reference_field, str):
                raise ToolError("referential_integrity requires reference_dataset and reference_field")
            rc, rs, rt, reference_sql = _qualified(reference)
            reference_columns = {row["column_name"] for row in self.tools.describe_table(rc, rs, rt)}
            if reference_field not in reference_columns:
                raise ToolError(f"reference field {reference_field} is not present in {reference}")
            rf = _field(reference_field)
            extra_fields = [reference_field]
            sql = (f"SELECT COUNT(l.{field}) AS checked_rows, "
                   f"COUNT_IF(l.{field} IS NOT NULL AND r.{rf} IS NULL) AS failed_rows "
                   f"FROM {dataset_sql} l LEFT JOIN {reference_sql} r ON l.{field} = r.{rf}")
            return sql, None, extra_fields
        elif rule.rule_type == "freshness":
            days = rule.parameters.get("max_age_days")
            if not isinstance(days, (int, float)) or days < 0:
                raise ToolError("freshness requires non-negative max_age_days")
            sql = (f"SELECT COUNT({field}) AS checked_rows, CASE WHEN MAX({field}) IS NULL OR "
                   f"DATE_DIFF('day', CAST(MAX({field}) AS timestamp), CURRENT_TIMESTAMP) > {days} "
                   f"THEN 1 ELSE 0 END AS failed_rows, MAX({field}) AS latest_value FROM {dataset_sql}")
            return sql, None, []
        elif rule.rule_type == "volume":
            clauses = []
            if "min_rows" in rule.parameters:
                clauses.append(f"COUNT(*) < {_literal(rule.parameters['min_rows'])}")
            if "max_rows" in rule.parameters:
                clauses.append(f"COUNT(*) > {_literal(rule.parameters['max_rows'])}")
            if not clauses:
                raise ToolError("volume requires min_rows and/or max_rows")
            return (f"SELECT COUNT(*) AS checked_rows, CASE WHEN {' OR '.join(clauses)} "
                    f"THEN 1 ELSE 0 END AS failed_rows FROM {dataset_sql}", None, [])
        else:
            raise ToolError("custom_read_only_check is reserved for repository-owned checks")
        sql = (f"SELECT {checked} AS checked_rows, COUNT_IF({failure}) AS failed_rows "
               f"FROM {dataset_sql}")
        if field:
            sample_sql = f"SELECT {field} AS failed_value FROM {dataset_sql} WHERE {failure}"
        return sql, sample_sql, extra_fields

    @staticmethod
    def _masked(row: dict[str, Any]) -> dict[str, Any]:
        masked = {}
        for key, value in row.items():
            if value is None or key.endswith("count"):
                masked[key] = value
            else:
                digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]
                masked[key] = f"masked:{digest}"
        return masked

    @staticmethod
    def _recommendation(rule: DQRule, failed: int) -> str:
        if not failed:
            return "No remediation is indicated; continue monitoring this rule."
        actions = {
            "not_null": "Investigate upstream records missing the required field.",
            "unique": "Investigate duplicate ingestion or natural-key construction.",
            "accepted_values": "Review source mapping and reference categories.",
            "range": "Review out-of-range source values and metric semantics.",
            "referential_integrity": "Investigate missing dimension/reference records upstream.",
            "freshness": "Investigate source delivery and pipeline completion.",
            "volume": "Investigate source completeness and ingestion volume.",
        }
        return actions.get(rule.rule_type, "Investigate the evidence before taking corrective action.")
