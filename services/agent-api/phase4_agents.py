from __future__ import annotations

import math
from typing import Any

from agents import AnalyticsAgent, DataDiscoveryAgent
from config import Settings
from models import Evidence, Permissions
from quality_models import DQRule, InsightFinding
from tools import PermissionDenied, ToolError, ToolRegistry, quote_identifier


def _dataset_sql(dataset: str) -> str:
    parts = dataset.split(".")
    if len(parts) != 3 or any(not part.replace("_", "").isalnum() for part in parts):
        raise ToolError("dataset must be a three-part catalog.schema.table identifier")
    return ".".join(quote_identifier(part) for part in parts)


class DataQualityAgent:
    identity = "data_quality"

    def __init__(self, discovery: DataDiscoveryAgent, settings: Settings):
        self.discovery, self.settings = discovery, settings

    def run(self, objective: str, permissions: Permissions, tools: ToolRegistry,
            context: dict[str, Any] | None = None):
        if not permissions.can_run_data_quality_checks:
            raise PermissionDenied("data-quality permission is required")
        context = context or {}
        if context.get("existing_results") or any(token in objective.lower() for token in
                ("reconciliation", "financial consistency", "business validation")):
            summary = tools.dq_summary(permissions)
            evidence = [Evidence.model_validate(item) for item in summary["evidence"]]
            failures = sum(int(item["failed_rows"] or 0) for item in summary["checks"])
            return {"dataset": summary["dataset"], "trust_status": (
                    "issues_detected" if failures else "passed_evaluated_rules"),
                    "existing_results_summary": summary,
                    "explanation": (f"Observed {failures} failed rows in repository-owned "
                                    "financial, referential, and reconciliation checks.")}, evidence, []
        requested_dataset = context.get("dataset")
        rules = tools.dq_list_rules(requested_dataset)
        if requested_ids := context.get("rule_ids"):
            requested = set(requested_ids)
            rules = [rule for rule in rules if rule.rule_id in requested]
        elif requested_dataset:
            rules = self._rank_rules(rules, objective)
        else:
            rules = self._rank_rules(rules, objective)
            if rules:
                requested_dataset = rules[0].dataset
                rules = [rule for rule in rules if rule.dataset == requested_dataset]

        evidence: list[Evidence] = []
        warnings: list[str] = []
        if not rules:
            return {"dataset": requested_dataset, "findings": [], "trust_status": "unknown",
                    "explanation": "No explicit dbt/configured rule supports this request; no rule was invented."}, evidence, [
                        "Supply a dataset with an existing dbt test or an explicit typed Phase 4 rule"]

        limit = min(self.settings.dq_max_rules_per_request, len(rules))
        findings = [tools.dq_run_rule(rule, permissions) for rule in rules[:limit]]
        for finding in findings:
            evidence.extend(finding.evidence)
        if len(rules) > limit:
            warnings.append(f"Evaluation was bounded to {limit} of {len(rules)} matching rules")
        failed = sum(finding.status == "failed" for finding in findings)
        trust = "issues_detected" if failed else "passed_evaluated_rules"
        return {"dataset": requested_dataset, "trust_status": trust,
                "evaluated_rule_count": len(findings), "failed_rule_count": failed,
                "findings": [finding.model_dump(mode="json") for finding in findings],
                "explanation": (f"Observed {failed} failed rule(s) across {len(findings)} evaluated "
                                "explicit rules. This is bounded evidence, not a guarantee about unevaluated rules.")}, evidence, warnings

    @staticmethod
    def _rank_rules(rules: list[DQRule], objective: str) -> list[DQRule]:
        terms = {token.strip("?.,").lower() for token in objective.split() if len(token) > 2}
        preferred_types = set()
        if terms & {"complete", "completeness", "missing", "null"}:
            preferred_types.add("not_null")
        if terms & {"duplicate", "duplicates", "unique", "uniqueness"}:
            preferred_types.add("unique")
        if terms & {"category", "categories", "valid", "values"}:
            preferred_types.add("accepted_values")
        def score(rule: DQRule):
            haystack = f"{rule.dataset} {rule.field or ''}".lower().replace("_", " ")
            return (5 * (rule.rule_type in preferred_types)
                    + sum(2 for term in terms if term in haystack)
                    + 2 * ("silver" in rule.dataset)
                    + (rule.source == "dbt_test"))
        return sorted(rules, key=lambda rule: (-score(rule), rule.rule_id))


class InsightAgent:
    identity = "insight"
    TIME_FIELDS = ("reporting_month", "snapshot_date", "reporting_date", "payment_date", "as_of_date")
    METRICS = ("principal_repayment_rate", "cohort_principal_repayment_rate",
               "avg_principal_repayment_rate", "repayment_rate", "loan_count", "payment_count")
    DIMENSIONS = ("district", "region", "parish", "source_system", "status")

    def __init__(self, discovery: DataDiscoveryAgent, analytics: AnalyticsAgent, settings: Settings):
        self.discovery, self.analytics, self.settings = discovery, analytics, settings

    def run(self, objective: str, permissions: Permissions, tools: ToolRegistry,
            context: dict[str, Any] | None = None):
        if not permissions.can_generate_insights:
            raise PermissionDenied("insight generation permission is required")
        if not permissions.can_execute_read_queries:
            raise PermissionDenied("read-query permission is required")
        context = context or {}
        temporal = any(word in objective.lower() for word in
                       ("changed", "change", "previous", "trend", "movement", "deteriorat"))
        if not temporal and not context.get("comparison"):
            return self._ranking(objective, permissions, tools, context)

        candidate, metadata_evidence, metadata_warnings = self._candidate(
            objective, tools, permissions, context)
        if not candidate:
            limitation = InsightFinding(type="limitation", classification="limitation",
                summary="Available metadata does not expose a dataset with both a supported metric and time field.",
                confidence=1.0, warnings=["No reporting periods were invented"])
            return {"insights": [limitation.model_dump(mode="json")], "comparison": None}, metadata_evidence, metadata_warnings
        dataset, columns = candidate
        names = {column["column_name"] for column in columns}
        metric = context.get("metric") or next((name for name in self.METRICS if name in names), None)
        time_field = ((context.get("comparison") or {}).get("time_field")
                      or context.get("time_field") or next((name for name in self.TIME_FIELDS if name in names), None))
        dimension = context.get("dimension") or next((name for name in self.DIMENSIONS if name in names), None)
        if not metric or not time_field:
            limitation = InsightFinding(type="limitation", metric=metric,
                classification="limitation", summary=(
                    f"{dataset} cannot support a temporal comparison because an actual metric/time field pair was not found."),
                confidence=1.0, warnings=["No reporting periods were invented"])
            return {"dataset": dataset, "insights": [limitation.model_dump(mode="json")],
                    "comparison": None}, metadata_evidence, metadata_warnings
        result = self._comparison_query(dataset, metric, time_field, dimension, permissions, tools)
        insights, comparison, warnings = self._compare(result, dataset, metric, time_field,
                                                        dimension, context.get("threshold"))
        evidence = metadata_evidence + [Evidence(kind="query",
            reference=result.get("query_id") or "trino-query", details={
                "dataset": dataset, "metric": metric, "time_field": time_field,
                "observed_periods_only": True})]
        for insight in insights:
            insight.evidence.extend(evidence[-1:])
        if "cohort" in metric:
            warning = ("The selected dbt model labels this as approval-cohort current state, "
                       "not reconstructed month-end portfolio history")
            warnings.append(warning)
            for insight in insights:
                insight.warnings.append(warning)
        return {"dataset": dataset, "comparison": comparison,
                "insights": [item.model_dump(mode="json") for item in insights],
                "method": "deterministic observed-period comparison"}, evidence, metadata_warnings + warnings

    def _ranking(self, objective, permissions, tools, context):
        if not context.get("dataset"):
            analytical, evidence, warnings = self.analytics.run(objective, permissions, tools)
            data = analytical.get("data")
            if not data:
                limitation = InsightFinding(type="limitation", classification="limitation",
                    summary="Available metadata cannot support the requested ranking.")
                return {"insights": [limitation.model_dump(mode="json")]}, evidence, warnings
            rows = [dict(zip(data["columns"], row)) for row in data["rows"]]
            metric, dimension = "repayment_performance", data["columns"][0]
            dataset = analytical["analytical_request"]["datasets"][0]
            query_sql = analytical["query"]
        else:
            dataset = context["dataset"]
            catalog, schema, table = dataset.split(".")
            columns = tools.describe_table(catalog, schema, table)
            names = {column["column_name"] for column in columns}
            metric = context.get("metric") or next((name for name in self.METRICS if name in names), None)
            dimension = context.get("dimension") or next((name for name in self.DIMENSIONS if name in names), None)
            if not metric or not dimension:
                limitation = InsightFinding(type="limitation", classification="limitation",
                    summary=f"{dataset} lacks a supported metric/dimension pair.")
                return {"dataset": dataset, "insights": [limitation.model_dump(mode="json")]}, [], []
            sql = (f"SELECT {quote_identifier(dimension)}, AVG({quote_identifier(metric)}) AS value "
                   f"FROM {_dataset_sql(dataset)} WHERE {quote_identifier(dimension)} IS NOT NULL "
                   f"GROUP BY {quote_identifier(dimension)} ORDER BY value ASC LIMIT "
                   f"{min(20, permissions.max_rows, self.settings.max_rows)}")
            query = tools.execute_query(sql, min(20, permissions.max_rows))
            rows = [dict(zip(query["columns"], row)) for row in query["rows"]]
            evidence = [Evidence(kind="query", reference=query.get("query_id") or "trino-query",
                                 details={"dataset": dataset, "ranking": "ascending"})]
            warnings, query_sql = [], query["sql"]
        values = [float(row[data_key]) for row in rows for data_key in
                  (["repayment_performance"] if "repayment_performance" in row else ["value"])
                  if row[data_key] is not None]
        mean = sum(values) / len(values) if values else 0.0
        deviation = math.sqrt(sum((value - mean) ** 2 for value in values) / len(values)) if values else 0.0
        ranked = []
        value_key = "repayment_performance" if rows and "repayment_performance" in rows[0] else "value"
        for rank, row in enumerate(rows, 1):
            value = float(row[value_key]) if row[value_key] is not None else None
            ranked.append({"rank": rank, "entity": row[dimension], "value": value,
                           "z_score": ((value - mean) / deviation if value is not None and deviation else 0.0),
                           "is_unusual": bool(value is not None and deviation and abs((value - mean) / deviation) >= 2.0)})
        summary = (f"Observed {len(ranked)} {dimension} values ordered from lowest to highest {metric}. "
                   "This ranking describes association only; it does not establish causes.")
        insight = InsightFinding(type="ranking", metric=metric, dimension=dimension,
            summary=summary, evidence=evidence[-1:] if evidence else [], details={
                "ranking": ranked, "anomaly_method": "population z-score >= 2.0",
                "query": query_sql})
        return {"dataset": dataset, "insights": [insight.model_dump(mode="json")],
                "method": "ascending ranking with explainable z-score flag"}, evidence, warnings

    def _candidate(self, objective, tools, permissions, context):
        if dataset := context.get("dataset"):
            parts = dataset.split(".")
            if len(parts) != 3:
                raise ToolError("dataset must be a three-part identifier")
            columns = tools.describe_table(*parts)
            return (dataset, columns), [Evidence(kind="metadata", reference=dataset)], []
        discovery, evidence, warnings = self.discovery.run(objective, permissions, tools)
        def score(item):
            names = {column["column_name"] for column in item["columns"]}
            return (5 * bool(names & set(self.TIME_FIELDS)) + 5 * bool(names & set(self.METRICS))
                    + 2 * ("consumption" == item["schema"])
                    + 2 * ("principal_repayment_rate" in names))
        candidates = sorted(discovery["datasets"], key=score, reverse=True)
        if not candidates or score(candidates[0]) < 10:
            return None, evidence, warnings
        item = candidates[0]
        return (f"{item['catalog']}.{item['schema']}.{item['table']}", item["columns"]), evidence, warnings

    def _comparison_query(self, dataset, metric, time_field, dimension, permissions, tools):
        dimension_select = f", {quote_identifier(dimension)}" if dimension else ""
        dimension_group = f", {quote_identifier(dimension)}" if dimension else ""
        sql = (f"WITH recent_periods AS (SELECT DISTINCT {quote_identifier(time_field)} AS period "
               f"FROM {_dataset_sql(dataset)} WHERE {quote_identifier(time_field)} IS NOT NULL "
               f"ORDER BY period DESC LIMIT 2) SELECT {quote_identifier(time_field)} AS period"
               f"{dimension_select}, AVG({quote_identifier(metric)}) AS value FROM {_dataset_sql(dataset)} "
               f"WHERE {quote_identifier(time_field)} IN (SELECT period FROM recent_periods) "
               f"GROUP BY {quote_identifier(time_field)}{dimension_group} "
               f"ORDER BY period DESC, value DESC LIMIT {min(200, permissions.max_rows, self.settings.max_rows)}")
        return tools.execute_query(sql, min(200, permissions.max_rows))

    def _compare(self, result, dataset, metric, time_field, dimension, threshold):
        rows = [dict(zip(result["columns"], row)) for row in result["rows"]]
        periods = []
        for row in rows:
            if row["period"] not in periods:
                periods.append(row["period"])
        if len(periods) < 2:
            finding = InsightFinding(type="limitation", metric=metric, dimension=dimension,
                classification="limitation", summary=(
                    f"Only {len(periods)} observed {time_field} period(s) exist in {dataset}; two are required."),
                warnings=["No comparison period was invented"])
            return [finding], None, []
        current_period, previous_period = periods[:2]
        current_rows = [row for row in rows if row["period"] == current_period]
        previous_rows = [row for row in rows if row["period"] == previous_period]
        current = sum(float(row["value"]) for row in current_rows if row["value"] is not None) / max(1, sum(row["value"] is not None for row in current_rows))
        previous = sum(float(row["value"]) for row in previous_rows if row["value"] is not None) / max(1, sum(row["value"] is not None for row in previous_rows))
        absolute = current - previous
        relative = absolute / abs(previous) if previous else None
        direction = "increased" if absolute > 0 else "decreased" if absolute < 0 else "was unchanged"
        finding = InsightFinding(type="metric_change", metric=metric, dimension=dimension,
            summary=(f"Observed {metric} {direction} from {previous:.6g} in {previous_period} "
                     f"to {current:.6g} in {current_period}. No causal explanation is inferred."),
            current_value=current, previous_value=previous, absolute_change=absolute,
            relative_change=relative, details={"current_period": str(current_period),
                "previous_period": str(previous_period), "time_field": time_field})
        findings = [finding]
        warnings = []
        applied_threshold = threshold if threshold is not None else self.settings.insight_change_threshold
        if relative is not None and abs(relative) >= applied_threshold:
            findings.append(InsightFinding(type="anomaly", metric=metric, dimension=dimension,
                summary=(f"The observed absolute percentage change ({abs(relative):.2%}) meets the "
                         f"configured {applied_threshold:.2%} monitoring threshold; this is a flag, not a cause."),
                current_value=current, previous_value=previous, absolute_change=absolute,
                relative_change=relative, confidence=0.9, details={
                    "method": "absolute percentage change", "threshold": applied_threshold,
                    "threshold_source": "request" if threshold is not None else "platform_default"}))
        if threshold is None:
            warnings.append("Anomaly flag uses the platform default percentage-change threshold, not a business SLA")
        if dimension:
            previous_rank = {row[dimension]: rank for rank, row in enumerate(
                sorted(previous_rows, key=lambda item: item["value"] or float("-inf"), reverse=True), 1)}
            current_ranked = sorted(current_rows, key=lambda item: item["value"] or float("-inf"), reverse=True)
            movement = [{"entity": row[dimension], "current_rank": rank,
                         "previous_rank": previous_rank.get(row[dimension]),
                         "rank_change": (previous_rank[row[dimension]] - rank
                                         if row[dimension] in previous_rank else None)}
                        for rank, row in enumerate(current_ranked, 1)]
            findings.append(InsightFinding(type="ranking_movement", metric=metric,
                dimension=dimension, summary=(f"Observed ranking movement for {len(movement)} "
                    f"{dimension} values across the two actual periods; no causal claim is made."),
                details={"movements": movement}))
        comparison = {"current_period": str(current_period), "comparison_period": str(previous_period),
                      "time_field": time_field, "metric": metric,
                      "dimensions": [dimension] if dimension else []}
        return findings, comparison, warnings
