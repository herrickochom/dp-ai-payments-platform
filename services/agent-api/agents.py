from collections import defaultdict
from typing import Any

from config import Settings
from builder_models import DataSourceBuildRequest
from models import AnalyticalRequest, Evidence, Measure, OrderBy, Permissions
from tools import PermissionDenied, ToolRegistry, quote_identifier


STOP_WORDS = {"what", "which", "where", "have", "does", "with", "contain", "information"}


class DataDiscoveryAgent:
    identity = "data_discovery"

    def run(self, objective: str, permissions: Permissions, tools: ToolRegistry) -> tuple[dict, list, list]:
        if not permissions.can_discover_metadata:
            raise PermissionDenied("metadata discovery permission is required")
        matches = tools.search(objective)
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        for match in matches:
            grouped[(match["catalog"], match["schema"], match["table"])].append(match)

        datasets, evidence = [], []
        described: dict[str, list[dict[str, Any]]] = {}
        # Search results are real information_schema rows. Describe only the best
        # tables to stay bounded and obtain full column evidence.
        ranked = sorted(grouped.items(), key=lambda item: -sum(x["score"] for x in item[1]))[:10]
        for (catalog, schema, table), table_matches in ranked:
            columns = tools.describe_table(catalog, schema, table)
            name = f"{catalog}.{schema}.{table}"
            described[name] = columns
            datasets.append({"catalog": catalog, "schema": schema, "table": table,
                             "matched_columns": [item["column"] for item in table_matches],
                             "columns": columns})
            evidence.append(Evidence(kind="metadata", reference=name,
                details={"matched_columns": [item["column"] for item in table_matches]}))

        joins = self._join_candidates(described)
        dimensions, measures = set(), set()
        for columns in described.values():
            for column in columns:
                name, data_type = column["column_name"], column["data_type"].lower()
                if any(token in name.lower() for token in ("district", "region", "date", "status", "type")):
                    dimensions.add(name)
                if any(token in data_type for token in ("decimal", "double", "real", "integer", "bigint")):
                    measures.add(name)
        warnings = [] if datasets else ["No matching tables or columns were found in live Trino metadata"]
        result = {"datasets": datasets, "tables": [f"{d['catalog']}.{d['schema']}.{d['table']}" for d in datasets],
                  "join_candidates": joins, "dimensions": sorted(dimensions),
                  "measures": sorted(measures), "confidence": min(1.0, len(datasets) / 3),
                  "relationships": "inferred only; Trino metadata exposes no foreign-key confirmation"}
        return result, evidence, warnings

    @staticmethod
    def _join_candidates(described: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
        owners: dict[str, list[str]] = defaultdict(list)
        for table, columns in described.items():
            for column in columns:
                name = column["column_name"]
                if name.endswith(("_id", "_sk")):
                    owners[name].append(table)
        return [{"left": tables[0], "right": table, "column": column,
                 "relationship": "inferred", "confidence": 0.6}
                for column, tables in owners.items() if len(tables) > 1
                for table in tables[1:]][:20]


class AnalyticsAgent:
    identity = "analytics"

    def __init__(self, discovery: DataDiscoveryAgent, settings: Settings):
        self.discovery, self.settings = discovery, settings

    def run(self, objective: str, permissions: Permissions, tools: ToolRegistry) -> tuple[dict, list, list]:
        if not permissions.can_execute_read_queries:
            raise PermissionDenied("read-query permission is required")
        discovery, evidence, warnings = self.discovery.run(objective, permissions, tools)
        candidate = self._select_candidate(discovery["datasets"])
        if not candidate:
            return {"analytical_request": None, "query": None, "data": None,
                    "explanation": "Available metadata cannot support this analytical request."}, evidence, warnings

        column_names = {column["column_name"] for column in candidate["columns"]}
        dimension = next((name for name in ("district", "region", "parish") if name in column_names), None)
        rate = next((name for name in ("principal_repayment_rate", "repayment_rate") if name in column_names), None)
        if not dimension or not rate:
            warnings.append("No discovered dataset contains both a geographic dimension and repayment-rate measure")
            return {"analytical_request": None, "query": None, "data": None,
                    "relevant_datasets": discovery["tables"],
                    "explanation": "The discovered columns do not support repayment performance by geography."}, evidence, warnings

        dataset = f"{candidate['catalog']}.{candidate['schema']}.{candidate['table']}"
        spec = AnalyticalRequest(datasets=[dataset], dimensions=[dimension],
            measures=[Measure(name="repayment_performance", expression=rate, aggregation="avg")],
            group_by=[dimension], order_by=[OrderBy(field="repayment_performance", direction="asc")],
            limit=min(20, permissions.max_rows, self.settings.max_rows))
        quoted_dataset = ".".join(quote_identifier(part) for part in dataset.split("."))
        sql = (f"SELECT {quote_identifier(dimension)}, AVG({quote_identifier(rate)}) AS repayment_performance "
               f"FROM {quoted_dataset} WHERE {quote_identifier(dimension)} IS NOT NULL "
               f"GROUP BY {quote_identifier(dimension)} ORDER BY repayment_performance ASC LIMIT {spec.limit}")
        query_result = tools.execute_query(sql, min(permissions.max_rows, spec.limit))
        data_source = tools.build_data_source(DataSourceBuildRequest(
            semantic_request=spec, permissions=permissions,
            query_id=query_result.get("query_id")))
        evidence.append(Evidence(kind="query", reference=query_result.get("query_id") or "trino-query",
            details={"dataset": dataset, "row_count": query_result["row_count"]}))
        return {"analytical_request": spec.model_dump(),
                "data_source": data_source.model_dump(), "query": query_result["sql"],
                "data": {"columns": query_result["columns"], "rows": query_result["rows"],
                         "row_count": query_result["row_count"]},
                "explanation": "Results are ordered from lowest to highest average repayment performance."}, evidence, warnings

    @staticmethod
    def _select_candidate(datasets: list[dict[str, Any]]) -> dict[str, Any] | None:
        def score(dataset):
            names = {c["column_name"] for c in dataset["columns"]}
            return (3 * bool(names & {"district", "region", "parish"})
                    + 4 * bool(names & {"principal_repayment_rate", "repayment_rate"})
                    + 2 * ("principal_repayment_rate" in names)
                    + 2 * (dataset["schema"] == "consumption"))
        ranked = sorted(datasets, key=score, reverse=True)
        return ranked[0] if ranked and score(ranked[0]) >= 7 else None
