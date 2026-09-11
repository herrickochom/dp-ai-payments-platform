import re
import time
from typing import Any, Callable

import sqlglot
from sqlglot import exp

from config import Settings
from models import ToolCall
from trino_gateway import TrinoGateway

SEARCH_STOP_WORDS = {"what", "which", "where", "have", "does", "with", "contain",
                     "contains", "information", "data", "table", "tables", "dataset",
                     "datasets", "pdm", "the", "and", "for"}


class ToolError(RuntimeError):
    pass


class PermissionDenied(ToolError):
    pass


class ToolLimitExceeded(ToolError):
    pass


class QueryValidationError(ToolError):
    pass


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class ToolRegistry:
    def __init__(self, gateway: TrinoGateway, settings: Settings):
        self.gateway, self.settings = gateway, settings
        self.calls: list[ToolCall] = []

    def _call(self, name: str, function: Callable[[], Any]) -> Any:
        if len(self.calls) >= self.settings.max_tool_calls:
            raise ToolLimitExceeded(f"tool-call limit {self.settings.max_tool_calls} exceeded")
        started = time.monotonic()
        record = ToolCall(tool=name, status="completed", duration_ms=0)
        # Reserve the call before execution so nested controlled operations
        # cannot bypass the global request limit.
        self.calls.append(record)
        try:
            result = function()
        except Exception as exc:
            record.status = "failed"
            record.duration_ms = (time.monotonic() - started) * 1000
            record.metadata = {"error_category": type(exc).__name__}
            raise
        record.duration_ms = (time.monotonic() - started) * 1000
        return result

    def list_catalogs(self):
        return self._call("metadata.list_catalogs", lambda: self._rows("SHOW CATALOGS"))

    def list_schemas(self, catalog: str):
        sql = f"SHOW SCHEMAS FROM {quote_identifier(catalog)}"
        return self._call("metadata.list_schemas", lambda: self._rows(sql))

    def list_tables(self, catalog: str, schema: str):
        sql = ("SELECT table_name, table_type FROM "
               f"{quote_identifier(catalog)}.information_schema.tables "
               "WHERE table_schema = ? ORDER BY table_name")
        return self._call("metadata.list_tables", lambda: self._parameterized(sql, [schema]))

    def describe_table(self, catalog: str, schema: str, table: str):
        sql = ("SELECT column_name, data_type, ordinal_position FROM "
               f"{quote_identifier(catalog)}.information_schema.columns "
               "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")
        return self._call("metadata.describe_table", lambda: self._parameterized(sql, [schema, table]))

    def search(self, query: str, catalog: str | None = None):
        catalog = catalog or self.settings.trino_catalog
        terms = [t.lower() for t in re.findall(r"[a-zA-Z][a-zA-Z0-9_]+", query)
                 if len(t) > 2 and t.lower() not in SEARCH_STOP_WORDS]
        if not terms:
            raise ToolError("metadata search needs at least one specific business term")
        sql = ("SELECT DISTINCT table_schema, table_name, column_name, data_type FROM "
               f"{quote_identifier(catalog)}.information_schema.columns ORDER BY table_schema, table_name")
        rows = self._call("metadata.search", lambda: self._rows(sql))
        scored = []
        for row in rows:
            haystack = " ".join(str(row[key]).lower().replace("_", " ")
                                for key in ("table_schema", "table_name", "column_name"))
            score = sum(1 for term in terms if term in haystack)
            if score:
                scored.append({"catalog": catalog, "schema": row["table_schema"],
                               "table": row["table_name"], "column": row["column_name"],
                               "type": row["data_type"], "score": score})
        return sorted(scored, key=lambda item: (-item["score"], item["schema"], item["table"]))

    def find_columns(self, names: list[str], catalog: str | None = None):
        return self.search(" ".join(names), catalog)

    def get_lineage(self, dataset: str):
        return self._call("metadata.get_lineage", lambda: {
            "status": "not_available", "dataset": dataset,
            "warning": "Runtime lineage is not configured in Phase 1"
        })

    def rag_search(self, query: str):
        return self._call("rag.search", lambda: {
            "status": "not_configured", "results": [],
            "warning": "No knowledge index is configured"
        })

    def platform_health(self):
        return self._call("platform.health", self.gateway.health)

    def validate_query(self, sql: str, max_rows: int) -> dict[str, Any]:
        return self._call("query.validate", lambda: validate_read_query(
            sql, min(max_rows, self.settings.max_rows), self.settings.max_query_length))

    def explain_query(self, sql: str, max_rows: int):
        validated = self.validate_query(sql, max_rows)
        return self._call("query.explain", lambda: self._rows("EXPLAIN " + validated["sql"]))

    def execute_query(self, sql: str, max_rows: int):
        validated = self.validate_query(sql, max_rows)
        def execute():
            columns, rows, query_id = self.gateway.execute(validated["sql"])
            return {"columns": columns, "rows": rows, "row_count": len(rows),
                    "query_id": query_id, "sql": validated["sql"]}
        return self._call("query.execute", execute)

    def build_data_source(self, request):
        from builders import DataSourceBuilder
        return self._call("builder.datasource.build", lambda: DataSourceBuilder().build(request, self))

    def build_visualization(self, request):
        from builders import VisualizationBuilder
        return self._call("builder.visualization.build", lambda: VisualizationBuilder().build(request))

    def build_dashboard(self, request):
        from builders import DashboardBuilder
        return self._call("builder.dashboard.build", lambda: DashboardBuilder().build(request))

    def _rows(self, sql: str):
        columns, rows, _ = self.gateway.execute(sql)
        return [dict(zip(columns, row)) for row in rows]

    def _parameterized(self, sql: str, parameters: list[str]):
        # Values originate in API inputs/metadata; quote them as SQL string values.
        for value in parameters:
            sql = sql.replace("?", "'" + value.replace("'", "''") + "'", 1)
        return self._rows(sql)


def validate_read_query(sql: str, max_rows: int, max_length: int) -> dict[str, Any]:
    candidate = sql.strip()
    if not candidate or len(candidate) > max_length:
        raise QueryValidationError("query is empty or exceeds the configured maximum length")
    try:
        statements = sqlglot.parse(candidate, read="trino")
    except sqlglot.errors.ParseError as exc:
        raise QueryValidationError(f"invalid SQL: {exc}") from exc
    if len(statements) != 1:
        raise QueryValidationError("exactly one SQL statement is allowed")
    tree = statements[0]
    if not isinstance(tree, (exp.Select, exp.Union, exp.Subquery)):
        raise QueryValidationError("only SELECT or WITH/CTE SELECT queries are allowed")
    forbidden = tuple(getattr(exp, name) for name in (
        "Insert", "Update", "Delete", "Drop", "Alter", "Create", "Command",
        "Merge", "Grant", "Revoke", "Transaction") if hasattr(exp, name))
    if any(tree.find(node) is not None for node in forbidden):
        raise QueryValidationError("write, DDL, privilege, and command statements are forbidden")
    bounded = f"SELECT * FROM ({candidate.rstrip(';')}) AS agent_bounded LIMIT {max_rows}"
    return {"valid": True, "sql": bounded, "max_rows": max_rows}
