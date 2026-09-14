import re
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from typing import Any, Callable

import sqlglot
from sqlglot import exp

from classification import (
    ClassificationError,
    TrustedClassificationResolver,
)
from config import Settings
from governance import GovernancePolicyEngine, mask_value
from governance_models import (
    GovernanceRequest,
    PolicyDecision,
    PolicyDecisionType,
)
from models import ToolCall
from knowledge import KnowledgeRetriever
from knowledge_models import KnowledgeQuery
from observability import emit
from trino_gateway import TrinoGateway


SEARCH_STOP_WORDS = {
    "what",
    "which",
    "where",
    "have",
    "does",
    "with",
    "contain",
    "contains",
    "information",
    "data",
    "table",
    "tables",
    "dataset",
    "datasets",
    "pdm",
    "the",
    "and",
    "for",
}

SEARCH_STOP_WORDS.update(
    {
        "build",
        "dashboard",
        "visualize",
        "visualise",
        "chart",
        "graph",
    }
)


class ToolError(RuntimeError):
    pass


class PermissionDenied(ToolError):
    pass


class ToolLimitExceeded(ToolError):
    pass


class ToolTimeout(ToolError):
    """Raised when a tool call misses its bounded execution deadline."""

    pass


class QueryValidationError(ToolError):
    pass


class GovernanceDenied(PermissionDenied):
    """
    Raised when deterministic governance policy prevents a tool operation.
    """

    pass


class GovernanceApprovalRequired(PermissionDenied):
    """
    Raised when a request requires explicit approval before execution.
    """

    pass


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


#
# Shared bounded executor used to enforce per-tool execution deadlines.
# A small worker pool is sufficient for the local deterministic backend.
#
_TOOL_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="agent-tool")


class ToolRegistry:
    def __init__(
        self,
        gateway: TrinoGateway,
        settings: Settings,
        governance_engine: GovernancePolicyEngine | None = None,
        governance_request: GovernanceRequest | None = None,
        classification_resolver: TrustedClassificationResolver | None = None,
        knowledge_retriever: KnowledgeRetriever | None = None,
        audit_store=None,
        request_id: str | None = None,
    ):
        self.gateway = gateway
        self.settings = settings
        self.calls: list[ToolCall] = []

        #
        # Phase 5 governance remains deterministic and local.
        #
        self.governance_engine = (
            governance_engine
            if governance_engine is not None
            else GovernancePolicyEngine()
        )

        #
        # Request-scoped governance context.
        #
        # Agents do not need to know how policy evaluation works.
        # Any analytical query executed through this ToolRegistry
        # automatically inherits this governance request unless an
        # explicit GovernanceRequest is supplied to execute_query().
        #
        self.governance_request = governance_request

        #
        # Trusted platform classification.
        #
        # Dataset and field classification must come from platform-owned
        # metadata, never from caller-supplied GovernanceRequest.resource
        # classification values.
        #
        self.classification_resolver = (
            classification_resolver
            if classification_resolver is not None
            else TrustedClassificationResolver()
        )
        self.knowledge_retriever = knowledge_retriever
        self.audit_store = audit_store
        self.request_id = request_id or None

    # ------------------------------------------------------------------
    # Controlled tool execution
    # ------------------------------------------------------------------

    def _tool_timeout_seconds(self) -> float:
        return float(getattr(self.settings, "tool_timeout_seconds", 30) or 30)

    def _tool_max_retries(self) -> int:
        return int(getattr(self.settings, "tool_max_retries", 1) or 0)

    @staticmethod
    def _is_transient(exc: BaseException) -> bool:
        return isinstance(exc, (ToolTimeout, TimeoutError, ConnectionError, OSError))

    @staticmethod
    def _execute_bounded(function: Callable[[], Any], timeout: float) -> Any:
        future = _TOOL_EXECUTOR.submit(function)
        try:
            return future.result(timeout=timeout)
        except FuturesTimeoutError:
            # A worker-raised timeout error has already completed; otherwise
            # the future timed out and is still running in the pool.
            if future.done():
                failure = future.exception()
                raise failure from None
            raise ToolTimeout("tool execution exceeded the bounded deadline") from None

    def _call(
        self,
        name: str,
        function: Callable[[], Any],
    ) -> Any:
        if len(self.calls) >= self.settings.max_tool_calls:
            raise ToolLimitExceeded(
                f"tool-call limit "
                f"{self.settings.max_tool_calls} exceeded"
            )

        started = time.monotonic()

        record = ToolCall(
            tool=name,
            status="completed",
            duration_ms=0,
        )

        #
        # Reserve the call before execution so nested controlled
        # operations cannot bypass the global request limit.
        #
        self.calls.append(record)

        max_retries = self._tool_max_retries()
        attempts = 0

        while True:
            attempts += 1
            try:
                result = self._execute_bounded(
                    function, self._tool_timeout_seconds()
                )
            except Exception as exc:
                record.status = "failed"
                record.duration_ms = (
                    time.monotonic() - started
                ) * 1000
                record.metadata = {
                    "error_category": type(exc).__name__,
                    "attempts": attempts,
                }
                emit(
                    "tool_invoked",
                    tool=name,
                    request_id=self.request_id,
                    status="failed",
                    error_category=record.metadata["error_category"],
                    attempts=attempts,
                    duration_ms=round(record.duration_ms, 2),
                )
                if not self._is_transient(exc) or attempts > max_retries:
                    raise
                continue

            record.status = "completed"
            record.duration_ms = (
                time.monotonic() - started
            ) * 1000

            if attempts > 1:
                record.metadata["retries"] = attempts - 1

            emit(
                "tool_invoked",
                tool=name,
                request_id=self.request_id,
                status="completed",
                duration_ms=round(record.duration_ms, 2),
            )
            return result

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def list_catalogs(self):
        return self._call(
            "metadata.list_catalogs",
            lambda: self._rows("SHOW CATALOGS"),
        )

    def list_schemas(
        self,
        catalog: str,
    ):
        sql = (
            f"SHOW SCHEMAS FROM "
            f"{quote_identifier(catalog)}"
        )

        return self._call(
            "metadata.list_schemas",
            lambda: self._rows(sql),
        )

    def list_tables(
        self,
        catalog: str,
        schema: str,
    ):
        sql = (
            "SELECT table_name, table_type FROM "
            f"{quote_identifier(catalog)}."
            "information_schema.tables "
            "WHERE table_schema = ? "
            "ORDER BY table_name"
        )

        return self._call(
            "metadata.list_tables",
            lambda: self._parameterized(
                sql,
                [schema],
            ),
        )

    def describe_table(
        self,
        catalog: str,
        schema: str,
        table: str,
    ):
        sql = (
            "SELECT column_name, data_type, ordinal_position FROM "
            f"{quote_identifier(catalog)}."
            "information_schema.columns "
            "WHERE table_schema = ? "
            "AND table_name = ? "
            "ORDER BY ordinal_position"
        )

        return self._call(
            "metadata.describe_table",
            lambda: self._parameterized(
                sql,
                [schema, table],
            ),
        )

    def search(
        self,
        query: str,
        catalog: str | None = None,
    ):
        catalog = (
            catalog
            or self.settings.trino_catalog
        )

        terms = [
            term.lower()
            for term in re.findall(
                r"[a-zA-Z][a-zA-Z0-9_]+",
                query,
            )
            if (
                len(term) > 2
                and term.lower()
                not in SEARCH_STOP_WORDS
            )
        ]

        if not terms:
            raise ToolError(
                "metadata search needs at least one "
                "specific business term"
            )

        sql = (
            "SELECT DISTINCT "
            "table_schema, "
            "table_name, "
            "column_name, "
            "data_type "
            "FROM "
            f"{quote_identifier(catalog)}."
            "information_schema.columns "
            "ORDER BY table_schema, table_name"
        )

        rows = self._call(
            "metadata.search",
            lambda: self._rows(sql),
        )

        scored = []

        for row in rows:
            haystack = " ".join(
                str(row[key])
                .lower()
                .replace("_", " ")
                for key in (
                    "table_schema",
                    "table_name",
                    "column_name",
                )
            )

            score = sum(
                1
                for term in terms
                if term in haystack
            )

            if score:
                scored.append(
                    {
                        "catalog": catalog,
                        "schema": row["table_schema"],
                        "table": row["table_name"],
                        "column": row["column_name"],
                        "type": row["data_type"],
                        "score": score,
                    }
                )

        return sorted(
            scored,
            key=lambda item: (
                -item["score"],
                item["schema"],
                item["table"],
            ),
        )

    def find_columns(
        self,
        names: list[str],
        catalog: str | None = None,
    ):
        return self.search(
            " ".join(names),
            catalog,
        )

    def get_lineage(
        self,
        dataset: str,
    ):
        return self._call(
            "metadata.get_lineage",
            lambda: {
                "status": "not_available",
                "dataset": dataset,
                "warning": (
                    "Runtime lineage is not "
                    "configured in Phase 1"
                ),
            },
        )

    # ------------------------------------------------------------------
    # RAG
    # ------------------------------------------------------------------

    def rag_search(
        self,
        query: KnowledgeQuery,
    ):
        return self._call(
            "rag.search",
            lambda: (
                self.knowledge_retriever.retrieve(query, self.governance_request).model_dump(mode="json")
                if self.knowledge_retriever is not None
                else {"status": "not_configured", "results": [],
                      "warning": "No knowledge index is configured"}
            ),
        )

    # ------------------------------------------------------------------
    # Platform
    # ------------------------------------------------------------------

    def platform_health(self):
        return self._call(
            "platform.health",
            self.gateway.health,
        )

    # ------------------------------------------------------------------
    # Query validation
    # ------------------------------------------------------------------

    def validate_query(
        self,
        sql: str,
        max_rows: int,
    ) -> dict[str, Any]:
        return self._call(
            "query.validate",
            lambda: validate_read_query(
                sql,
                min(
                    max_rows,
                    self.settings.max_rows,
                ),
                self.settings.max_query_length,
            ),
        )

    def explain_query(
        self,
        sql: str,
        max_rows: int,
    ):
        validated = self.validate_query(
            sql,
            max_rows,
        )

        return self._call(
            "query.explain",
            lambda: self._rows(
                "EXPLAIN "
                + validated["sql"]
            ),
        )

    # ------------------------------------------------------------------
    # Phase 5 governance
    # ------------------------------------------------------------------

    def evaluate_governance(
        self,
        request: GovernanceRequest,
    ) -> PolicyDecision:
        """
        Run deterministic governance policy evaluation.

        This does not execute a model and does not access Trino.
        """

        return self.governance_engine.evaluate(
            request
        )

    def _authoritative_governance_request(
        self,
        sql: str,
        request: GovernanceRequest,
    ) -> GovernanceRequest:
        """
        Replace caller-supplied resource classification with trusted
        SQL-derived platform classification.

        Identity, purpose, action, approval, and geographic scope remain
        request context. Dataset classification, selected fields, field
        classifications, and semantic types are resolved from trusted
        platform metadata.

        Any classification-resolution failure fails closed before Trino
        execution.
        """

        district = getattr(
            request.resource,
            "district",
            None,
        )
        parish = getattr(
            request.resource,
            "parish",
            None,
        )

        try:
            resource = self.classification_resolver.resolve_sql(
                sql,
                district=district,
                parish=parish,
            )

        except ClassificationError as exc:
            raise GovernanceDenied(
                "Trusted data classification failed: "
                f"{exc}"
            ) from exc

        #
        # Pydantic v2 model_copy preserves all request fields that are not
        # resource classification metadata, including identity, purpose,
        # action, approval, and any future governance context fields.
        #
        return request.model_copy(
            update={
                "resource": resource,
            }
        )

    def _enforce_governance_pre_query(
        self,
        request: GovernanceRequest,
    ) -> PolicyDecision:
        """
        Evaluate governance before analytical query execution.

        DENY and REQUIRE_APPROVAL prevent Trino execution.

        MASK permits execution but requires the result to be
        transformed before being returned to the caller.
        """

        decision = self.evaluate_governance(
            request
        )

        #
        # Durable structured evidence for consequential policy decisions
        # (DENY / REQUIRE_APPROVAL / MASK). Routine ALLOW decisions for
        # internal query enforcement are not persisted at tool level; the
        # explicit /governance endpoints persist every evaluation.
        #
        if self.audit_store is not None and decision.decision != PolicyDecisionType.ALLOW:
            self.audit_store.record_decision(request, decision, self.request_id)

        if (
            decision.decision
            == PolicyDecisionType.DENY
        ):
            reason = (
                "; ".join(decision.reasons)
                or "Governance policy denied access."
            )

            raise GovernanceDenied(
                reason
            )

        if (
            decision.decision
            == PolicyDecisionType.REQUIRE_APPROVAL
        ):
            reason = (
                "; ".join(decision.reasons)
                or (
                    "Governance policy requires "
                    "approval."
                )
            )

            raise GovernanceApprovalRequired(
                reason
            )

        return decision

    @staticmethod
    def _mask_query_rows(
        columns: list[str],
        rows: list[list[Any]],
        request: GovernanceRequest,
        decision: PolicyDecision,
    ) -> list[list[Any]]:
        """
        Mask only fields explicitly selected by the deterministic
        governance decision.

        The returned row structure stays list[list[Any]] to preserve
        the Phase 1-4 query-result contract.
        """

        if (
            decision.decision
            != PolicyDecisionType.MASK
        ):
            return rows

        masked_names = {
            name.lower()
            for name in decision.masked_fields
        }

        classifications = {
            item.field.lower(): item
            for item
            in request.resource.field_classifications
        }

        output: list[list[Any]] = []

        for row in rows:
            safe_row = list(row)

            for index, column in enumerate(
                columns
            ):
                column_key = column.lower()

                if (
                    column_key
                    not in masked_names
                ):
                    continue

                classification = (
                    classifications.get(
                        column_key
                    )
                )

                #
                # Fail closed at field level:
                # if policy says MASK but classification metadata
                # cannot be resolved, replace the value entirely.
                #
                if classification is None:
                    safe_row[index] = "***"
                    continue

                safe_row[index] = mask_value(
                    classification.semantic_type,
                    safe_row[index],
                )

            output.append(
                safe_row
            )

        return output

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute_query(
        self,
        sql: str,
        max_rows: int,
        governance_request: GovernanceRequest | None = None,
    ):
        """
        Execute a bounded read-only query.

        An explicit governance_request overrides the request-scoped
        governance context configured on this ToolRegistry.

        When governance is active:

        1. SQL is validated.
        2. Trusted classification is resolved from the SQL.
        3. Caller-supplied resource classification is replaced.
        4. Governance is evaluated before execution.
        5. DENY stops execution.
        6. REQUIRE_APPROVAL stops execution.
        7. ALLOW executes normally.
        8. MASK executes and masks governed fields before results
           leave ToolRegistry.

        Requests without governance context remain backward compatible
        with Phase 1-4 behaviour.
        """

        #
        # If an individual query does not provide an explicit governance
        # request, inherit the request-scoped governance context.
        #
        if governance_request is None:
            governance_request = (
                self.governance_request
            )

        validated = self.validate_query(
            sql,
            max_rows,
        )

        governance_decision = None

        if governance_request is not None:
            governance_request = (
                self._authoritative_governance_request(
                    sql,
                    governance_request,
                )
            )

            governance_decision = (
                self._enforce_governance_pre_query(
                    governance_request
                )
            )

        def execute():
            columns, rows, query_id = (
                self.gateway.execute(
                    validated["sql"]
                )
            )

            if (
                governance_request is not None
                and governance_decision is not None
            ):
                rows = self._mask_query_rows(
                    columns=columns,
                    rows=rows,
                    request=governance_request,
                    decision=governance_decision,
                )

            result = {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "query_id": query_id,
                "sql": validated["sql"],
            }

            if governance_decision is not None:
                result["governance"] = {
                    "decision_id": (
                        governance_decision.decision_id
                    ),
                    "decision": (
                        governance_decision
                        .decision
                        .value
                    ),
                    "masked_fields": (
                        governance_decision
                        .masked_fields
                    ),
                    "matched_policies": (
                        governance_decision
                        .matched_policies
                    ),
                    "approval_required": (
                        governance_decision
                        .approval_required
                    ),
                }

            return result

        return self._call(
            "query.execute",
            execute,
        )

    # ------------------------------------------------------------------
    # Builders
    # ------------------------------------------------------------------

    def build_data_source(
        self,
        request,
    ):
        from builders import DataSourceBuilder

        return self._call(
            "builder.datasource.build",
            lambda: (
                DataSourceBuilder()
                .build(
                    request,
                    self,
                )
            ),
        )

    def build_visualization(
        self,
        request,
    ):
        from builders import VisualizationBuilder

        return self._call(
            "builder.visualization.build",
            lambda: (
                VisualizationBuilder()
                .build(request)
            ),
        )

    def build_dashboard(
        self,
        request,
    ):
        from builders import DashboardBuilder

        return self._call(
            "builder.dashboard.build",
            lambda: (
                DashboardBuilder()
                .build(request)
            ),
        )

    # ------------------------------------------------------------------
    # Data Quality
    # ------------------------------------------------------------------

    def dq_list_rules(
        self,
        dataset: str | None = None,
    ):
        from quality_rules import DQRuleCatalogue

        catalogue = DQRuleCatalogue(
            self.settings.dbt_models_path,
            self.settings.trino_catalog,
        )

        return self._call(
            "dq.list_rules",
            lambda: (
                catalogue.list_rules(
                    dataset
                )
            ),
        )

    def dq_run_rule(
        self,
        rule,
        permissions,
    ):
        from data_quality import DataQualityEngine

        return self._call(
            "dq.run_rule",
            lambda: (
                DataQualityEngine(self)
                .run_rule(
                    rule,
                    permissions,
                )
            ),
        )

    def dq_profile(
        self,
        request,
    ):
        from data_quality import DataQualityEngine

        return self._call(
            "dq.profile",
            lambda: (
                DataQualityEngine(self)
                .profile(request)
            ),
        )

    def dq_summary(
        self,
        permissions,
        dataset: str = (
            "iceberg.silver."
            "slv_pdm_dq_results"
        ),
    ):
        from data_quality import DataQualityEngine

        return self._call(
            "dq.summary",
            lambda: (
                DataQualityEngine(self)
                .existing_results_summary(
                    permissions,
                    dataset,
                )
            ),
        )

    # ------------------------------------------------------------------
    # Internal SQL helpers
    # ------------------------------------------------------------------

    def _rows(
        self,
        sql: str,
    ):
        columns, rows, _ = (
            self.gateway.execute(sql)
        )

        return [
            dict(
                zip(
                    columns,
                    row,
                )
            )
            for row in rows
        ]

    def _parameterized(
        self,
        sql: str,
        parameters: list[str],
    ):
        #
        # Values originate in API inputs/metadata.
        # Quote them as SQL string values.
        #
        for value in parameters:
            sql = sql.replace(
                "?",
                "'"
                + value.replace(
                    "'",
                    "''",
                )
                + "'",
                1,
            )

        return self._rows(sql)


def validate_read_query(
    sql: str,
    max_rows: int,
    max_length: int,
) -> dict[str, Any]:
    candidate = sql.strip()

    if (
        not candidate
        or len(candidate) > max_length
    ):
        raise QueryValidationError(
            "query is empty or exceeds "
            "the configured maximum length"
        )

    try:
        statements = sqlglot.parse(
            candidate,
            read="trino",
        )

    except sqlglot.errors.ParseError as exc:
        raise QueryValidationError(
            f"invalid SQL: {exc}"
        ) from exc

    if len(statements) != 1:
        raise QueryValidationError(
            "exactly one SQL statement "
            "is allowed"
        )

    tree = statements[0]

    if not isinstance(
        tree,
        (
            exp.Select,
            exp.Union,
            exp.Subquery,
        ),
    ):
        raise QueryValidationError(
            "only SELECT or WITH/CTE "
            "SELECT queries are allowed"
        )

    forbidden = tuple(
        getattr(
            exp,
            name,
        )
        for name in (
            "Insert",
            "Update",
            "Delete",
            "Drop",
            "Alter",
            "Create",
            "Command",
            "Merge",
            "Grant",
            "Revoke",
            "Transaction",
        )
        if hasattr(
            exp,
            name,
        )
    )

    if any(
        tree.find(node) is not None
        for node in forbidden
    ):
        raise QueryValidationError(
            "write, DDL, privilege, "
            "and command statements "
            "are forbidden"
        )

    bounded = (
        "SELECT * FROM ("
        f"{candidate.rstrip(';')}"
        ") AS agent_bounded "
        f"LIMIT {max_rows}"
    )

    return {
        "valid": True,
        "sql": bounded,
        "max_rows": max_rows,
    }
