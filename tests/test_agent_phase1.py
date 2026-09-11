import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SERVICE = ROOT / "services" / "agent-api"
if not SERVICE.exists() and Path("/app/config.py").exists():
    SERVICE = Path("/app")
sys.path.insert(0, str(SERVICE))

from config import Settings
from models import AgentRequest, Permissions
from orchestrator import Orchestrator
from tools import QueryValidationError, ToolLimitExceeded, ToolRegistry, validate_read_query


COLUMNS = ["table_schema", "table_name", "column_name", "data_type"]
METADATA = [
    ["consumption", "cns_pdm_parish_performance", "district", "varchar"],
    ["consumption", "cns_pdm_parish_performance", "principal_repayment_rate", "double"],
    ["consumption", "cns_pdm_parish_performance", "loan_count", "bigint"],
    ["gold", "gld_fct_pdm_loans", "district_sk", "varchar"],
    ["gold", "gld_fct_pdm_loans", "repayment_rate", "double"],
]


class FakeGateway:
    def __init__(self, unavailable=False): self.unavailable = unavailable
    def health(self):
        if self.unavailable: raise ConnectionError("Trino down")
        return {"status": "healthy", "query_id": "q-health"}
    def execute(self, sql):
        if self.unavailable: raise ConnectionError("Trino down")
        lowered = sql.lower()
        if "information_schema.columns" in lowered and "where table_schema" not in lowered:
            return COLUMNS, METADATA, "q-meta"
        if "information_schema.columns" in lowered:
            rows = [["district", "varchar", 1], ["principal_repayment_rate", "double", 2],
                    ["loan_count", "bigint", 3]]
            return ["column_name", "data_type", "ordinal_position"], rows, "q-desc"
        if "avg(" in lowered:
            return ["district", "repayment_performance"], [["A", 0.42], ["B", 0.81]], "q-data"
        if "show catalogs" in lowered: return ["Catalog"], [["iceberg"]], "q-cat"
        if "show schemas" in lowered: return ["Schema"], [["consumption"], ["gold"]], "q-sch"
        if "information_schema.tables" in lowered:
            return ["table_name", "table_type"], [["cns_pdm_parish_performance", "BASE TABLE"]], "q-tab"
        return ["healthy"], [[1]], "q"


class QuerySafetyTests(unittest.TestCase):
    def test_select_and_cte_allowed_and_bounded(self):
        for sql in ("SELECT * FROM x", "WITH x AS (SELECT 1 a) SELECT a FROM x"):
            result = validate_read_query(sql, 7, 1000)
            self.assertIn("LIMIT 7", result["sql"])
    def test_writes_and_ddl_are_blocked(self):
        for verb in ("INSERT INTO x VALUES (1)", "UPDATE x SET a=1", "DELETE FROM x",
                     "DROP TABLE x", "ALTER TABLE x ADD COLUMN a int", "CREATE TABLE x(a int)",
                     "TRUNCATE TABLE x", "MERGE INTO x USING y ON x.a=y.a WHEN MATCHED THEN DELETE",
                     "GRANT SELECT ON x TO user", "REVOKE SELECT ON x FROM user"):
            with self.subTest(verb=verb), self.assertRaises(QueryValidationError):
                validate_read_query(verb, 10, 1000)
    def test_multi_statement_and_length_bypass_blocked(self):
        with self.assertRaises(QueryValidationError): validate_read_query("SELECT 1; DROP TABLE x", 10, 1000)
        with self.assertRaises(QueryValidationError): validate_read_query("SELECT " + "x" * 50, 10, 10)
    def test_row_limit_is_enforced(self):
        result = validate_read_query("SELECT * FROM x LIMIT 9999", 5, 1000)
        self.assertTrue(result["sql"].endswith("LIMIT 5"))


class AgentTests(unittest.TestCase):
    def setUp(self):
        self.settings = Settings(max_rows=50, max_tool_calls=20, query_timeout_seconds=9)
        self.orchestrator = Orchestrator(self.settings, FakeGateway())
    def test_discovery_routes_and_returns_real_metadata_evidence(self):
        response = self.orchestrator.execute(AgentRequest(objective="What tables contain PDM repayment information?"))
        self.assertEqual("data_discovery", response.agent)
        self.assertIn("iceberg.consumption.cns_pdm_parish_performance", response.result["tables"])
        self.assertTrue(response.evidence)
    def test_analytics_routes_discovers_specifies_validates_and_executes(self):
        response = self.orchestrator.execute(AgentRequest(objective="What is repayment performance by district?"))
        self.assertEqual("analytics", response.agent)
        self.assertEqual("completed", response.status)
        self.assertEqual(["district"], response.result["analytical_request"]["dimensions"])
        self.assertEqual(2, response.result["data"]["row_count"])
        self.assertIn("metadata.search", [c.tool for c in response.tool_calls])
        self.assertIn("query.validate", [c.tool for c in response.tool_calls])
        self.assertIn("query.execute", [c.tool for c in response.tool_calls])
    def test_unknown_request_fails_safely(self):
        response = self.orchestrator.execute(AgentRequest(objective="Write me a poem"))
        self.assertEqual("unsupported", response.status)
    def test_unavailable_metadata_fails_without_invention(self):
        response = Orchestrator(self.settings, FakeGateway(True)).execute(
            AgentRequest(agent="data_discovery", objective="What tables contain repayments?"))
        self.assertEqual("failed", response.status)
        self.assertEqual({}, response.result)
    def test_inferred_join_is_labelled(self):
        result, _, _ = self.orchestrator.discovery.run("district repayment", Permissions(),
                                                        ToolRegistry(FakeGateway(), self.settings))
        self.assertIn("inferred", result["relationships"])
    def test_tool_limit(self):
        registry = ToolRegistry(FakeGateway(), Settings(max_tool_calls=1))
        registry.list_catalogs()
        with self.assertRaises(ToolLimitExceeded): registry.list_catalogs()
    def test_permissions_and_configured_timeout(self):
        denied = self.orchestrator.execute(AgentRequest(agent="analytics", objective="repayment by district",
            permissions=Permissions(can_execute_read_queries=False)))
        self.assertEqual("failed", denied.status)
        self.assertEqual(9, self.settings.query_timeout_seconds)
    def test_environment_driven_configuration(self):
        with patch.dict(os.environ, {"TRINO_HOST": "query.example", "AGENT_MODEL_PROVIDER": "disabled"}):
            module = importlib.reload(importlib.import_module("config"))
            configured = module.Settings()
        self.assertEqual("query.example", configured.trino_host)
        self.assertEqual("disabled", configured.model_provider)


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import api
        api.orchestrator = Orchestrator(Settings(), FakeGateway())
        cls.client = TestClient(api.app)
    def test_agents_endpoint(self): self.assertEqual(2, len(self.client.get("/agents").json()["agents"]))
    def test_health_endpoint(self): self.assertEqual(200, self.client.get("/agents/health").status_code)
    def test_query_endpoint_and_validation_error(self):
        self.assertEqual(200, self.client.post("/agents/query", json={"objective": "What tables contain repayment information?"}).status_code)
        response = self.client.post("/agents/query", json={"objective": "x"})
        self.assertEqual(422, response.status_code)


if __name__ == "__main__": unittest.main()
