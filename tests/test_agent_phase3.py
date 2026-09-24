import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

from agent_asgi_client import LocalClient

ROOT = Path(__file__).resolve().parents[1]
SERVICE = ROOT / "services" / "agent-api"
if not SERVICE.exists() and Path("/app/config.py").exists(): SERVICE = Path("/app")
sys.path.insert(0, str(SERVICE))

from bi_adapter import AdapterError, AdapterPermissionDenied, SUPERSET_TYPES, SupersetAdapter
from builder_models import (DashboardBuildRequest, DashboardFilter, DataSourceBuildRequest,
                            Encoding, LayoutItem, VisualizationBuildRequest)
from builders import DashboardBuilder, DataSourceBuilder, VisualizationBuilder
from config import Settings
from models import AgentRequest, AnalyticalRequest, Measure, Permissions
from orchestrator import Orchestrator
from tools import ToolRegistry

DATASET = "iceberg.consumption.cns_pdm_local_government_performance"


class Gateway:
    def execute(self, sql):
        if "information_schema.columns" in sql and "WHERE table_schema" not in sql:
            return ["table_schema", "table_name", "column_name", "data_type"], [
                ["consumption", "cns_pdm_local_government_performance", "district", "varchar"],
                ["consumption", "cns_pdm_local_government_performance", "principal_repayment_rate", "double"]], "qm"
        if "information_schema.columns" in sql:
            return ["column_name", "data_type", "ordinal_position"], [
                ["district", "varchar", 1], ["principal_repayment_rate", "double", 2]], "qd"
        if "AVG(" in sql:
            return ["district", "repayment_performance"], [["Kabale", .24]], "qq"
        return ["healthy"], [[1]], "qh"
    def health(self): return {"status": "healthy"}


def artifacts():
    semantic = AnalyticalRequest(datasets=[DATASET], dimensions=["district"],
        measures=[Measure(name="rate", expression="principal_repayment_rate", aggregation="avg")])
    source = DataSourceBuilder().build(DataSourceBuildRequest(semantic_request=semantic),
                                       ToolRegistry(Gateway(), Settings()))
    visuals = []
    for identifier, kind, enc in (
        ("kpi", "kpi", [Encoding(channel="value", field="principal_repayment_rate", role="measure", aggregation="avg")]),
        ("bar", "bar", [Encoding(channel="x", field="district", role="dimension"), Encoding(channel="y", field="principal_repayment_rate", role="measure", aggregation="avg")]),
        ("table", "table", [Encoding(channel="column", field="district", role="dimension")])):
        visuals.append(VisualizationBuilder().build(VisualizationBuildRequest(
            id=identifier, type=kind, title=identifier, data_source=source, encoding=enc)))
    dashboard = DashboardBuilder().build(DashboardBuildRequest(title="Repayment dashboard",
        visualizations=visuals, filters=[DashboardFilter(field="district", data_source_id=source.id)],
        layout=[LayoutItem(visualization_id=v.id, x=0, y=i*4, width=12, height=4)
                for i, v in enumerate(visuals)]))
    return source, visuals, dashboard


class Phase3AgentTests(unittest.TestCase):
    def setUp(self): self.orchestrator = Orchestrator(Settings(), Gateway())
    def test_visualization_fallback_and_provenance(self):
        result = self.orchestrator.execute(AgentRequest(agent="visualization",
            objective="Visualize repayment performance by district"))
        self.assertEqual("completed", result.status)
        self.assertEqual("deterministic_fallback", result.result["strategy"])
        self.assertEqual(["kpi", "bar", "table"], [v["type"] for v in result.result["visualizations"]])
        self.assertTrue(any(e.kind == "visualization_agent" for e in result.evidence))
    def test_dashboard_fallback_layout_and_provenance(self):
        result = self.orchestrator.execute(AgentRequest(agent="dashboard",
            objective="Build repayment performance dashboard by district"))
        self.assertEqual("completed", result.status)
        self.assertEqual(3, len(result.result["dashboard"]["layout"]))
        self.assertTrue(any(e.kind == "dashboard_agent" for e in result.evidence))
    def test_routes_and_permissions(self):
        self.assertEqual("dashboard", self.orchestrator.route(AgentRequest(objective="Build a district dashboard")))
        denied = self.orchestrator.execute(AgentRequest(agent="visualization", objective="district chart",
            permissions=Permissions(can_build_visualizations=False)))
        self.assertEqual("failed", denied.status)


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.source, self.visuals, self.dashboard = artifacts()
        self.adapter = SupersetAdapter(Settings(superset_password="secret"), Mock())
    def test_dry_run_dataset_chart_dashboard_layout_filter_mapping(self):
        plan = self.adapter.plan(self.dashboard)
        self.assertEqual("dry_run", plan["mode"])
        self.assertEqual(5, len(plan["actions"]))
        self.assertEqual("district", plan["filters"][0]["field"])
        self.assertEqual(12, plan["layout"][0]["width"])
    def test_chart_type_mappings(self):
        expected = {"kpi": "big_number_total", "table": "table", "bar": "echarts_timeseries_bar",
                    "line": "echarts_timeseries_line", "scatter": "bubble"}
        self.assertEqual(expected, SUPERSET_TYPES)
        for visual in self.visuals:
            payload = self.adapter.chart_payload(visual, 7)
            self.assertEqual(expected[visual.type], payload["viz_type"])
            self.assertEqual(7, payload["datasource_id"])
    def test_stable_identity(self):
        self.assertEqual(self.adapter.plan(self.dashboard), self.adapter.plan(self.dashboard))
    def test_dashboard_mapping_and_chart_relationships(self):
        self.adapter._get = Mock(return_value=[{"id": 6}])
        self.adapter._write = Mock(return_value=6)
        result = self.adapter._upsert_dashboard(
            self.dashboard, {"kpi": 62, "bar": 63, "table": 64},
            {self.source.id: 43}, {})
        self.assertEqual(6, result)
        payload = self.adapter._write.call_args.args[3]
        self.assertNotIn("charts", payload)
        self.adapter._write.reset_mock()
        self.adapter._associate_charts([62, 63, 64], 6, {})
        self.assertEqual(3, self.adapter._write.call_count)
        self.assertEqual({"dashboards": [6]}, self.adapter._write.call_args.args[3])
    def test_publish_permission_denied(self):
        with self.assertRaises(AdapterPermissionDenied):
            self.adapter.publish(self.dashboard, Permissions(can_publish_bi_assets=False))
    def test_publish_create_and_partial_failure_propagation(self):
        adapter = SupersetAdapter(Settings(superset_password="x"), Mock())
        adapter._login = Mock(return_value={})
        adapter._database_id = Mock(return_value=1)
        adapter._upsert_dataset = Mock(return_value=10)
        adapter._upsert_chart = Mock(side_effect=[20, RuntimeError("API down")])
        with self.assertRaisesRegex(AdapterError, "failed after"):
            adapter.publish(self.dashboard, Permissions(can_publish_bi_assets=True))
    def test_publish_reuses_one_dataset(self):
        adapter = SupersetAdapter(Settings(superset_password="x"), Mock())
        adapter._login = Mock(return_value={}); adapter._database_id = Mock(return_value=1)
        adapter._upsert_dataset = Mock(return_value=10); adapter._upsert_chart = Mock(side_effect=[20,21,22])
        adapter._upsert_dashboard = Mock(return_value=30)
        adapter._associate_charts = Mock()
        result = adapter.publish(self.dashboard, Permissions(can_publish_bi_assets=True))
        self.assertEqual(1, adapter._upsert_dataset.call_count)
        self.assertEqual(30, result["artifacts"][-1]["superset_id"])


class Phase3ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import api
        api.orchestrator = Orchestrator(Settings(), Gateway())
        cls.client = LocalClient(api.app)
    def test_visualize_and_dashboard_dry_run(self):
        self.assertEqual(200, self.client.post("/agents/visualize", json={
            "objective": "Visualize repayment performance by district"}).status_code)
        response = self.client.post("/agents/dashboard", json={
            "objective": "Build repayment performance dashboard by district", "publish": False})
        self.assertEqual(200, response.status_code, response.text)
        self.assertEqual("dry_run", response.json()["superset"]["mode"])
    def test_publish_requires_permission(self):
        _, _, dashboard = artifacts()
        response = self.client.post("/publish/superset", json={
            "dashboard": dashboard.model_dump(mode="json"), "publish": True})
        self.assertEqual(502, response.status_code)
        self.assertEqual("AdapterPermissionDenied", response.json()["error"]["category"])
    def test_phase1_endpoint_compatible(self):
        self.assertEqual(200, self.client.post("/agents/query", json={
            "objective": "What tables contain repayment information?"}).status_code)


if __name__ == "__main__": unittest.main()
