import sys
import unittest
from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
SERVICE = ROOT / "services" / "agent-api"
if not SERVICE.exists() and Path("/app/config.py").exists(): SERVICE = Path("/app")
sys.path.insert(0, str(SERVICE))

from builder_models import (DashboardBuildRequest, DashboardFilter, DataSourceBuildRequest,
                            Encoding, LayoutItem, VisualizationBuildRequest)
from builders import (BuilderValidationError, DashboardBuilder, DataSourceBuilder,
                      VisualizationBuilder, recommend_visualizations)
from config import Settings
from models import AnalyticalRequest, Measure, Permissions
from orchestrator import Orchestrator
from tools import PermissionDenied, ToolRegistry


DATASET = "iceberg.consumption.cns_pdm_local_government_performance"
COLUMNS = [
    ["district", "varchar", 1], ["region", "varchar", 2],
    ["principal_repayment_rate", "double", 3], ["loan_count", "bigint", 4],
    ["reporting_month", "date", 5], ["label", "varchar", 6],
]


class Gateway:
    def execute(self, sql):
        lowered = sql.lower()
        if "information_schema.columns" in lowered and "where table_schema" in lowered:
            if "missing_table" in lowered: rows = []
            else: rows = COLUMNS
            return ["column_name", "data_type", "ordinal_position"], rows, "q-description"
        if "information_schema.columns" in lowered:
            rows = [["consumption", "cns_pdm_local_government_performance", *row[:2]] for row in COLUMNS]
            return ["table_schema", "table_name", "column_name", "data_type"], rows, "q-metadata"
        if "avg(" in lowered:
            return ["district", "repayment_performance"], [["Kabale", .24]], "q-analytics"
        return ["healthy"], [[1]], "q-health"
    def health(self): return {"status": "healthy", "query_id": "q-health"}


def semantic(dataset=DATASET, dimension="district", measure="principal_repayment_rate", aggregation="avg"):
    return AnalyticalRequest(datasets=[dataset], dimensions=[dimension],
        measures=[Measure(name="repayment", expression=measure, aggregation=aggregation)],
        group_by=[dimension], limit=20)


def source_spec(**kwargs):
    request = DataSourceBuildRequest(semantic_request=semantic(**kwargs),
                                     semantic_request_id="semantic-1", query_id="q-1")
    return DataSourceBuilder().build(request, ToolRegistry(Gateway(), Settings()))


def visualization(kind="bar", identifier="ranking", source=None):
    if source is None and kind == "scatter":
        scatter_semantic = semantic()
        scatter_semantic.measures.append(Measure(name="loans", expression="loan_count", aggregation="sum"))
        source = DataSourceBuilder().build(DataSourceBuildRequest(semantic_request=scatter_semantic),
                                           ToolRegistry(Gateway(), Settings()))
    source = source or source_spec()
    if kind == "kpi": enc = [Encoding(channel="value", field="principal_repayment_rate", role="measure", aggregation="avg")]
    elif kind == "scatter": enc = [Encoding(channel="x", field="principal_repayment_rate", role="measure"),
                                    Encoding(channel="y", field="loan_count", role="measure")]
    elif kind == "table": enc = [Encoding(channel="column", field="district", role="dimension")]
    elif kind == "line": enc = [Encoding(channel="x", field="district", role="dimension"),
                                 Encoding(channel="y", field="principal_repayment_rate", role="measure")]
    else: enc = [Encoding(channel="x", field="district", role="dimension"),
                 Encoding(channel="y", field="principal_repayment_rate", role="measure")]
    return VisualizationBuilder().build(VisualizationBuildRequest(
        id=identifier, type=kind, title=identifier, data_source=source, encoding=enc))


class DataSourceBuilderTests(unittest.TestCase):
    def test_valid_dataset_dimension_and_measure(self):
        result = source_spec()
        self.assertEqual(DATASET, result.dataset)
        self.assertEqual(["dimension", "measure"], [field.role for field in result.fields])
        self.assertTrue(result.read_only)
        self.assertEqual("semantic-1", result.semantic_request_id)
    def test_nonexistent_table_and_column_rejected(self):
        with self.assertRaises(BuilderValidationError): source_spec(dataset="iceberg.consumption.missing_table")
        with self.assertRaises(BuilderValidationError): source_spec(dimension="missing_column")
    def test_invalid_aggregation_and_non_numeric_measure_rejected(self):
        with self.assertRaises(BuilderValidationError): source_spec(aggregation="median")
        with self.assertRaises(BuilderValidationError): source_spec(measure="label", aggregation="avg")
    def test_inferred_join_label_and_permission(self):
        from builder_models import JoinSpec
        request = DataSourceBuildRequest(semantic_request=semantic(), joins=[JoinSpec(
            left_dataset=DATASET, right_dataset=DATASET, left_field="district", right_field="district")])
        result = DataSourceBuilder().build(request, ToolRegistry(Gateway(), Settings()))
        self.assertEqual("inferred", result.joins[0].relationship)
        self.assertTrue(result.warnings)
        request.permissions.can_execute_read_queries = False
        with self.assertRaises(PermissionDenied):
            DataSourceBuilder().build(request, ToolRegistry(Gateway(), Settings()))


class VisualizationBuilderTests(unittest.TestCase):
    def test_supported_types(self):
        for kind in ("kpi", "bar", "line", "scatter", "table"):
            with self.subTest(kind=kind): self.assertEqual(kind, visualization(kind).type)
    def test_unknown_type_rejected_by_model(self):
        with self.assertRaises(ValidationError):
            VisualizationBuildRequest(id="x", type="pie", title="x", data_source=source_spec(), encoding=[])
    def test_permissions_rejected(self):
        with self.assertRaises(PermissionDenied):
            VisualizationBuilder().build(VisualizationBuildRequest(id="denied", type="table",
                title="denied", data_source=source_spec(),
                encoding=[Encoding(channel="column", field="district", role="dimension")],
                permissions=Permissions(can_execute_read_queries=False)))
    def test_missing_field_and_role_rejected(self):
        with self.assertRaises(BuilderValidationError):
            VisualizationBuilder().build(VisualizationBuildRequest(id="bad", type="bar", title="bad",
                data_source=source_spec(), encoding=[Encoding(channel="x", field="missing", role="dimension")]))
        with self.assertRaises(BuilderValidationError):
            VisualizationBuilder().build(VisualizationBuildRequest(id="bad", type="kpi", title="bad",
                data_source=source_spec(), encoding=[Encoding(channel="value", field="district", role="measure")]))


class DashboardBuilderTests(unittest.TestCase):
    def test_valid_dashboard_and_provenance(self):
        chart = visualization()
        result = DashboardBuilder().build(DashboardBuildRequest(title="Repayment", visualizations=[chart],
            filters=[DashboardFilter(field="district", data_source_id=chart.data_source.id)],
            layout=[LayoutItem(visualization_id=chart.id, x=0, y=0, width=12, height=4)]))
        self.assertEqual("returned_only", result.persistence)
        self.assertTrue(result.evidence)
    def test_missing_visualization_invalid_filter_and_duplicate_ids(self):
        chart = visualization()
        with self.assertRaises(BuilderValidationError):
            DashboardBuilder().build(DashboardBuildRequest(title="x", visualizations=[chart],
                layout=[LayoutItem(visualization_id="missing", x=0, y=0, width=12, height=4)]))
        with self.assertRaises(BuilderValidationError):
            DashboardBuilder().build(DashboardBuildRequest(title="x", visualizations=[chart],
                filters=[DashboardFilter(field="missing", data_source_id=chart.data_source.id)],
                layout=[LayoutItem(visualization_id=chart.id, x=0, y=0, width=12, height=4)]))
        with self.assertRaises(BuilderValidationError):
            DashboardBuilder().build(DashboardBuildRequest(title="x", visualizations=[chart, chart],
                layout=[LayoutItem(visualization_id=chart.id, x=0, y=0, width=12, height=4)]))
    def test_invalid_layout_rejected(self):
        with self.assertRaises(ValidationError): LayoutItem(visualization_id="x", x=8, y=0, width=8, height=4)


class IntegrationTests(unittest.TestCase):
    def setUp(self): self.orchestrator = Orchestrator(Settings(), Gateway())
    def test_analytics_to_all_builders(self):
        from models import AgentRequest
        analytics = self.orchestrator.execute(AgentRequest(objective="repayment performance by district"))
        self.assertEqual("completed", analytics.status)
        self.assertIn("data_source", analytics.result)
        source = source_spec()
        charts = [VisualizationBuilder().build(item) for item in recommend_visualizations(source, "District repayment")]
        dashboard = DashboardBuilder().build(DashboardBuildRequest(title="District repayment",
            visualizations=charts, layout=[LayoutItem(visualization_id=item.id, x=0, y=i*4,
            width=12, height=4) for i, item in enumerate(charts)]))
        self.assertEqual(3, len(dashboard.visualizations))
    def test_builder_permission_cannot_bypass_phase1(self):
        request = DataSourceBuildRequest(semantic_request=semantic(), permissions=Permissions(
            can_discover_metadata=True, can_execute_read_queries=False))
        with self.assertRaises(PermissionDenied):
            ToolRegistry(Gateway(), Settings()).build_data_source(request)
    def test_phase1_and_builder_api_compatibility(self):
        import api
        api.orchestrator = self.orchestrator
        client = TestClient(api.app)
        self.assertEqual(200, client.post("/agents/query", json={
            "objective": "What tables contain repayment information?"}).status_code)
        response = client.post("/agents/build", json={
            "objective": "What is repayment performance by district?"})
        self.assertEqual(200, response.status_code, response.text)
        self.assertEqual(3, len(response.json()["visualizations"]))


if __name__ == "__main__": unittest.main()
