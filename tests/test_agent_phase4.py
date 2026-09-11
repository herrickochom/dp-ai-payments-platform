import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVICE = ROOT / "services" / "agent-api"
if not SERVICE.exists() and Path("/app/config.py").exists():
    ROOT = Path("/")
    SERVICE = Path("/app")
sys.path.insert(0, str(SERVICE))

from config import Settings
from data_quality import DataQualityEngine
from models import AgentRequest, Permissions
from orchestrator import Orchestrator
from quality_models import DQProfileRequest, DQRule
from quality_rules import DQRuleCatalogue
from tools import PermissionDenied, QueryValidationError, ToolRegistry, validate_read_query


class Phase4Gateway:
    def __init__(self, checked=10, failed=0, no_time=False, error=False):
        self.checked, self.failed, self.no_time, self.error = checked, failed, no_time, error
        self.sql = []

    def health(self):
        return {"status": "healthy", "query_id": "q-health"}

    def execute(self, sql):
        self.sql.append(sql)
        lowered = sql.lower()
        if self.error and "information_schema" not in lowered:
            raise ConnectionError("query failed")
        if "information_schema.columns" in lowered and "where table_schema" not in lowered:
            rows = []
            for name, kind in self._columns():
                rows.append(["consumption", "quality_target", name, kind])
            return ["table_schema", "table_name", "column_name", "data_type"], rows, "q-search"
        if "information_schema.columns" in lowered:
            rows = [[name, kind, index] for index, (name, kind) in enumerate(self._columns(), 1)]
            return ["column_name", "data_type", "ordinal_position"], rows, "q-describe"
        if "having count(*) > 1" in lowered:
            return ["failed_value", "duplicate_count"], [["secret-loan", 2]], "q-sample"
        if "null_count_0" in lowered:
            return ["row_count", "null_count_0", "distinct_count_0", "min_value_0", "max_value_0"], [[10, 1, 9, 0.1, 0.9]], "q-profile"
        if "with recent_periods" in lowered:
            return ["period", "district", "value"], [
                ["2026-08-01", "A", 0.4], ["2026-08-01", "B", 0.6],
                ["2026-07-01", "A", 0.8], ["2026-07-01", "B", 0.6],
            ], "q-insight"
        if "group by entity_type, rule_code" in lowered:
            return ["entity_type", "rule_code", "checked_rows", "failed_rows"], [
                ["LOAN", "REPAID_COMPONENTS_RECONCILE", 10, self.failed]], "q-summary"
        if "avg(" in lowered and "group by" in lowered:
            return ["district", "repayment_performance"], [["A", 0.4], ["B", 0.8]], "q-ranking"
        if "latest_value" in lowered:
            return ["checked_rows", "failed_rows", "latest_value"], [[self.checked, self.failed, "2026-08-01"]], "q-dq"
        if "checked_rows" in lowered and "failed_rows" in lowered:
            return ["checked_rows", "failed_rows"], [[self.checked, self.failed]], "q-dq"
        return ["healthy"], [[1]], "q"

    def _columns(self):
        columns = [("loan_id", "varchar"), ("beneficiary_id", "varchar"),
                   ("status", "varchar"), ("principal_repayment_rate", "double"),
                   ("event_date", "date"), ("district", "varchar"),
                   ("entity_type", "varchar"), ("rule_code", "varchar"),
                   ("rule_passed", "boolean")]
        if not self.no_time:
            columns.append(("reporting_month", "date"))
        return columns


def rule(kind, **parameters):
    field = None if kind == "volume" else parameters.pop("field", {
        "not_null": "loan_id", "unique": "loan_id", "accepted_values": "status",
        "range": "principal_repayment_rate", "referential_integrity": "beneficiary_id",
        "freshness": "event_date"}.get(kind))
    return DQRule(rule_id=f"test.{kind}", dataset="iceberg.consumption.quality_target",
                  rule_type=kind, field=field, severity="high", parameters=parameters,
                  source="phase4_config", provenance="test configuration")


class DataQualityTests(unittest.TestCase):
    def registry(self, failed=0, **kwargs):
        return ToolRegistry(Phase4Gateway(failed=failed, **kwargs),
                            Settings(max_tool_calls=30, dq_max_sample_rows=5))

    def test_not_null_pass_and_fail_with_recommendation_and_evidence(self):
        passed = DataQualityEngine(self.registry()).run_rule(rule("not_null"), Permissions())
        failed = DataQualityEngine(self.registry(2)).run_rule(rule("not_null"), Permissions())
        self.assertEqual("passed", passed.status)
        self.assertEqual(("failed", 0.2), (failed.status, failed.failure_rate))
        self.assertIn("upstream", failed.recommendation)
        self.assertTrue(any(item.kind == "query" for item in failed.evidence))

    def test_duplicate_and_unique_detection(self):
        finding = DataQualityEngine(self.registry(3)).run_rule(rule("unique"), Permissions())
        self.assertEqual(3, finding.failed_rows)
        self.assertEqual("unique", finding.rule_type)

    def test_range_and_accepted_values(self):
        for item in (rule("range", min=0, max=1),
                     rule("accepted_values", values=["OPEN", "CLOSED"])):
            finding = DataQualityEngine(self.registry(1)).run_rule(item, Permissions())
            self.assertEqual("failed", finding.status)

    def test_referential_integrity_freshness_and_volume(self):
        rules = [rule("referential_integrity", reference_dataset="iceberg.consumption.quality_target",
                      reference_field="beneficiary_id"),
                 rule("freshness", max_age_days=3), rule("volume", min_rows=20)]
        for item in rules:
            self.assertEqual("failed", DataQualityEngine(self.registry(1)).run_rule(item, Permissions()).status)

    def test_dbt_rule_provenance_and_no_fabricated_rule(self):
        models_path = (ROOT / "transform" / "dbt" / "models") if (ROOT / "transform").exists() else Path("/app/dbt-models")
        catalogue = DQRuleCatalogue(str(models_path))
        rules = catalogue.list_rules("iceberg.silver.slv_pdm_loans")
        self.assertTrue(any(item.rule_type == "not_null" and item.field == "loan_id" for item in rules))
        self.assertTrue(all(item.source == "dbt_test" and item.provenance for item in rules))
        response = Orchestrator(Settings(), Phase4Gateway()).execute(AgentRequest(
            agent="data_quality", objective="check a fabricated thing",
            context={"dataset": "iceberg.consumption.no_such_rule"}))
        self.assertEqual([], response.result["findings"])
        self.assertIn("no rule was invented", response.result["explanation"].lower())

    def test_inferred_rule_is_labelled_and_lower_confidence(self):
        inferred = rule("range", min=0, max=1).model_copy(update={"source": "inferred"})
        finding = DataQualityEngine(self.registry()).run_rule(inferred, Permissions())
        self.assertEqual("inferred", finding.rule_source)
        self.assertLess(finding.confidence, 1)

    def test_permissions_and_no_write_execution(self):
        with self.assertRaises(PermissionDenied):
            DataQualityEngine(self.registry()).run_rule(rule("not_null"), Permissions(
                can_run_data_quality_checks=False))
        with self.assertRaises(PermissionDenied):
            DataQualityEngine(self.registry()).run_rule(rule("not_null"), Permissions(
                can_execute_read_queries=False))
        with self.assertRaises(ValueError):
            DQRule(rule_id="bad", dataset="iceberg.x.y", rule_type="custom_read_only_check",
                   source="phase4_config", parameters={"sql": "DELETE FROM x"})
        with self.assertRaises(QueryValidationError):
            validate_read_query("UPDATE x SET value = 1", 5, 1000)

    def test_samples_are_permission_gated_bounded_and_masked(self):
        hidden = DataQualityEngine(self.registry(2)).run_rule(rule("unique"), Permissions())
        shown = DataQualityEngine(self.registry(2)).run_rule(rule("unique"), Permissions(
            can_view_data_quality_samples=True))
        self.assertEqual([], hidden.sample)
        self.assertLessEqual(len(shown.sample), 5)
        self.assertTrue(shown.sample[0]["failed_value"].startswith("masked:"))

    def test_bounded_aggregate_profile(self):
        result = DataQualityEngine(self.registry()).profile(DQProfileRequest(
            dataset="iceberg.consumption.quality_target",
            fields=["principal_repayment_rate"]))
        self.assertTrue(result["aggregate_only"])
        self.assertEqual(0.1, result["fields"][0]["null_rate"])

    def test_existing_dbt_financial_and_reconciliation_summary_is_reused(self):
        result = DataQualityEngine(self.registry(2)).existing_results_summary(Permissions())
        self.assertEqual("existing_dbt_model", result["source"])
        self.assertEqual(2, result["checks"][0]["failed_rows"])
        self.assertTrue(result["aggregate_only"])


class InsightTests(unittest.TestCase):
    def orchestrator(self, **kwargs):
        return Orchestrator(Settings(max_tool_calls=30, insight_change_threshold=0.2),
                            Phase4Gateway(**kwargs))

    def request(self, **context):
        base = {"dataset": "iceberg.consumption.quality_target",
                "metric": "principal_repayment_rate", "dimension": "district",
                "time_field": "reporting_month"}
        base.update(context)
        return AgentRequest(agent="insight", objective="What changed compared with the previous period?",
                            context=base)

    def test_current_previous_percentage_threshold_anomaly_and_ranking_movement(self):
        response = self.orchestrator().execute(self.request(threshold=0.2))
        self.assertEqual("completed", response.status)
        types = {item["type"] for item in response.result["insights"]}
        self.assertTrue({"metric_change", "anomaly", "ranking_movement"}.issubset(types))
        change = response.result["insights"][0]
        self.assertAlmostEqual(-0.2, change["absolute_change"])
        self.assertAlmostEqual(-2 / 7, change["relative_change"])
        self.assertEqual("2026-08-01", response.result["comparison"]["current_period"])

    def test_no_temporal_field_is_clear_limitation(self):
        request = self.request(time_field=None)
        response = self.orchestrator(no_time=True).execute(request)
        self.assertEqual("limitation", response.result["insights"][0]["type"])
        self.assertIn("cannot support", response.result["insights"][0]["summary"])

    def test_query_error_and_permissions_propagate(self):
        failed = self.orchestrator(error=True).execute(self.request())
        self.assertEqual("failed", failed.status)
        denied = self.orchestrator().execute(self.request().model_copy(update={
            "permissions": Permissions(can_generate_insights=False)}))
        self.assertEqual("failed", denied.status)

    def test_causal_language_is_conservative_and_provenance_preserved(self):
        response = self.orchestrator().execute(self.request())
        summaries = " ".join(item["summary"] for item in response.result["insights"]).lower()
        self.assertIn("no causal", summaries)
        self.assertNotIn(" caused ", summaries)
        self.assertTrue(any(item.kind == "query" and item.reference == "q-insight"
                            for item in response.evidence))

    def test_static_ranking_and_zscore_method(self):
        request = AgentRequest(agent="insight", objective="repayment differences across districts",
            context={"dataset": "iceberg.consumption.quality_target",
                     "metric": "principal_repayment_rate", "dimension": "district"})
        response = self.orchestrator().execute(request)
        insight = response.result["insights"][0]
        self.assertEqual("ranking", insight["type"])
        self.assertEqual("population z-score >= 2.0", insight["details"]["anomaly_method"])


if __name__ == "__main__":
    unittest.main()
