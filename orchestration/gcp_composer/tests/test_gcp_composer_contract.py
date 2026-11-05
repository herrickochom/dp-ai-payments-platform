"""Offline tests for the isolated Composer contract."""

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

DAG_PATH = (
    ROOT
    / "orchestration/gcp_composer/dags"
    / "pdm_gcp_payments_pipeline.py"
)

MODULE_PATH = (
    ROOT
    / "infrastructure/terraform/gcp/modules/composer_environment"
)

DEV_PATH = (
    ROOT
    / "infrastructure/terraform/gcp/environments/dev"
)


class ComposerContractTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.source = DAG_PATH.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_dag_has_no_schedule(self):
        self.assertIn("schedule=None", self.source)

    def test_dag_disables_retries(self):
        self.assertIn('"retries": 0', self.source)

    def test_dag_limits_concurrency(self):
        self.assertIn("max_active_runs=1", self.source)
        self.assertIn("max_active_tasks=1", self.source)

    def test_execution_fails_closed(self):
        self.assertIn("raise RuntimeError(BLOCKER)", self.source)

    def test_no_subprocess_or_docker_execution(self):
        self.assertNotIn("subprocess", self.source)
        self.assertNotIn("docker compose", self.source)

    def test_no_dataflow_submission(self):
        self.assertNotIn("DataflowStart", self.source)
        self.assertNotIn("DataflowCreate", self.source)
        self.assertNotIn("googleapiclient", self.source)

    def test_required_tasks_exist(self):
        expected = {
            "admission_gate",
            "verify_bronze_manifest",
            "submit_dataflow",
            "observe_dataflow",
            "verify_bigquery_landing",
            "record_acceptance",
        }
        actual = {
            node.name
            for node in ast.walk(self.tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertTrue(expected.issubset(actual))

    def test_composer_module_is_isolated(self):
        references = [
            path
            for path in DEV_PATH.glob("*.tf")
            if "composer_environment"
            in path.read_text(encoding="utf-8")
        ]
        self.assertEqual(references, [])

    def test_composer_has_private_configuration(self):
        source = (MODULE_PATH / "main.tf").read_text(
            encoding="utf-8"
        )
        self.assertIn("enable_private_environment = true", source)
        self.assertIn("prevent_destroy = true", source)
        self.assertIn('deletion_policy = "PREVENT"', source)

    def test_dag_is_syntactically_valid(self):
        compile(self.source, str(DAG_PATH), "exec")


if __name__ == "__main__":
    unittest.main()
