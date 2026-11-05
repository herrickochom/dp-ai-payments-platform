"""Offline integration tests for the synthetic payment workflow."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


RUNTIME = Path(__file__).resolve().parents[1] / "runtime"
sys.path.insert(0, str(RUNTIME))

spec = importlib.util.spec_from_file_location(
    "local_integration",
    RUNTIME / "local_integration.py",
)
integration = importlib.util.module_from_spec(spec)
spec.loader.exec_module(integration)


class LocalIntegrationTests(unittest.TestCase):

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "payments.sqlite"
        self.simulator = integration.LocalSimulator(self.database)
        self.manifest = integration.example_manifest()

    def test_successful_acceptance(self):
        result = self.simulator.execute(self.manifest)
        self.assertEqual(result["state"], "ACCEPTED")
        self.assertEqual(result["attempts"], 1)
        self.assertEqual(result["evidence"]["object_count"], 2)

    def test_duplicate_does_not_reexecute(self):
        first = self.simulator.execute(self.manifest)
        second = self.simulator.execute(self.manifest)
        self.assertEqual(first["execution_id"], second["execution_id"])
        self.assertEqual(second["decision"], "ALREADY_ACCEPTED")
        self.assertEqual(second["attempts"], 1)

    def test_failure_and_recovery(self):
        failed = self.simulator.execute(
            self.manifest, fail_before_acceptance=True
        )
        self.assertEqual(failed["state"], "FAILED")

        recovered = self.simulator.recover_failed(
            failed["execution_id"]
        )
        self.assertEqual(recovered["state"], "ACCEPTED")
        self.assertEqual(recovered["attempts"], 2)

    def test_recovery_is_persistent(self):
        failed = self.simulator.execute(
            self.manifest, fail_before_acceptance=True
        )
        restarted = integration.LocalSimulator(self.database)
        recovered = restarted.recover_failed(
            failed["execution_id"]
        )
        self.assertEqual(recovered["state"], "ACCEPTED")

    def test_recovery_rejects_accepted_run(self):
        accepted = self.simulator.execute(self.manifest)
        with self.assertRaises(ValueError):
            self.simulator.recover_failed(
                accepted["execution_id"]
            )

    def test_different_generation_is_distinct(self):
        first = self.simulator.execute(self.manifest)
        changed = json.loads(json.dumps(self.manifest))
        changed["objects"][0]["generation"] = "99999"
        second = self.simulator.execute(changed)
        self.assertNotEqual(
            first["execution_id"],
            second["execution_id"],
        )

    def test_invalid_manifest_is_rejected(self):
        self.manifest["objects"][0]["uri"] = (
            "gs://example-bucket/payments/*.json"
        )
        with self.assertRaises(ValueError):
            self.simulator.execute(self.manifest)

    def test_acceptance_evidence_is_persistent(self):
        accepted = self.simulator.execute(self.manifest)
        restarted = integration.LocalSimulator(self.database)
        stored = restarted.inspect(accepted["execution_id"])
        self.assertEqual(
            stored["evidence"]["manifest_sha256"],
            accepted["execution_id"],
        )

    def test_admission_is_idempotent(self):
        first = self.simulator.admit(self.manifest)
        second = self.simulator.admit(self.manifest)
        self.assertEqual(first["decision"], "NEW")
        self.assertEqual(second["decision"], "DUPLICATE")

    def test_no_external_cloud_calls(self):
        source = (RUNTIME / "local_integration.py").read_text()
        for prohibited in (
            "google.cloud",
            "googleapiclient",
            "subprocess",
            "terraform apply",
        ):
            self.assertNotIn(prohibited, source)


if __name__ == "__main__":
    unittest.main()
