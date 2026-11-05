"""Standard-library-only tests. No cloud operations."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


controller = load_module(
    "bounded_controller",
    HERE / "runtime/bounded_controller.py",
)
gate = load_module(
    "deployment_gate",
    HERE / "runtime/deployment_gate.py",
)


def manifest():
    return {
        "schema_version": 1,
        "source": "payments",
        "objects": [{
            "uri": "gs://example-bucket/payments/batch-001.json",
            "generation": "12345",
            "sha256": "a" * 64,
        }],
    }


class Stage5Tests(unittest.TestCase):

    def test_valid_manifest(self):
        self.assertEqual(
            controller.validate_manifest(manifest())["source"],
            "payments",
        )

    def test_rejects_wildcard(self):
        m = manifest()
        m["objects"][0]["uri"] = "gs://bucket/*.json"
        with self.assertRaises(controller.ContractError):
            controller.validate_manifest(m)

    def test_rejects_empty_objects(self):
        m = manifest()
        m["objects"] = []
        with self.assertRaises(controller.ContractError):
            controller.validate_manifest(m)

    def test_rejects_duplicate_objects(self):
        m = manifest()
        m["objects"].append(dict(m["objects"][0]))
        with self.assertRaises(controller.ContractError):
            controller.validate_manifest(m)

    def test_rejects_invalid_generation(self):
        m = manifest()
        m["objects"][0]["generation"] = "latest"
        with self.assertRaises(controller.ContractError):
            controller.validate_manifest(m)

    def test_rejects_invalid_checksum(self):
        m = manifest()
        m["objects"][0]["sha256"] = "bad"
        with self.assertRaises(controller.ContractError):
            controller.validate_manifest(m)

    def test_deterministic_execution_id(self):
        self.assertEqual(
            controller.execution_id(manifest()),
            controller.execution_id(manifest()),
        )

    def test_object_order_independent(self):
        m = manifest()
        second = dict(m["objects"][0])
        second["uri"] = "gs://example-bucket/payments/batch-002.json"
        m["objects"].append(second)

        reversed_manifest = dict(m)
        reversed_manifest["objects"] = list(reversed(m["objects"]))

        self.assertEqual(
            controller.execution_id(m),
            controller.execution_id(reversed_manifest),
        )

    def test_durable_duplicate_detection(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.sqlite"

            first = controller.ExecutionLedger(path).admit(manifest())
            second = controller.ExecutionLedger(path).admit(manifest())

            self.assertEqual(first["decision"], "NEW")
            self.assertEqual(second["decision"], "DUPLICATE")
            self.assertEqual(first["status"], "BLOCKED")

    def test_generation_change_creates_new_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = controller.ExecutionLedger(
                Path(tmp) / "ledger.sqlite"
            )
            first = ledger.admit(manifest())

            m = manifest()
            m["objects"][0]["generation"] = "12346"
            second = ledger.admit(m)

            self.assertNotEqual(
                first["execution_id"],
                second["execution_id"],
            )

    def test_invalid_manifest_does_not_create_ledger(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.sqlite"
            m = manifest()
            m["objects"] = []

            with self.assertRaises(controller.ContractError):
                controller.ExecutionLedger(path).admit(m)

            self.assertFalse(path.exists())


class Stage6Tests(unittest.TestCase):

    def test_default_deployment_blocked(self):
        result = gate.assess()
        self.assertFalse(result["ready"])
        self.assertIn("deployment_authorised", result["missing"])

    def test_partial_approvals_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "approvals.json"
            path.write_text(
                json.dumps({"iam_review_approved": True}),
                encoding="utf-8",
            )
            result = gate.assess(path)
            self.assertFalse(result["ready"])

    def test_invalid_approvals_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "approvals.json"
            path.write_text("invalid", encoding="utf-8")
            self.assertFalse(gate.assess(path)["ready"])

    def test_assessment_never_deploys(self):
        result = gate.assess()
        self.assertTrue(result["assessment_only"])
        self.assertFalse(result["deployment_performed"])


if __name__ == "__main__":
    unittest.main()
