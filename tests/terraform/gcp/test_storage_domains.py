"""GCP-006C: Governed storage domain regression tests."""

import unittest
from collections import Counter
from pathlib import Path

import hcl2
import yaml


ROOT = Path(__file__).resolve().parents[4]
ENV = ROOT / "infrastructure/terraform/gcp/environments/dev"
POLICY = ROOT / "platform/config/governance/gcp_retention_schedule.yaml"


def normalise(value):
    """Remove surrounding HCL string-literal quotation marks."""
    if isinstance(value, str):
        if len(value) >= 2 and value[0] == value[-1] == '"':
            return value[1:-1]
    return value


class GovernedStorageDomainTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with POLICY.open(encoding="utf-8") as stream:
            cls.policy = yaml.safe_load(stream)

        with (ENV / "storage_domains.tf").open(
            encoding="utf-8"
        ) as stream:
            cls.config = hcl2.load(stream)

        with (ENV / "storage_variables.tf").open(
            encoding="utf-8"
        ) as stream:
            cls.variables = hcl2.load(stream)

        cls.locals = cls.config["locals"][0]

        cls.domains = {
            name: {
                **domain,
                "storage_class": normalise(domain["storage_class"]),
                "datasets": [
                    normalise(dataset)
                    for dataset in domain["datasets"]
                ],
            }
            for name, domain in cls.locals["storage_domains"].items()
        }

        cls.protected = [
            normalise(category)
            for category in cls.locals["protected_storage_categories"]
        ]

    def test_all_datasets_accounted_for_once(self):
        assigned = [
            dataset
            for domain in self.domains.values()
            for dataset in domain["datasets"]
        ]

        counts = Counter(assigned + self.protected)

        self.assertEqual(
            set(counts),
            set(self.policy["datasets"]),
        )

        self.assertTrue(
            all(count == 1 for count in counts.values())
        )

    def test_storage_classes_match_policy(self):
        for domain_name, domain in self.domains.items():
            for dataset in domain["datasets"]:
                with self.subTest(
                    domain=domain_name,
                    dataset=dataset,
                ):
                    expected = self.policy["datasets"][dataset]["storage_class"]
                    self.assertEqual(domain["storage_class"], expected)

    def test_protected_categories(self):
        self.assertEqual(
            set(self.protected),
            {"iceberg_tables", "flink_recovery_state"},
        )

        assigned = {
            dataset
            for domain in self.domains.values()
            for dataset in domain["datasets"]
        }

        self.assertTrue(set(self.protected).isdisjoint(assigned))

    def test_deployment_disabled_by_default(self):
        variables = {
            normalise(name): details
            for item in self.variables["variable"]
            for name, details in item.items()
        }

        self.assertFalse(
            variables["enable_governed_storage_deployment"]["default"]
        )
        self.assertFalse(
            variables["governance_approved"]["default"]
        )

    def test_module_deployment_gate(self):
        modules = self.config["module"]

        module = next(
            details
            for item in modules
            for name, details in item.items()
            if normalise(name) == "governed_storage_domains"
        )

        expression = str(module["for_each"])

        self.assertIn(
            "var.enable_governed_storage_deployment",
            expression,
        )
        self.assertIn("local.storage_domains", expression)

    def test_governance_remains_proposed(self):
        self.assertEqual(self.policy["status"], "proposed")

        for dataset in self.policy["datasets"].values():
            self.assertEqual(dataset["approval_status"], "proposed")
            self.assertIs(dataset["automatic_deletion"], False)


    def test_bucket_security_controls(self):
        """Verify the module's mandatory security safeguards."""
        module_path = (
            ROOT
            / "infrastructure/terraform/gcp/modules/governed_gcs_bucket/main.tf"
        )

        with module_path.open(encoding="utf-8") as stream:
            config = hcl2.load(stream)

        resources = [
            details
            for block in config["resource"]
            for resource_type, instances in block.items()
            if normalise(resource_type) == "google_storage_bucket"
            for name, details in instances.items()
            if normalise(name) == "governed"
        ]

        self.assertEqual(len(resources), 1)
        bucket = resources[0]

        self.assertIs(bucket["uniform_bucket_level_access"], True)
        self.assertEqual(
            normalise(bucket["public_access_prevention"]),
            "enforced",
        )
        self.assertIs(bucket["force_destroy"], False)

        lifecycle = bucket["lifecycle"][0]

        self.assertIs(lifecycle["prevent_destroy"], True)

        preconditions = lifecycle["precondition"]

        self.assertTrue(
            any(
                "var.governance_approved" in str(item["condition"])
                for item in preconditions
            )
        )

    def test_generated_bucket_names(self):
        """Check naming constraints for current environment defaults."""
        import re

        variables_path = ENV / "variables.tf"

        with variables_path.open(encoding="utf-8") as stream:
            config = hcl2.load(stream)

        variables = {
            normalise(name): details
            for block in config["variable"]
            for name, details in block.items()
        }

        platform = normalise(variables["platform_name"]["default"])
        environment = normalise(variables["environment"]["default"])

        prefix = f"{platform}-{environment}"

        self.assertEqual(len(self.domains), 5)

        names = [
            f"{prefix}-{domain.replace('_', '-')}".lower()
            for domain in self.domains
        ]

        self.assertEqual(len(names), len(set(names)))

        for name in names:
            with self.subTest(bucket=name):
                self.assertGreaterEqual(len(name), 3)
                self.assertLessEqual(len(name), 63)
                self.assertRegex(
                    name,
                    r"^[a-z0-9][a-z0-9._-]*[a-z0-9]$",
                )


if __name__ == "__main__":
    unittest.main()
