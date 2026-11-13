"""GCP-006D1: Governed interoperability IAM regression tests."""

import unittest
from pathlib import Path

import hcl2


ROOT = Path(__file__).resolve().parents[4]
ENV = ROOT / "infrastructure/terraform/gcp/environments/dev"


def normalise(value):
    """Remove surrounding HCL string-literal quotation marks."""
    if isinstance(value, str):
        if len(value) >= 2 and value[0] == value[-1] == '"':
            return value[1:-1]
    return value


def load_hcl(path):
    with path.open(encoding="utf-8") as stream:
        return hcl2.load(stream)


class InteroperabilityIAMTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.iam = load_hcl(ENV / "interoperability_iam.tf")
        cls.variables_config = load_hcl(
            ENV / "interoperability_iam_variables.tf"
        )

        cls.variables = {
            normalise(name): details
            for block in cls.variables_config["variable"]
            for name, details in block.items()
        }

        cls.resources = {
            (normalise(resource_type), normalise(name)): details
            for block in cls.iam["resource"]
            for resource_type, instances in block.items()
            for name, details in instances.items()
        }

        cls.writer = cls.resources[
            ("google_storage_bucket_iam_member", "interoperability_writer")
        ]
        cls.reader = cls.resources[
            ("google_storage_bucket_iam_member", "interoperability_reader")
        ]

    def test_deployment_disabled_by_default(self):
        self.assertIs(
            self.variables["enable_interoperability_iam_deployment"]["default"],
            False,
        )

    def test_principals_empty_by_default(self):
        for principal in (
            "interoperability_writer_principal",
            "interoperability_reader_principal",
        ):
            with self.subTest(principal=principal):
                self.assertEqual(
                    normalise(self.variables[principal]["default"]),
                    "",
                )

    def test_only_bucket_scoped_iam_resources(self):
        self.assertEqual(
            set(self.resources),
            {
                (
                    "google_storage_bucket_iam_member",
                    "interoperability_writer",
                ),
                (
                    "google_storage_bucket_iam_member",
                    "interoperability_reader",
                ),
            },
        )

    def test_writer_and_reader_roles(self):
        self.assertEqual(
            normalise(self.writer["role"]),
            "roles/storage.objectCreator",
        )
        self.assertEqual(
            normalise(self.reader["role"]),
            "roles/storage.objectViewer",
        )

    def test_interoperability_bucket_only(self):
        for name, resource in (
            ("writer", self.writer),
            ("reader", self.reader),
        ):
            with self.subTest(identity=name):
                self.assertIn(
                    'module.governed_storage_domains["interoperability"]',
                    str(resource["bucket"]),
                )

    def test_all_three_deployment_gates(self):
        expression = str(
            self.iam["locals"][0]["interoperability_iam_enabled"]
        )

        for gate in (
            "var.enable_interoperability_iam_deployment",
            "var.enable_governed_storage_deployment",
            "var.governance_approved",
        ):
            with self.subTest(gate=gate):
                self.assertIn(gate, expression)

        for resource in (self.writer, self.reader):
            self.assertIn(
                "local.interoperability_iam_enabled",
                str(resource["count"]),
            )

    def test_principals_are_distinct_and_nonempty(self):
        for resource in (self.writer, self.reader):
            lifecycle = resource["lifecycle"][0]
            condition = str(lifecycle["precondition"][0]["condition"])

            self.assertIn("trimspace(", condition)
            self.assertIn("interoperability_writer_principal", condition)
            self.assertIn("interoperability_reader_principal", condition)
            self.assertIn("!=", condition)

    def test_principal_validation_excludes_public_members(self):
        for name in (
            "interoperability_writer_principal",
            "interoperability_reader_principal",
        ):
            with self.subTest(principal=name):
                validation = self.variables[name]["validation"][0]
                condition = str(validation["condition"])

                self.assertIn("serviceAccount:", condition)
                self.assertIn("principal://", condition)
                self.assertIn("principalSet://", condition)

                self.assertNotIn("allUsers", condition)
                self.assertNotIn("allAuthenticatedUsers", condition)

    def test_no_service_account_or_key_resources(self):
        prohibited = {
            "google_service_account",
            "google_service_account_key",
            "google_project_iam_member",
            "google_project_iam_binding",
            "google_project_iam_policy",
        }

        actual_types = {
            resource_type
            for resource_type, _ in self.resources
        }

        self.assertTrue(actual_types.isdisjoint(prohibited))


if __name__ == "__main__":
    unittest.main()
