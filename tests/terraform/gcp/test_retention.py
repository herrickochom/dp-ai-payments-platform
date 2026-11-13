#!/usr/bin/env python3
"""Regression tests for GCP retention governance."""

import copy
import importlib.util
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[4]
VALIDATOR = (
    ROOT / "infrastructure/terraform/gcp/scripts/validate_retention.py"
)
POLICY = (
    ROOT / "platform/config/governance/gcp_retention_schedule.yaml"
)

spec = importlib.util.spec_from_file_location(
    "validate_retention", VALIDATOR
)
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)


class RetentionGovernanceTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with POLICY.open(encoding="utf-8") as f:
            cls.baseline = yaml.safe_load(f)

    def setUp(self):
        self.policy = copy.deepcopy(self.baseline)

    def assert_rejected(self):
        with self.assertRaises(ValueError):
            validator.validate_policy(self.policy)

    def test_baseline_valid(self):
        self.assertEqual(
            validator.validate_policy(self.policy), 9
        )

    def test_automatic_deletion_rejected(self):
        self.policy["datasets"]["operational_events"][
            "automatic_deletion"
        ] = True
        self.assert_rejected()

    def test_default_deletion_rejected(self):
        self.policy["defaults"]["automatic_deletion"] = True
        self.assert_rejected()

    def test_legal_hold_protection_required(self):
        self.policy["governance"][
            "legal_hold_blocks_disposal"
        ] = False
        self.assert_rejected()

    def test_disposal_authorisation_required(self):
        self.policy["governance"][
            "require_disposal_authorisation"
        ] = False
        self.assert_rejected()

    def test_audit_controls_required(self):
        self.policy["governance"][
            "audit_retention_actions"
        ] = False
        self.assert_rejected()

    def test_iceberg_protection_required(self):
        self.policy["governance"][
            "protect_active_iceberg_snapshots"
        ] = False
        self.assert_rejected()

    def test_flink_protection_required(self):
        self.policy["governance"][
            "protect_flink_recovery_state"
        ] = False
        self.assert_rejected()

    def test_iceberg_transition_rejected(self):
        self.policy["datasets"]["iceberg_tables"][
            "lifecycle_transition_enabled"
        ] = True
        self.assert_rejected()

    def test_invalid_storage_class_rejected(self):
        self.policy["datasets"]["audit_evidence"][
            "storage_class"
        ] = "INVALID"
        self.assert_rejected()

    def test_invalid_retention_period_rejected(self):
        self.policy["datasets"]["operational_events"][
            "retention_days"
        ] = -1
        self.assert_rejected()

    def test_premature_dataset_approval_rejected(self):
        self.policy["datasets"]["audit_evidence"][
            "approval_status"
        ] = "approved"
        self.assert_rejected()


if __name__ == "__main__":
    unittest.main()
