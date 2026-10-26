#!/usr/bin/env python3
"""Validate proposed GCP storage governance policies."""

from pathlib import Path
import sys
import yaml

ROOT = Path(__file__).resolve().parents[4]

POLICY = (
    ROOT
    / "platform/config/governance/gcp_retention_schedule.yaml"
)

ALLOWED_CLASSES = {
    "STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"
}

REQUIRED_FIELDS = {
    "classification",
    "storage_class",
    "retention_days",
    "retention_basis",
    "automatic_deletion",
    "approval_status",
}

REQUIRED_GOVERNANCE = {
    "legal_hold_blocks_disposal",
    "require_disposal_authorisation",
    "audit_retention_actions",
    "protect_active_iceberg_snapshots",
    "protect_flink_recovery_state",
    "enforce_policy_before_deployment",
}


def validate_policy(policy):
    if not isinstance(policy, dict):
        raise ValueError("Retention policy must be a mapping.")

    if policy.get("status") not in {"proposed", "approved"}:
        raise ValueError("Invalid retention policy status.")

    defaults = policy.get("defaults", {})

    if defaults.get("automatic_deletion") is not False:
        raise ValueError("Automatic deletion must remain disabled.")

    if defaults.get("require_approved_schedule") is not True:
        raise ValueError("Approved retention schedule is mandatory.")

    governance = policy.get("governance", {})

    for control in REQUIRED_GOVERNANCE:
        if governance.get(control) is not True:
            raise ValueError(
                f"Required governance control disabled: {control}"
            )

    datasets = policy.get("datasets")

    if not isinstance(datasets, dict) or not datasets:
        raise ValueError("Dataset policies are missing.")

    for name, dataset in datasets.items():
        if not isinstance(dataset, dict):
            raise ValueError(f"{name}: invalid dataset policy.")

        missing = REQUIRED_FIELDS - dataset.keys()

        if missing:
            raise ValueError(
                f"{name}: missing fields {sorted(missing)}"
            )

        if dataset["storage_class"] not in ALLOWED_CLASSES:
            raise ValueError(
                f"{name}: invalid storage class."
            )

        if not isinstance(dataset["classification"], str) or not dataset["classification"].strip():
            raise ValueError(
                f"{name}: classification is required."
            )

        if not isinstance(dataset["retention_basis"], str) or not dataset["retention_basis"].strip():
            raise ValueError(
                f"{name}: retention basis is required."
            )

        days = dataset["retention_days"]

        if days is not None:
            if type(days) is not int or days <= 0:
                raise ValueError(
                    f"{name}: invalid retention period."
                )

        if dataset["automatic_deletion"] is not False:
            raise ValueError(
                f"{name}: automatic deletion is not authorised."
            )

        if dataset["approval_status"] not in {
            "proposed", "approved"
        }:
            raise ValueError(
                f"{name}: invalid approval status."
            )

        if (
            policy["status"] == "proposed"
            and dataset["approval_status"] == "approved"
        ):
            raise ValueError(
                f"{name}: dataset cannot be approved "
                "under a proposed schedule."
            )

        if (
            name == "iceberg_tables"
            and dataset.get("lifecycle_transition_enabled") is not False
        ):
            raise ValueError(
                "Active Iceberg tables must not have "
                "automatic lifecycle transitions."
            )

    return len(datasets)


def main():
    try:
        with POLICY.open(encoding="utf-8") as f:
            policy = yaml.safe_load(f)

        count = validate_policy(policy)

    except (OSError, yaml.YAMLError, ValueError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"PASS: {count} dataset policies validated.")
    print("PASS: Mandatory governance controls validated.")
    print("PASS: Automatic deletion remains disabled.")

    if policy["status"] != "approved":
        print(
            "NOTICE: Schedule remains proposed. "
            "Infrastructure enforcement is not authorised."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
