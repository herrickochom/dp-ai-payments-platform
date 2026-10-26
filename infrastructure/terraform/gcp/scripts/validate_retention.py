#!/usr/bin/env python3

from pathlib import Path
import sys
import yaml

ROOT = Path(__file__).resolve().parents[4]

POLICY = (
    ROOT
    / "platform/config/governance/gcp_retention_schedule.yaml"
)

ALLOWED_CLASSES = {
    "STANDARD",
    "NEARLINE",
    "COLDLINE",
    "ARCHIVE",
}

REQUIRED_FIELDS = {
    "classification",
    "storage_class",
    "retention_days",
    "retention_basis",
    "automatic_deletion",
}

def main():
    with POLICY.open() as f:
        policy = yaml.safe_load(f)

    assert policy["status"] in {"proposed", "approved"}

    datasets = policy["datasets"]

    for name, dataset in datasets.items():
        missing = REQUIRED_FIELDS - dataset.keys()

        if missing:
            raise ValueError(
                f"{name}: missing fields {sorted(missing)}"
            )

        if dataset["storage_class"] not in ALLOWED_CLASSES:
            raise ValueError(
                f"{name}: invalid storage class"
            )

        days = dataset["retention_days"]

        if days is not None:
            if type(days) is not int or days <= 0:
                raise ValueError(
                    f"{name}: invalid retention period"
                )

        if dataset["automatic_deletion"]:
            raise ValueError(
                f"{name}: automatic deletion requires "
                "separate enforcement approval"
            )

    print(f"PASS: {len(datasets)} dataset policies validated.")

    if policy["status"] != "approved":
        print(
            "NOTICE: Schedule remains proposed. "
            "Infrastructure enforcement is not authorised."
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
