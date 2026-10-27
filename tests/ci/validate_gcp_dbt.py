#!/usr/bin/env python3
"""Validate the GCP dbt project without cloud authentication."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[2]
PROJECT = ROOT / "transform" / "dbt_gcp"

EXPECTED_MODELS = {
    "slv_payment_event_identity_status",
    "slv_payment_events_trusted",
    "slv_payment_event_conflicts",
}

EXPECTED_TESTS = {
    "assert_trusted_identity_unique",
    "assert_trusted_conflict_disjoint",
    "assert_identity_observation_reconciliation",
    "assert_identity_partition",
}


def validate_manifest(manifest, project_id, dataset):
    nodes = manifest.get("nodes", {})

    models = {
        node["name"]: node
        for node in nodes.values()
        if node.get("resource_type") == "model"
    }

    tests = {
        node["name"]
        for node in nodes.values()
        if node.get("resource_type") == "test"
    }

    failures = []

    print("\n=== Silver model validation ===")

    for name in sorted(EXPECTED_MODELS):
        node = models.get(name)

        if node is None:
            failures.append(f"Missing model: {name}")
            continue

        actual_project = node.get("database")
        actual_dataset = node.get("schema")

        if actual_project != project_id:
            failures.append(
                f"{name}: unexpected project {actual_project}"
            )

        if actual_dataset != dataset:
            failures.append(
                f"{name}: unexpected dataset {actual_dataset}"
            )

        print(
            f"{name}: {actual_project}.{actual_dataset}"
        )

    print("\n=== SQL integrity test registration ===")

    for name in sorted(EXPECTED_TESTS):
        if name not in tests:
            failures.append(f"Missing test: {name}")
            print(f"FAIL: {name}")
        else:
            print(f"PASS: {name}")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        return False

    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        default="pdm-offline-validation",
    )
    parser.add_argument(
        "--dataset",
        default="pdm_payments_dev",
    )
    parser.add_argument(
        "--dbt",
        default="dbt",
    )

    args = parser.parse_args()

    # Offline validation must never target a production project.
    allowed_projects = {"pdm-offline-validation"}

    if args.project_id not in allowed_projects:
        raise SystemExit(
            "STOP: Project is not approved for offline CI validation."
        )

    if args.dataset != "pdm_payments_dev":
        raise SystemExit(
            "STOP: Unexpected development dataset."
        )

    profile = f"""pdm_gcp:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      keyfile: /nonexistent/pdm-gcp-offline-validation.json
      project: {args.project_id}
      dataset: {args.dataset}
      location: europe-west2
      threads: 1
"""

    with tempfile.TemporaryDirectory(
        prefix="pdm-gcp-dbt-ci-"
    ) as directory:
        temp = Path(directory)
        profiles = temp / "profiles"
        target = temp / "target"

        profiles.mkdir()
        (profiles / "profiles.yml").write_text(profile)

        env = os.environ.copy()
        env["GCP_PROJECT_ID"] = args.project_id

        print("=== Fresh offline dbt parse ===", flush=True)

        result = subprocess.run(
            [
                args.dbt,
                "parse",
                "--project-dir",
                str(PROJECT),
                "--profiles-dir",
                str(profiles),
                "--target-path",
                str(target),
                "--no-partial-parse",
            ],
            env=env,
            check=False,
        )

        if result.returncode:
            raise SystemExit(
                f"FAIL: dbt parse returned {result.returncode}"
            )

        manifest_path = target / "manifest.json"

        if not manifest_path.is_file():
            raise SystemExit("FAIL: dbt manifest missing.")

        manifest = json.loads(manifest_path.read_text())

        if not validate_manifest(
            manifest,
            args.project_id,
            args.dataset,
        ):
            raise SystemExit(1)

    print("\nPASS: Offline GCP dbt validation.")
    print("No GCP authentication or deployment requested.")


if __name__ == "__main__":
    main()
