"""Offline Composer deployment-readiness assessment.

This tool NEVER deploys infrastructure.
"""

import json
from pathlib import Path


REQUIRED_APPROVALS = (
    "composer_image_verified",
    "airflow_compatibility_verified",
    "iam_review_approved",
    "network_review_approved",
    "cost_review_approved",
    "billing_controls_verified",
    "manifest_generation_verification_implemented",
    "dataflow_idempotency_implemented",
    "durable_recovery_implemented",
    "bigquery_reconciliation_implemented",
    "runtime_integration_tests_passed",
    "deployment_authorised",
)


def assess(approval_path=None):
    approvals = {}

    if approval_path is not None:
        path = Path(approval_path)
        if path.is_file():
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    approvals = loaded
            except (ValueError, OSError):
                pass

    missing = [
        key for key in REQUIRED_APPROVALS
        if approvals.get(key) is not True
    ]

    return {
        "ready": not missing,
        "missing": missing,
        "assessment_only": True,
        "deployment_performed": False,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--approvals")
    args = parser.parse_args()

    print(json.dumps(assess(args.approvals), indent=2))
