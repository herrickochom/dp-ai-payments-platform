"""Static IAM contract for the single MinIO bucket."""

import json
from pathlib import Path

import pytest


POLICY_DIR = Path(__file__).resolve().parents[2] / "platform/minio/policies"
BUCKET = "arn:aws:s3:::dp-ai-payment"
RAW = f"{BUCKET}/raw/v2/*"
WAREHOUSE = f"{BUCKET}/warehouse/*"
QUARANTINE = f"{BUCKET}/restricted/cdc-quarantine/v1/*"
POLICIES = {
    "raw-ingest": ({"raw/v2", "raw/v2/*"}, {RAW: {"s3:GetObject", "s3:PutObject"}}),
    "cdc-quarantine": ({"restricted/cdc-quarantine/v1", "restricted/cdc-quarantine/v1/*"}, {QUARANTINE: {"s3:GetObject", "s3:PutObject"}}),
    "platform-raw-read": ({"raw/v2", "raw/v2/*"}, {RAW: {"s3:GetObject"}}),
    "nessie-catalog": ({"warehouse", "warehouse/*"}, {WAREHOUSE: {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"}}),
    "trino-iceberg-read": ({"warehouse", "warehouse/*"}, {WAREHOUSE: {"s3:GetObject"}}),
    "ordinary-transform": ({"raw/v2", "raw/v2/*", "warehouse", "warehouse/*"}, {RAW: {"s3:GetObject"}, WAREHOUSE: {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"}}),
    "restricted-transform": ({"raw/v2", "raw/v2/*", "warehouse", "warehouse/*"}, {RAW: {"s3:GetObject"}, WAREHOUSE: {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"}}),
    "ml-transform": ({"raw/v2", "raw/v2/*", "warehouse", "warehouse/*"}, {RAW: {"s3:GetObject"}, WAREHOUSE: {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"}}),
}


def test_exactly_eight_policies_exist():
    assert {path.stem for path in POLICY_DIR.glob("*.json")} == set(POLICIES)


@pytest.mark.parametrize("name", POLICIES)
def test_policy_authority_is_exact_and_scoped(name):
    policy = json.loads((POLICY_DIR / f"{name}.json").read_text())
    assert policy["Version"] == "2012-10-17"
    assert set(policy) == {"Version", "Statement"}
    expected_prefixes, expected_objects = POLICIES[name]
    actual_prefixes = set()
    actual_objects = {}

    for statement in policy["Statement"]:
        assert statement["Effect"] == "Allow"
        assert "NotAction" not in statement and "NotResource" not in statement
        actions = set(statement["Action"])
        resources = set(statement["Resource"])
        assert actions <= {"s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"}
        assert "s3:*" not in actions
        assert "*" not in resources
        assert resources <= {BUCKET, RAW, WAREHOUSE, QUARANTINE}
        if "s3:ListBucket" in actions:
            assert actions == {"s3:ListBucket"}
            assert resources == {BUCKET}
            if name == "nessie-catalog" and "Condition" not in statement:
                # Nessie 0.108.4 readiness performs HeadBucket, which cannot
                # carry an s3:prefix. Object access remains warehouse-scoped.
                actual_prefixes.update(expected_prefixes)
            else:
                assert set(statement["Condition"]) == {"StringLike"}
                assert set(statement["Condition"]["StringLike"]) == {"s3:prefix"}
                actual_prefixes.update(statement["Condition"]["StringLike"]["s3:prefix"])
        else:
            assert "Condition" not in statement
            assert len(resources) == 1
            resource = next(iter(resources))
            assert resource in {RAW, WAREHOUSE, QUARANTINE}
            actual_objects.setdefault(resource, set()).update(actions)

    assert actual_prefixes == expected_prefixes
    assert actual_objects == expected_objects
