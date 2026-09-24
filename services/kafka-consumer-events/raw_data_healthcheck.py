#!/usr/bin/env python3
"""Report healthy once Raw contains at least one Avro object for every topic."""

import os
import sys

import boto3
from botocore.client import Config as BotoConfig

from services.shared.security.runtime_security import (
    validate_object_store_security,
)
from services.shared.security.secret_provider import require_secret


def main() -> int:
    topics = [topic.strip() for topic in os.environ["KAFKA_TOPICS"].split(",") if topic.strip()]
    raw_prefix = os.getenv("RAW_PREFIX", "").strip("/")
    bucket = os.getenv("OBJECT_STORE_BUCKET", "").strip()
    region = os.getenv("OBJECT_STORE_REGION", "").strip()
    endpoint = os.getenv("S3_ENDPOINT", "").strip()
    use_ssl = os.getenv("S3_USE_SSL", "").strip()

    if not raw_prefix:
        raise RuntimeError("RAW_PREFIX is required")
    if not bucket:
        raise RuntimeError("OBJECT_STORE_BUCKET is required")
    if not region:
        raise RuntimeError("OBJECT_STORE_REGION is required")
    if not endpoint:
        raise RuntimeError("S3_ENDPOINT is required")
    if not use_ssl:
        raise RuntimeError("S3_USE_SSL is required")
    access_key = os.getenv("PLATFORM_RAW_READ_ACCESS_KEY", "").strip()
    secret_key = require_secret("PLATFORM_RAW_READ_SECRET_KEY")
    if not access_key:
        raise RuntimeError("PLATFORM_RAW_READ_ACCESS_KEY is required")
    endpoint, _, ca_bundle = validate_object_store_security(
        endpoint,
        use_ssl=use_ssl,
        ca_bundle=os.getenv("S3_CA_BUNDLE", ""),
    )

    client_kwargs = {
        "endpoint_url": endpoint,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "config": BotoConfig(signature_version="s3v4"),
        "region_name": region,
    }

    if ca_bundle:
        client_kwargs["verify"] = ca_bundle

    client = boto3.client("s3", **client_kwargs)

    paginator = client.get_paginator("list_objects_v2")
    discovered = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{raw_prefix}/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.endswith(".avro"):
                for topic in topics:
                    if f"/topic={topic}/" in key:
                        discovered.add(topic)

        if len(discovered) == len(topics):
            return 0

    missing = sorted(set(topics) - discovered)
    print(f"Raw data is not ready; missing topics: {', '.join(missing)}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
