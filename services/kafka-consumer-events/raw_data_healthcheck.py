#!/usr/bin/env python3
"""Report healthy once Raw contains at least one Avro object for every topic."""

import os
import sys

import boto3
from botocore.client import Config as BotoConfig


def main() -> int:
    topics = [topic.strip() for topic in os.environ["KAFKA_TOPICS"].split(",") if topic.strip()]
    raw_prefix = os.getenv("RAW_PREFIX", "raw/v2").strip("/")
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )

    paginator = client.get_paginator("list_objects_v2")
    discovered = set()
    for page in paginator.paginate(Bucket=os.getenv("MINIO_BUCKET", "dp-ai-payment"), Prefix=f"{raw_prefix}/"):
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
