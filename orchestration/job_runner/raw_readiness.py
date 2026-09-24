"""Read-only Raw/S3 readiness checks for scheduled orchestration."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass

import boto3
from botocore.client import Config as BotoConfig

from services.shared.security.runtime_security import (
    validate_object_store_security,
)


@dataclass(frozen=True)
class TopicReadiness:
    topic: str
    object_count: int


def _topics_from_env() -> tuple[str, ...]:
    raw = os.environ.get("KAFKA_TOPICS", "")

    topics = tuple(
        dict.fromkeys(
            topic.strip()
            for topic in raw.split(",")
            if topic.strip()
        )
    )

    if not topics:
        raise RuntimeError(
            "KAFKA_TOPICS must contain at least one topic"
        )

    return topics


def _client():
    access_key = os.environ.get(
        "PLATFORM_RAW_READ_ACCESS_KEY"
    )
    secret_key = os.environ.get(
        "PLATFORM_RAW_READ_SECRET_KEY"
    )

    if not access_key or not secret_key:
        raise RuntimeError(
            "dedicated raw-read object-store credentials are required"
        )

    endpoint = os.environ.get("S3_ENDPOINT", "").strip()
    use_ssl = os.environ.get("S3_USE_SSL", "").strip()
    ca_bundle = os.environ.get("S3_CA_BUNDLE", "").strip()
    region = os.environ.get("OBJECT_STORE_REGION", "").strip()

    if not endpoint:
        raise RuntimeError("S3_ENDPOINT is required")
    if not use_ssl:
        raise RuntimeError("S3_USE_SSL is required")
    if not region:
        raise RuntimeError("OBJECT_STORE_REGION is required")

    endpoint, _, ca_bundle = validate_object_store_security(
        endpoint,
        use_ssl=use_ssl,
        ca_bundle=ca_bundle,
    )

    client_kwargs = {
        "endpoint_url": endpoint,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "config": BotoConfig(
            signature_version="s3v4",
        ),
        "region_name": region,
    }

    if ca_bundle:
        client_kwargs["verify"] = ca_bundle

    return boto3.client("s3", **client_kwargs)


def run_raw_readiness() -> dict:
    """Inspect Raw object metadata without reading object payloads."""

    bucket = os.environ.get("OBJECT_STORE_BUCKET", "").strip()
    raw_root = os.environ.get("RAW_ROOT", "").strip()
    raw_version = os.environ.get("RAW_VERSION", "").strip()
    raw_prefix = os.environ.get("RAW_PREFIX", "").strip()

    if not bucket:
        raise RuntimeError("OBJECT_STORE_BUCKET is required")
    if not raw_root:
        raise RuntimeError("RAW_ROOT is required")
    if not raw_version:
        raise RuntimeError("RAW_VERSION is required")
    if not raw_prefix:
        raise RuntimeError("RAW_PREFIX is required")
    if raw_prefix != f"{raw_root}/{raw_version}":
        raise ValueError("RAW_PREFIX must equal RAW_ROOT + '/' + RAW_VERSION")

    topics = _topics_from_env()
    client = _client()

    # Read-only bucket metadata operation.
    client.head_bucket(
        Bucket=bucket,
    )

    counts = {
        topic: 0
        for topic in topics
    }

    total_objects = 0
    total_bytes = 0
    avro_objects = 0

    paginator = client.get_paginator(
        "list_objects_v2"
    )

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=f"{raw_prefix}/",
    ):
        for item in page.get(
            "Contents",
            [],
        ):
            key = item["Key"]

            total_objects += 1
            total_bytes += int(
                item.get("Size", 0)
            )

            if not key.endswith(".avro"):
                continue

            avro_objects += 1

            for topic in topics:
                if f"/topic={topic}/" in key:
                    counts[topic] += 1

    missing = sorted(
        topic
        for topic, count in counts.items()
        if count == 0
    )

    if total_objects == 0:
        raise RuntimeError(
            f"Raw prefix is empty: {raw_prefix}/"
        )

    if avro_objects == 0:
        raise RuntimeError(
            f"Raw prefix contains no Avro objects: "
            f"{raw_prefix}/"
        )

    if missing:
        raise RuntimeError(
            "Raw data is not ready; missing topics: "
            + ", ".join(missing)
        )

    topic_readiness = [
        asdict(
            TopicReadiness(
                topic=topic,
                object_count=counts[topic],
            )
        )
        for topic in topics
    ]

    return {
        "status": "SUCCESS",
        "mutating": False,
        "bucket": bucket,
        "prefix": f"{raw_prefix}/",
        "expected_topic_count": len(topics),
        "ready_topic_count": len(topics),
        "total_objects": total_objects,
        "total_bytes": total_bytes,
        "avro_objects": avro_objects,
        "topics": topic_readiness,
        "payload_reads": 0,
        "object_writes": 0,
        "object_deletes": 0,
    }
