"""Read-only Raw/S3 readiness checks for scheduled orchestration."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass

import boto3
from botocore.client import Config as BotoConfig


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
    endpoint = os.environ.get(
        "S3_ENDPOINT",
        "http://minio:9000",
    )

    access_key = os.environ.get("MINIO_ROOT_USER")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD")

    if not access_key or not secret_key:
        raise RuntimeError(
            "MinIO credentials are required"
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=BotoConfig(
            signature_version="s3v4",
        ),
        region_name="us-east-1",
    )


def run_raw_readiness() -> dict:
    """Inspect Raw object metadata without reading object payloads."""

    bucket = os.environ.get(
        "MINIO_BUCKET",
        "dp-ai-payment",
    )

    raw_prefix = os.environ.get(
        "RAW_PREFIX",
        "raw/v2",
    ).strip("/")

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
