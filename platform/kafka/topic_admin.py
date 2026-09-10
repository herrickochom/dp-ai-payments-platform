#!/usr/bin/env python3
"""Reconcile Kafka topics from the single authoritative topics.yaml manifest."""
import os
import subprocess
import sys
from typing import Dict, List

import yaml

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:9092")
MANIFEST = os.getenv("TOPICS_CONFIG", "/app/topics.yaml")
TOPICS = os.getenv("KAFKA_TOPICS_COMMAND", "kafka-topics")
CONFIGS = os.getenv("KAFKA_CONFIGS_COMMAND", "kafka-configs")


def run(*args: str, **kwargs) -> str:
    check = kwargs.get("check", True)
    result = subprocess.run(args, check=check, universal_newlines=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout + result.stderr


def load_manifest(path: str) -> List[Dict]:
    with open(path, encoding="utf-8") as stream:
        document = yaml.safe_load(stream)
    topics = document.get("topics") if isinstance(document, dict) else None
    if not isinstance(topics, list) or not topics:
        raise ValueError("topics.yaml must contain a non-empty 'topics' list")
    required = {"name", "partitions", "replication", "retention_ms"}
    defaults = document.get("defaults", {})
    topics = [{**defaults, **topic} for topic in topics]
    for topic in topics:
        missing = required - topic.keys()
        if missing:
            raise ValueError(f"Topic {topic.get('name', '<unnamed>')} missing {sorted(missing)}")
    return topics


def describe(name: str) -> str:
    return run(TOPICS, "--bootstrap-server", BOOTSTRAP, "--describe", "--topic", name,
               check=False)


def current_partitions(description: str) -> int:
    marker = "PartitionCount:"
    return int(description.split(marker, 1)[1].split()[0])


def current_replication(description: str) -> int:
    marker = "ReplicationFactor:"
    return int(description.split(marker, 1)[1].split()[0])


def reconcile(topic: dict) -> None:
    name = str(topic["name"])
    partitions = int(topic["partitions"])
    replication = int(os.getenv("TOPIC_REPLICATION_FACTOR_OVERRIDE", topic["replication"]))
    retention = str(topic["retention_ms"])
    cleanup = str(topic.get("cleanup_policy", "delete"))
    description = describe(name)
    if "Topic '" in description and "does not exist" in description or "UNKNOWN_TOPIC" in description:
        run(TOPICS, "--bootstrap-server", BOOTSTRAP, "--create", "--topic", name,
            "--partitions", str(partitions), "--replication-factor", str(replication),
            "--config", f"retention.ms={retention}", "--config", f"cleanup.policy={cleanup}")
        print(f"created {name}")
        return

    actual_partitions = current_partitions(description)
    actual_replication = current_replication(description)
    if actual_partitions < partitions:
        run(TOPICS, "--bootstrap-server", BOOTSTRAP, "--alter", "--topic", name,
            "--partitions", str(partitions))
        print(f"increased partitions {name}: {actual_partitions} -> {partitions}")
    elif actual_partitions > partitions:
        raise RuntimeError(f"{name}: cannot decrease partitions {actual_partitions} -> {partitions}")
    if actual_replication != replication:
        raise RuntimeError(
            f"{name}: replication drift {actual_replication} != {replication}; "
            "partition reassignment must be performed explicitly"
        )
    run(CONFIGS, "--bootstrap-server", BOOTSTRAP, "--entity-type", "topics",
        "--entity-name", name, "--alter", "--add-config",
        f"retention.ms={retention},cleanup.policy={cleanup}")
    print(f"reconciled {name}")


def main() -> int:
    topics = load_manifest(MANIFEST)
    for topic in topics:
        reconcile(topic)
    managed = {str(topic["name"]) for topic in topics}
    existing = {name for name in run(TOPICS, "--bootstrap-server", BOOTSTRAP, "--list").splitlines()
                if name and not name.startswith("_")}
    unmanaged = sorted(existing - managed)
    if unmanaged:
        print("warning: unmanaged topics detected (not deleted): " + ", ".join(unmanaged),
              file=sys.stderr)
    print(f"reconciled {len(topics)} topics from {MANIFEST}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"topic reconciliation failed: {exc}", file=sys.stderr)
        raise
