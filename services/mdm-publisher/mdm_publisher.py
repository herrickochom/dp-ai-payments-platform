from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]

REGISTRY_PATH = (
    ROOT
    / "platform"
    / "mdm"
    / "contracts"
    / "mdm_publish_registry.json"
)

TOPIC_CONTRACT_PATH = (
    ROOT
    / "platform"
    / "mdm"
    / "contracts"
    / "mdm_kafka_topics.json"
)


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def load_registry() -> dict[str, Any]:
    return load_json(REGISTRY_PATH)


def load_topic_contract() -> dict[str, Any]:
    return load_json(TOPIC_CONTRACT_PATH)


def topic_policy() -> dict[str, dict[str, Any]]:
    contract = load_topic_contract()

    return {
        row["topic"]: row
        for row in contract["topics"]
    }


def stable_key(
    spec: dict[str, Any],
    record: dict[str, Any],
) -> str:
    if "key_field" in spec:
        fields = [spec["key_field"]]
    else:
        fields = list(spec["key_fields"])

    values = []

    for field in fields:
        value = record.get(field)

        if value is None or value == "":
            raise ValueError(
                f"Missing Kafka key field: {field}"
            )

        values.append(str(value))

    return "|".join(values)


def validate_authority(
    spec: dict[str, Any],
    policy: dict[str, Any],
    authority: str,
) -> None:
    expected = policy["consumer_authority"]

    if spec["authority"] != expected:
        raise PermissionError(
            "Registry/topic authority mismatch"
        )

    if authority != expected:
        raise PermissionError(
            f"Publisher authority {authority!r} "
            f"cannot publish topic requiring {expected!r}"
        )


def validate_registry() -> dict[str, int]:
    registry = load_registry()
    policies = topic_policy()

    if registry["publishing_enabled_by_default"] is not False:
        raise ValueError(
            "MDM publishing must default to disabled"
        )

    counts: dict[str, int] = {}

    for spec in registry["records"]:
        topic = spec["topic"]

        if topic not in policies:
            raise ValueError(
                f"Unknown MDM topic: {topic}"
            )

        if (
            spec["authority"]
            != policies[topic]["consumer_authority"]
        ):
            raise ValueError(
                f"Authority mismatch for {topic}"
            )

        path = ROOT / spec["file"]

        if not path.exists():
            raise FileNotFoundError(path)

        rows = load_json(path)

        if not isinstance(rows, list):
            raise ValueError(
                f"Expected list payload: {path}"
            )

        for row in rows:
            stable_key(spec, row)

        counts[topic] = len(rows)

    return counts


def build_publish_plan(
    authority: str,
) -> list[dict[str, Any]]:
    registry = load_registry()
    policies = topic_policy()

    plan: list[dict[str, Any]] = []

    for spec in registry["records"]:
        topic = spec["topic"]
        policy = policies[topic]

        if spec["authority"] != authority:
            continue

        validate_authority(
            spec,
            policy,
            authority,
        )

        rows = load_json(
            ROOT / spec["file"]
        )

        for row in rows:
            plan.append(
                {
                    "topic": topic,
                    "key": stable_key(
                        spec,
                        row,
                    ),
                    "record": row,
                }
            )

    return plan


def execute_publish(
    authority: str,
) -> int:
    """
    Explicit execution path.

    Importing this module, validation and plan generation never
    connect to Kafka.
    """
    if (
        os.getenv(
            "MDM_KAFKA_PUBLISH_ENABLED",
            "false",
        ).lower()
        != "true"
    ):
        raise RuntimeError(
            "MDM Kafka publishing is disabled"
        )

    bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS"
    )

    if not bootstrap:
        raise RuntimeError(
            "KAFKA_BOOTSTRAP_SERVERS is required"
        )

    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError(
            "confluent_kafka is required for execution"
        ) from exc

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
        }
    )

    plan = build_publish_plan(authority)

    delivered = 0

    def callback(err, _msg):
        nonlocal delivered

        if err is not None:
            raise RuntimeError(str(err))

        delivered += 1

    for item in plan:
        producer.produce(
            item["topic"],
            key=item["key"].encode("utf-8"),
            value=json.dumps(
                item["record"],
                separators=(",", ":"),
            ).encode("utf-8"),
            on_delivery=callback,
        )

    producer.flush()

    if delivered != len(plan):
        raise RuntimeError(
            f"Delivery mismatch: "
            f"{delivered}/{len(plan)}"
        )

    return delivered


def main() -> int:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--authority",
        choices=[
            "ordinary",
            "restricted_identity",
        ],
        required=True,
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
    )

    parser.add_argument(
        "--plan-only",
        action="store_true",
    )

    parser.add_argument(
        "--execute",
        action="store_true",
    )

    args = parser.parse_args()

    modes = sum(
        [
            args.validate_only,
            args.plan_only,
            args.execute,
        ]
    )

    if modes != 1:
        parser.error(
            "Choose exactly one of "
            "--validate-only, --plan-only, --execute"
        )

    counts = validate_registry()

    if args.validate_only:
        print("MDM_PUBLISHER_VALIDATION=PASS")

        for topic in sorted(counts):
            print(
                f"TOPIC={topic}|"
                f"ROWS={counts[topic]}"
            )

        return 0

    plan = build_publish_plan(
        args.authority
    )

    if args.plan_only:
        topic_counts: dict[str, int] = {}

        for item in plan:
            topic_counts[item["topic"]] = (
                topic_counts.get(
                    item["topic"],
                    0,
                )
                + 1
            )

        print(
            f"AUTHORITY={args.authority}"
        )
        print(
            f"PUBLISH_PLAN_RECORDS={len(plan)}"
        )

        for topic in sorted(topic_counts):
            print(
                f"PLAN_TOPIC={topic}|"
                f"ROWS={topic_counts[topic]}"
            )

        print("KAFKA_CONNECTIONS=0")
        return 0

    delivered = execute_publish(
        args.authority
    )

    print(
        f"MDM_PUBLISHED_RECORDS={delivered}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
