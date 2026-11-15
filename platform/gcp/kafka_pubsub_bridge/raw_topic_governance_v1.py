"""Authoritative fail-closed Raw v1 topic governance."""

import json
from pathlib import Path

import yaml


class GovernanceError(ValueError):
    pass


def load_governance():
    root = Path(__file__).resolve().parents[3]

    registry = yaml.safe_load(
        (root / "platform/kafka/topics.yaml").read_text(
            encoding="utf-8"
        )
    )

    document = json.loads(
        Path(__file__).with_name(
            "topic_routes_v1.json"
        ).read_text(encoding="utf-8")
    )

    entries = registry["topics"]
    routes = document["routes"]

    names = [entry["name"] for entry in entries]

    if len(names) != 38 or len(set(names)) != 38:
        raise GovernanceError("invalid 38-topic registry")

    if set(names) != set(routes):
        raise GovernanceError("topic registry/route mismatch")

    approved_modes = {"batch", "streaming", "real-time"}
    approved_formats = {"confluent_avro", "json", "unverified"}
    approved_statuses = {
        "legacy_payment_event",
        "contract_only",
        "blocked_restricted",
        "blocked_unverified_format",
    }

    for entry in entries:
        topic = entry["name"]
        route = routes[topic]

        if entry.get("processing_mode") not in approved_modes:
            raise GovernanceError("invalid processing mode")

        if route.get("processing_mode") != entry["processing_mode"]:
            raise GovernanceError("processing mode mismatch")

        if route.get("format") not in approved_formats:
            raise GovernanceError("unsupported payload format")

        if route.get("status") not in approved_statuses:
            raise GovernanceError("unsupported route status")

        if route["status"] == "blocked_restricted":
            if route["format"] != "json":
                raise GovernanceError("restricted route mismatch")

        if route["status"] == "blocked_unverified_format":
            if route["format"] != "unverified":
                raise GovernanceError("unverified route mismatch")

    return routes


def route_for(topic):
    routes = load_governance()

    if topic not in routes:
        raise GovernanceError("unknown Kafka topic")

    route = dict(routes[topic])

    if route["status"].startswith("blocked"):
        raise GovernanceError("topic blocked from ordinary ingestion")

    return route


def require_live_authorisation(topic, approved_topics):
    route = route_for(topic)

    if not isinstance(approved_topics, (set, frozenset)):
        raise GovernanceError("invalid approval set")

    if topic not in approved_topics:
        raise GovernanceError("live ingestion not explicitly authorised")

    return route
