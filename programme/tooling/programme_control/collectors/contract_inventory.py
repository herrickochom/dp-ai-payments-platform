"""Schema, contract and configuration inventory.

Records where machine-readable contracts live and what kind they are.
Avro schemas are read for their namespace/name metadata only. Secret
material, keystores and key stores are never opened.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .base import (
    KIND_CONTRACT,
    KIND_SCHEMA,
    PRESENT,
    CollectorResult,
    EvidenceItem,
    file_status,
    register,
    utc_now_iso,
)

# Directories scanned for contracts, with the contract kind they carry.
CONTRACT_SOURCES = (
    ("contracts/kafka", "*.avsc", "AVRO"),
    ("contracts/iso20022/xsd", "*.xsd", "XML_XSD"),
    ("platform/cdc/contracts", "*.json", "CDC"),
    ("platform/cdc/contracts", "*.avsc", "AVRO"),
    ("platform/mdm/contracts", "*.json", "JSON"),
    ("platform/source_registry/contracts", "*.json", "JSON"),
    ("orchestration/transform_runtime/contracts", "*.json", "JSON"),
    ("infra/terraform/contracts", "*.json", "JSON"),
)

# Files that hold key material: recorded as metadata, never opened.
KEY_MATERIAL_SUFFIXES = (".jks", ".p12", ".pfx", ".pem", ".key")

KAFKA_TOPICS = "platform/kafka/topics.yaml"
GOVERNANCE_CONFIGS = "platform/config/governance"


def _avro_metadata(path: Path) -> dict[str, Any]:
    """Read only structural Avro metadata (namespace, name, fields)."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {
        "namespace": document.get("namespace", ""),
        "schema_name": document.get("name", ""),
        "field_count": len(document.get("fields", []) or []),
        "schema_version": str(document.get("schema_version", "")),
    }


@register("contracts")
def collect_contracts(root: Path) -> CollectorResult:
    result = CollectorResult(name="contracts")
    observed = utc_now_iso()

    contracts: list[dict[str, Any]] = []
    for relative_dir, pattern, kind in CONTRACT_SOURCES:
        base = root / relative_dir
        if not base.is_dir():
            continue
        for path in sorted(base.glob(pattern)):
            status, size, digest = file_status(path)
            relative = str(path.relative_to(root))
            entry: dict[str, Any] = {
                "path": relative,
                "name": path.name,
                "kind": kind,
                "status": status,
                "bytes": size,
                "sha256": digest,
            }
            if kind == "AVRO" and status == PRESENT:
                entry.update(_avro_metadata(path))
            contracts.append(entry)
            result.add(
                EvidenceItem(
                    evidence_id=f"EV-CONTRACT-{len(contracts):03d}",
                    kind=KIND_CONTRACT,
                    title=f"{kind} contract {path.name}",
                    location=relative,
                    status=status,
                    source_path=relative,
                    observed_at=observed,
                    detail={
                        "sha256": digest,
                        "bytes": size,
                        "contract_kind": kind,
                        "schema_name": entry.get("schema_name", ""),
                        "namespace": entry.get("namespace", ""),
                        "field_count": entry.get("field_count", 0),
                    },
                )
            )

    key_material: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if path.suffix.lower() not in KEY_MATERIAL_SUFFIXES:
            continue
        if any(part in {".git", ".venv", "node_modules"} for part in path.parts):
            continue
        relative = str(path.relative_to(root))
        key_material.append({"path": relative, "bytes": path.stat().st_size})
        result.add(
            EvidenceItem(
                evidence_id=f"EV-KEYMATERIAL-{len(key_material):03d}",
                kind=KIND_SCHEMA,
                title=f"Key material present: {path.name} (metadata only)",
                location=relative,
                status="PRESENT_METADATA_ONLY",
                source_path=relative,
                observed_at=observed,
                detail={
                    "note": (
                        "Key/keystore material is never opened, parsed or "
                        "recorded by the programme control system."
                    ),
                    "bytes": path.stat().st_size,
                },
            )
        )

    topics_status, _, topics_hash = file_status(root / KAFKA_TOPICS)
    topic_count = 0
    if topics_status == PRESENT:
        import yaml

        try:
            topics_doc = yaml.safe_load(
                (root / KAFKA_TOPICS).read_text(encoding="utf-8")
            ) or {}
            topic_count = len(topics_doc.get("topics", []) or [])
        except Exception:  # noqa: BLE001 - malformed topic manifest is not fatal
            topic_count = 0

    result.add(
        EvidenceItem(
            evidence_id="EV-KAFKA-TOPIC-MANIFEST",
            kind=KIND_SCHEMA,
            title="Kafka topic manifest",
            location=KAFKA_TOPICS,
            status=topics_status,
            source_path=KAFKA_TOPICS,
            observed_at=observed,
            detail={"topic_count": topic_count, "sha256": topics_hash},
        )
    )

    by_kind: dict[str, int] = {}
    for entry in contracts:
        by_kind[entry["kind"]] = by_kind.get(entry["kind"], 0) + 1

    result.facts = {
        "contract_count": len(contracts),
        "contracts_by_kind": by_kind,
        "contracts": contracts,
        "key_material_count": len(key_material),
        "key_material": key_material,
        "topic_count": topic_count,
        "topic_manifest_status": topics_status,
        "governance_config_dir": GOVERNANCE_CONFIGS,
        "observed_at": observed,
    }
    return result
