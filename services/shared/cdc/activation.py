from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[3]

REGISTRY = (
    ROOT
    / "platform"
    / "source_registry"
    / "contracts"
    / "source_systems.json"
)

CDC_CONTRACT = (
    ROOT
    / "platform"
    / "cdc"
    / "contracts"
    / "pdm_cdc_contract.json"
)

CONNECTOR_CONTRACT = (
    ROOT
    / "platform"
    / "cdc"
    / "contracts"
    / "debezium_connector_contract.json"
)


class CDCActivationDenied(RuntimeError):
    pass


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def source_registry() -> dict[str, Any]:
    return load_json(REGISTRY)


def find_source(
    source_system: str,
) -> dict[str, Any]:
    registry = source_registry()

    matches = [
        row
        for row in registry["source_systems"]
        if row["source_system"] == source_system
    ]

    if len(matches) != 1:
        raise CDCActivationDenied(
            f"Source is not uniquely registered: "
            f"{source_system}"
        )

    return matches[0]


def validate_source_for_cdc(
    source_system: str,
) -> dict[str, Any]:
    source = find_source(source_system)

    required = {
        "adapter_type": "CDC_DATABASE",
        "mutable_database_source": True,
        "cdc_capable": True,
        "cdc_activated": True,
    }

    failures = []

    for field, expected in required.items():
        actual = source.get(field)

        if actual != expected:
            failures.append(
                f"{field}={actual!r};"
                f"expected={expected!r}"
            )

    if failures:
        raise CDCActivationDenied(
            f"CDC activation denied for "
            f"{source_system}: "
            + ", ".join(failures)
        )

    return source


def validate_allowlist(
    source_system: str,
) -> dict[str, Any]:
    contract = load_json(CDC_CONTRACT)

    allowed = contract.get(
        "cdc_sources",
        []
    )

    matches = [
        row
        for row in allowed
        if (
            isinstance(row, dict)
            and row.get("source_system")
            == source_system
        )
        or (
            isinstance(row, str)
            and row == source_system
        )
    ]

    if len(matches) != 1:
        raise CDCActivationDenied(
            f"CDC source not explicitly allowlisted: "
            f"{source_system}"
        )

    return contract


def validate_activation(
    source_system: str,
) -> dict[str, Any]:
    source = validate_source_for_cdc(
        source_system
    )

    validate_allowlist(
        source_system
    )

    return source


def current_activation_summary() -> dict[str, Any]:
    registry = source_registry()

    capable = [
        row["source_system"]
        for row in registry["source_systems"]
        if row.get("cdc_capable") is True
    ]

    activated = [
        row["source_system"]
        for row in registry["source_systems"]
        if row.get("cdc_activated") is True
    ]

    contract = load_json(CDC_CONTRACT)

    return {
        "cdc_capable": sorted(capable),
        "cdc_activated": sorted(activated),
        "allowlist_count": len(
            contract.get("cdc_sources", [])
        ),
        "contract_status": contract["status"],
    }
