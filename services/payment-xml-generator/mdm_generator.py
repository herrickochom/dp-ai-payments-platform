#!/usr/bin/env python3

"""
Generate deterministic synthetic MDM source records from the existing
PDM source-system fixtures.

This generator is for source-system simulation only.

It does NOT:
- materialise the protected beneficiary token-link table
- publish to Kafka directly
- perform probabilistic identity matching
- expose beneficiary identity to ordinary analytics
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1] if len(HERE.parents) > 1 else HERE

DATA_ROOT = Path(os.environ.get("DATA_ROOT", PROJECT_ROOT / "data"))
PDMIS_ROOT = DATA_ROOT / "pdmis"
AGENT_ROOT = DATA_ROOT / "agent_network"
MDM_ROOT = DATA_ROOT / "mdm"


def load_json(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text())

    if not isinstance(value, list):
        raise ValueError(f"Expected list: {path}")

    return value


def beneficiary_sources() -> list[dict[str, Any]]:
    rows = load_json(PDMIS_ROOT / "beneficiaries.json")

    return [
        {
            "master_domain": "beneficiary",
            "source_system": "pdmis",
            "source_entity": "beneficiaries",
            "source_record_id": row["beneficiary_id"],
            "beneficiary_id": row["beneficiary_id"],
            "nin": row["nin"],
            "nin_verified": row["nin_verified"],
            "name": row["name"],
            "date_of_birth": row["date_of_birth"],
            "phone": row["phone"],
            "alternative_phone": row["alternative_phone"],
            "email": row["email"],
            "household_id": row["household_id"],
            "region": row["region"],
            "district": row["district"],
            "county": row["county"],
            "sub_county": row["sub_county"],
            "parish": row["parish"],
            "village": row["village"],
            "is_active": row["is_active"],
            "source_created_at": row["created_at"],
            "source_updated_at": row["updated_at"],
        }
        for row in rows
    ]


def sacco_sources() -> list[dict[str, Any]]:
    rows = load_json(PDMIS_ROOT / "saccos.json")

    return [
        {
            "master_domain": "sacco",
            "source_system": "pdmis",
            "source_entity": "saccos",
            "source_record_id": row["sacco_id"],
            "sacco_id": row["sacco_id"],
            "name": row["name"],
            "registration_number": row["registration_number"],
            "wendi_account": row["wendi_account"],
            "region": row["region"],
            "district": row["district"],
            "county": row["county"],
            "sub_county": row["sub_county"],
            "parish": row["parish"],
            "village": row["village"],
            "is_active": row["is_active"],
            "source_created_at": row["created_at"],
            "source_updated_at": row["updated_at"],
        }
        for row in rows
    ]


def agent_sources() -> list[dict[str, Any]]:
    rows = load_json(AGENT_ROOT / "agent_profiles.json")

    return [
        {
            "master_domain": "agent",
            "source_system": "agent_network",
            "source_entity": "agent_profiles",
            "source_record_id": row["agent_id"],
            "agent_id": row["agent_id"],
            "agent_code": row["agent_code"],
            "name": row["name"],
            "phone": row["phone"],
            "registration_number": row["registration_number"],
            "network_provider": row["network_provider"],
            "region": row["region"],
            "district": row["district"],
            "county": row["county"],
            "sub_county": row["sub_county"],
            "parish": row["parish"],
            "village": row["village"],
            "is_active": row["is_active"],
            "verified": row["verified"],
            "source_created_at": row["created_at"],
            "source_updated_at": row["updated_at"],
        }
        for row in rows
    ]


def geography_sources() -> list[dict[str, Any]]:
    beneficiaries = load_json(
        PDMIS_ROOT / "beneficiaries.json"
    )

    seen: set[tuple[str, ...]] = set()
    output: list[dict[str, Any]] = []

    for row in beneficiaries:
        key = (
            row["region"],
            row["district"],
            row["county"],
            row["sub_county"],
            row["parish"],
            row["village"],
        )

        if key in seen:
            continue

        seen.add(key)

        output.append(
            {
                "master_domain": "geography",
                "source_system": "pdmis",
                "source_entity": "location_hierarchy",
                "source_record_id": "|".join(key),
                "location_key": "|".join(key),
                "region": key[0],
                "district": key[1],
                "county": key[2],
                "sub_county": key[3],
                "parish": key[4],
                "village": key[5],
            }
        )

    return output


def write_json(
    name: str,
    records: list[dict[str, Any]],
) -> None:
    MDM_ROOT.mkdir(parents=True, exist_ok=True)

    path = MDM_ROOT / name

    path.write_text(
        json.dumps(
            records,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )

    print(f"WROTE={path}|RECORDS={len(records)}")


def generate() -> None:
    write_json(
        "beneficiary_master_sources.json",
        beneficiary_sources(),
    )

    write_json(
        "sacco_master_sources.json",
        sacco_sources(),
    )

    write_json(
        "agent_master_sources.json",
        agent_sources(),
    )

    write_json(
        "geography_master_sources.json",
        geography_sources(),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic MDM source records."
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate source availability without writing data.",
    )
    parser.add_argument("--target", choices=("postgres", "json"),
                        help="Explicit MDM persistence target")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Load all inputs even in validation mode.
    beneficiary_sources()
    sacco_sources()
    agent_sources()
    geography_sources()

    if args.validate_only:
        print("MDM_SOURCE_VALIDATION=PASS")
        return

    if args.target is None:
        raise SystemExit("Select --target postgres or --target json")

    datasets = build_resolved_mdm()
    if args.target == "json":
        for name, rows in datasets.items():
            write_json(f"{name}.json", rows)
    else:
        from services.shared.mdm.postgres_store import persist_mdm
        persist_mdm(datasets)
        print("MDM_POSTGRES_MATERIALISATION=PASS")

# ------------------------------------------------------------------
# Controlled MDM resolution/materialisation
# ------------------------------------------------------------------

def build_resolved_mdm() -> dict[str, list[dict[str, Any]]]:
    """
    Materialise synthetic MDM fixtures.

    This function deliberately does not create or alter the protected
    beneficiary token-link.
    """
    from services.shared.mdm.resolver import (
        crosswalk_record,
        resolve_beneficiaries,
        resolve_simple_domain,
    )

    beneficiary_rows = beneficiary_sources()
    sacco_rows = sacco_sources()
    agent_rows = agent_sources()
    geography_rows = geography_sources()

    beneficiary_resolution, identity_alerts = (
        resolve_beneficiaries(beneficiary_rows)
    )

    sacco_resolution = resolve_simple_domain(
        "sacco",
        sacco_rows,
    )

    agent_resolution = resolve_simple_domain(
        "agent",
        agent_rows,
    )

    geography_resolution = resolve_simple_domain(
        "geography",
        geography_rows,
    )

    valid_from = "2026-09-18T00:00:00Z"

    all_resolutions = (
        beneficiary_resolution
        + sacco_resolution
        + agent_resolution
        + geography_resolution
    )

    crosswalk = [
        crosswalk_record(
            resolution,
            valid_from=valid_from,
        )
        for resolution in all_resolutions
    ]

    beneficiary_by_source = {
        row["source_record_id"]: row
        for row in beneficiary_rows
    }

    golden_beneficiaries = []

    for resolution in beneficiary_resolution:
        # Ambiguous identities do not become accepted golden persons.
        if resolution.match_status != "MATCHED":
            continue

        source = beneficiary_by_source[
            resolution.source_record_id
        ]

        golden_beneficiaries.append(
            {
                "mdm_beneficiary_id":
                    resolution.golden_record_id,
                "source_system":
                    resolution.source_system,
                "source_record_id":
                    resolution.source_record_id,

                # Restricted identity attributes.
                "nin": source["nin"],
                "nin_verified":
                    source["nin_verified"],
                "name": source["name"],
                "date_of_birth":
                    source["date_of_birth"],
                "phone": source["phone"],
                "alternative_phone":
                    source["alternative_phone"],
                "email": source["email"],

                "household_id":
                    source["household_id"],
                "region": source["region"],
                "district": source["district"],
                "county": source["county"],
                "sub_county":
                    source["sub_county"],
                "parish": source["parish"],
                "village": source["village"],

                "is_active": source["is_active"],
                "source_created_at":
                    source["source_created_at"],
                "source_updated_at":
                    source["source_updated_at"],

                "record_classification":
                    "RESTRICTED_IDENTITY",
            }
        )

    def simple_golden(
        source_rows,
        resolutions,
        golden_key,
    ):
        by_source = {
            row["source_record_id"]: row
            for row in source_rows
        }

        output = []

        for resolution in resolutions:
            source = dict(
                by_source[
                    resolution.source_record_id
                ]
            )

            source[golden_key] = (
                resolution.golden_record_id
            )

            output.append(source)

        return output

    golden_saccos = simple_golden(
        sacco_rows,
        sacco_resolution,
        "mdm_sacco_id",
    )

    golden_agents = simple_golden(
        agent_rows,
        agent_resolution,
        "mdm_agent_id",
    )

    golden_geography = simple_golden(
        geography_rows,
        geography_resolution,
        "mdm_location_id",
    )

    return {
        "beneficiary_master_sources": beneficiary_rows,
        "sacco_master_sources": sacco_rows,
        "agent_master_sources": agent_rows,
        "geography_master_sources": geography_rows,
        "golden_beneficiaries_restricted": golden_beneficiaries,
        "golden_saccos": golden_saccos,
        "golden_agents": golden_agents,
        "golden_geography": golden_geography,
        "source_crosswalk": crosswalk,
        "beneficiary_identity_alerts_restricted": identity_alerts,
    }


def materialise_resolved_mdm() -> None:
    """Retain the explicit legacy JSON export entry point."""
    for name, rows in build_resolved_mdm().items():
        write_json(f"{name}.json", rows)


if __name__ == "__main__":
    main()
