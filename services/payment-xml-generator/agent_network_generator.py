#!/usr/bin/env python3
"""Generate Wendi agent profiles, locations and successful cash-out XML."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from xml.etree import ElementTree as ET

from common_generator import (
    DATA_ROOT,
    PDMIS_ROOT,
    child,
    clean_directory,
    district_coordinates,
    event_timestamp,
    load_json,
    successful_contexts,
    validate_no_nulls,
    validate_xml_no_empty_text,
    write_json,
    write_xml,
)


def stable_fraction(value: str, scale: float) -> float:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    integer = int.from_bytes(digest[:4], "big")
    normalized = (integer / 0xFFFFFFFF) - 0.5
    return normalized * scale


def generate_roster() -> dict:
    saccos = load_json(PDMIS_ROOT / "saccos.json")
    profiles = []
    locations = []

    for index, sacco in enumerate(saccos, 1):
        district = sacco["district"]
        latitude, longitude = district_coordinates(district)
        agent_id = f"AGENT-{index:04d}"
        phone = f"25676{index:07d}"

        profiles.append(
            {
                "agent_id": agent_id,
                "agent_code": f"A-{sacco['sacco_id']}-{index:03d}",
                "name": f"Wendi Agent {sacco['parish']} {index}",
                "phone": phone,
                "registration_number": f"AGT-REG-{index:05d}",
                "registration_date": "2025-01-15",
                "network_provider": "WENDI",
                "commission_rate": round(0.015 + ((index % 7) * 0.0025), 4),
                "location": f"{sacco['parish']}, {district}, Uganda",
                "parish": sacco["parish"],
                "district": district,
                "region": sacco["region"],
                "verified": True,
                "is_active": True,
                "created_at": "2025-01-15T09:00:00+03:00",
                "updated_at": "2026-09-01T09:00:00+03:00",
            }
        )

        locations.append(
            {
                "agent_id": agent_id,
                "location_type": ("STORE", "KIOSK", "MOBILE_POINT", "SHOP")[
                    (index - 1) % 4
                ],
                "address": f"{sacco['parish']}, {district}, Uganda",
                "latitude": round(
                    latitude + stable_fraction(f"{agent_id}-lat", 0.03),
                    6,
                ),
                "longitude": round(
                    longitude + stable_fraction(f"{agent_id}-lon", 0.03),
                    6,
                ),
                "is_active": True,
            }
        )

    validate_no_nulls(profiles, "agent_profiles")
    validate_no_nulls(locations, "agent_locations")
    return {"profiles": profiles, "locations": locations}


def agent_by_sacco(roster: dict) -> dict:
    # SACCOs and agents are generated in the same stable order.
    saccos = load_json(PDMIS_ROOT / "saccos.json")
    return {
        sacco["sacco_id"]: roster["profiles"][index]["agent_id"]
        for index, sacco in enumerate(saccos)
    }


def generate_transaction(context: dict, agent_id: str) -> bytes:
    loan = context["loan"]
    beneficiary = context["beneficiary"]
    amount = context["amount"]
    loan_id = loan["loan_id"]
    district = beneficiary["district"]
    latitude, longitude = district_coordinates(district)

    cashout_amount = int(round(amount * 0.95))
    fee_amount = (0, 1000, 2000, 5000)[int(loan_id.split("-")[-1]) % 4]

    root = ET.Element("AgentTransaction")
    child(root, "transaction_id", f"AGT-TX-{loan_id}")
    child(root, "agent_id", agent_id)
    child(root, "loan_id", loan_id)
    child(root, "beneficiary_id", beneficiary["beneficiary_id"])
    child(root, "beneficiary_name", beneficiary["name"])
    child(root, "transaction_timestamp", event_timestamp(loan, 4))
    child(root, "transaction_type", "WITHDRAWAL")
    child(root, "amount", cashout_amount)
    child(root, "fee_amount", fee_amount)
    child(root, "is_assisted_withdrawal", "false")
    child(root, "beneficiary_verified", "true")
    child(root, "verification_method", "BIOMETRIC")
    child(root, "status", "COMPLETED")
    child(root, "reference", f"REF-{loan_id}")

    location = child(root, "location")
    child(location, "latitude", round(latitude + stable_fraction(f"{loan_id}-lat", 0.02), 6))
    child(location, "longitude", round(longitude + stable_fraction(f"{loan_id}-lon", 0.02), 6))
    child(
        location,
        "address",
        f"{beneficiary['parish']}, {beneficiary['district']}, Uganda",
    )

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"Agent transaction {loan_id}")
    return xml


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate agent-network source data.")
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()

    output_root = DATA_ROOT / "agent_network"
    tx_dir = output_root / "agent_transactions"

    if args.clean:
        clean_directory(tx_dir)

    roster = generate_roster()
    write_json(output_root / "agent_profiles.json", roster["profiles"])
    write_json(output_root / "agent_locations.json", roster["locations"])

    sacco_agents = agent_by_sacco(roster)
    contexts = successful_contexts(args.count)

    for seq, context in enumerate(contexts, 1):
        agent_id = sacco_agents[context["loan"]["sacco_id"]]
        write_xml(
            tx_dir / f"agent_tx_{seq:04d}.xml",
            generate_transaction(context, agent_id),
        )

    print(
        "Agent Network generation PASS: "
        f"{len(roster['profiles'])} profiles, "
        f"{len(roster['locations'])} locations, "
        f"{len(contexts)} cash-out transactions"
    )


if __name__ == "__main__":
    main()
