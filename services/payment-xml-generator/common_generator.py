#!/usr/bin/env python3
"""Shared helpers for the split PDM synthetic source generators."""

from __future__ import annotations

import json
import os
import shutil
import uuid
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple
from xml.etree import ElementTree as ET


PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        "/home/hochom/projects/dp-ai-payments-platform",
    )
)
DATA_ROOT = Path(os.getenv("DATA_ROOT", str(PROJECT_ROOT / "data")))
PDMIS_ROOT = DATA_ROOT / "pdmis"

PAIN001_NS = "urn:iso:std:iso:20022:tech:xsd:pain.001.001.09"
PAIN002_NS = "urn:iso:std:iso:20022:tech:xsd:pain.002.001.12"
PACS008_NS = "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"
PACS002_NS = "urn:iso:std:iso:20022:tech:xsd:pacs.002.001.10"
CAMT053_NS = "urn:iso:std:iso:20022:tech:xsd:camt.053.001.08"
CAMT052_NS = "urn:iso:std:iso:20022:tech:xsd:camt.052.001.08"
CAMT054_NS = "urn:iso:std:iso:20022:tech:xsd:camt.054.001.08"

STATUS_ACSC = "ACSC"
STATUS_PDNG = "PDNG"
STATUS_RJCT = "RJCT"

UGANDA_EAT = timezone(timedelta(hours=3))

# Synthetic centroids used only for local agent/location testing.
# They are not household addresses.
DISTRICT_COORDINATES: Dict[str, Tuple[float, float]] = {
    "Kampala": (0.3476, 32.5825),
    "Wakiso": (0.3981, 32.4780),
    "Mukono": (0.3533, 32.7553),
    "Mpigi": (0.2250, 32.3136),
    "Kamuli": (0.9472, 33.1197),
    "Kumi": (1.4608, 33.9361),
    "Bukedea": (1.3475, 34.0447),
    "Mbale": (1.0806, 34.1750),
    "Jinja": (0.4479, 33.2026),
    "Iganga": (0.6092, 33.4686),
    "Moroto": (2.5345, 34.6666),
    "Kotido": (2.9806, 34.1331),
    "Nakapiripirit": (1.8500, 34.7200),
    "Napak": (2.2500, 34.2500),
    "Gulu": (2.7746, 32.2990),
    "Lira": (2.2499, 32.8998),
    "Kitgum": (3.2783, 32.8867),
    "Agago": (2.8300, 33.3300),
    "Arua": (3.0201, 30.9111),
    "Adjumani": (3.3779, 31.7909),
    "Yumbe": (3.4651, 31.2469),
    "Nebbi": (2.4783, 31.0889),
    "Hoima": (1.4356, 31.3436),
    "Masindi": (1.6833, 32.7000),
    "Kabarole": (0.6710, 30.2750),
    "Kasese": (0.1833, 30.0833),
    "Masaka": (-0.3338, 31.7341),
    "Rakai": (-0.7029, 31.4099),
    "Mbarara": (-0.6072, 30.6545),
    "Ntungamo": (-0.8794, 30.2642),
    "Kabale": (-1.2486, 29.9899),
    "Rukungiri": (-0.7900, 29.9300),
    "Tororo": (0.6928, 34.1811),
    "Busia": (0.4659, 34.0922),
    "Bugiri": (0.5714, 33.7417),
    "Mayuge": (0.4597, 33.4803),
}


def load_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(
            f"Required source file does not exist: {path}. "
            "Run pdmis_generator.py first."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def write_xml(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def clean_directory(path: Path, suffix: str = ".xml") -> int:
    if not path.exists():
        return 0
    removed = 0
    for candidate in path.glob(f"*{suffix}"):
        candidate.unlink()
        removed += 1
    return removed


def child(
    parent: ET.Element,
    name: str,
    text: Any | None = None,
    **attributes: str,
) -> ET.Element:
    node = ET.SubElement(parent, name, attributes)
    if text is not None:
        node.text = str(text)
    return node


def require_fields(
    dataset: str,
    record: Mapping[str, Any],
    fields: Iterable[str],
) -> None:
    for field in fields:
        if field not in record:
            raise ValueError(f"{dataset}: missing required field '{field}'")
        value = record[field]
        if value is None:
            raise ValueError(f"{dataset}: field '{field}' is NULL")
        if isinstance(value, str) and not value.strip():
            raise ValueError(f"{dataset}: field '{field}' is blank")


def validate_no_nulls(value: Any, path: str = "root") -> None:
    if value is None:
        raise ValueError(f"{path} is NULL")
    if isinstance(value, str) and not value.strip():
        raise ValueError(f"{path} is blank")
    if isinstance(value, dict):
        for key, item in value.items():
            validate_no_nulls(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            validate_no_nulls(item, f"{path}[{index}]")


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def event_timestamp(loan: Mapping[str, Any], offset_days: int = 0, hour: int = 10) -> str:
    base = parse_iso_date(str(loan["approval_date"])) + timedelta(days=offset_days)
    dt = datetime.combine(base, time(hour=hour), tzinfo=UGANDA_EAT)
    return dt.isoformat(timespec="seconds")


def event_date(loan: Mapping[str, Any], offset_days: int = 0) -> str:
    base = parse_iso_date(str(loan["approval_date"])) + timedelta(days=offset_days)
    return base.isoformat()


def iso_payment_status(loan_status: str) -> str:
    mapping = {
        "DISBURSED": STATUS_ACSC,
        "APPROVED": STATUS_PDNG,
        "REJECTED": STATUS_RJCT,
    }
    try:
        return mapping[loan_status]
    except KeyError as exc:
        raise ValueError(f"Unsupported PDMIS loan_status: {loan_status}") from exc


def downstream_eligible(loan: Mapping[str, Any]) -> bool:
    # Rejected applications do not proceed to the payment network.
    return str(loan["loan_status"]) != "REJECTED"


def payment_amount(loan: Mapping[str, Any]) -> int:
    amount = int(loan["amount_requested"])
    if amount <= 0:
        raise ValueError(f"{loan['loan_id']}: payment amount must be > 0")
    return amount


def lifecycle_context(loan_id: str) -> Dict[str, str]:
    return {
        "vpm_message_id": f"ICMN-VPM-{loan_id}",
        "pmn_message_id": f"ICMN-PMN-{loan_id}",
        "vpm_transaction_id": f"TX-{loan_id}",
        "pmn_transaction_id": f"PMN-TX-{loan_id}",
        "psn_message_id": f"CPO-PSN-{loan_id}",
        "plm_message_id": f"CPO-PLM-{loan_id}",
        "wendi_statement_message_id": f"WENDI-STMT-{loan_id}",
        "wendi_notification_message_id": f"WENDI-NTF-{loan_id}",
        "wendi_pain001_message_id": f"WENDI-PAIN001-{loan_id}",
        "wendi_pain002_message_id": f"WENDI-PAIN002-{loan_id}",
        "wendi_tx_id": f"WENDI-TX-{loan_id}",
        "mtn_message_id": f"MTN-PACS008-{loan_id}",
        "mtn_transaction_id": f"MTN-TX-{loan_id}",
        "airtel_message_id": f"AIRTEL-PACS008-{loan_id}",
        "airtel_transaction_id": f"AIRTEL-TX-{loan_id}",
    }


def stable_uuid(namespace: str, value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"pdm://{namespace}/{value}"))


def network_for_index(index_zero_based: int) -> str:
    return "MTN" if index_zero_based % 2 == 0 else "AIRTEL"


def load_pdmis_context(count: int | None = None) -> List[Dict[str, Any]]:
    beneficiaries = load_json(PDMIS_ROOT / "beneficiaries.json")
    loans = load_json(PDMIS_ROOT / "loans.json")
    saccos = load_json(PDMIS_ROOT / "saccos.json")

    beneficiary_by_id = {
        row["beneficiary_id"]: row
        for row in beneficiaries
    }
    sacco_by_id = {
        row["sacco_id"]: row
        for row in saccos
    }

    contexts: List[Dict[str, Any]] = []

    for loan in loans:
        require_fields(
            "loans",
            loan,
            (
                "loan_id",
                "beneficiary_id",
                "sacco_id",
                "approval_date",
                "amount_requested",
                "amount_approved",
                "amount_disbursed",
                "loan_status",
                "business_plan_id",
                "project_type",
            ),
        )

        beneficiary = beneficiary_by_id.get(loan["beneficiary_id"])
        if beneficiary is None:
            raise ValueError(
                f"{loan['loan_id']}: beneficiary not found: {loan['beneficiary_id']}"
            )

        sacco = sacco_by_id.get(loan["sacco_id"])
        if sacco is None:
            raise ValueError(
                f"{loan['loan_id']}: SACCO not found: {loan['sacco_id']}"
            )

        require_fields(
            "beneficiaries",
            beneficiary,
            (
                "beneficiary_id",
                "name",
                "phone",
                "village",
                "parish",
                "sub_county",
                "district",
                "region",
            ),
        )
        require_fields(
            "saccos",
            sacco,
            (
                "sacco_id",
                "name",
                "wendi_account",
                "parish",
                "sub_county",
                "district",
                "region",
            ),
        )

        contexts.append(
            {
                "loan": loan,
                "beneficiary": beneficiary,
                "sacco": sacco,
                "status": iso_payment_status(str(loan["loan_status"])),
                "amount": payment_amount(loan),
                "lifecycle": lifecycle_context(str(loan["loan_id"])),
            }
        )

    if count is not None:
        if count < 1:
            raise ValueError("--count must be at least 1")
        contexts = contexts[:count]

    validate_no_nulls(contexts)
    return contexts


def eligible_contexts(count: int | None = None) -> List[Dict[str, Any]]:
    return [
        context
        for context in load_pdmis_context(count)
        if downstream_eligible(context["loan"])
    ]


def successful_contexts(count: int | None = None) -> List[Dict[str, Any]]:
    return [
        context
        for context in eligible_contexts(count)
        if context["status"] == STATUS_ACSC
    ]


def district_coordinates(district: str) -> Tuple[float, float]:
    try:
        return DISTRICT_COORDINATES[district]
    except KeyError as exc:
        raise ValueError(
            f"No synthetic coordinate configured for district '{district}'"
        ) from exc


def validate_xml_no_empty_text(xml_bytes: bytes, label: str) -> None:
    root = ET.fromstring(xml_bytes)
    for element in root.iter():
        if len(element) == 0:
            text = element.text
            if text is None or not str(text).strip():
                raise ValueError(
                    f"{label}: empty XML leaf element <{element.tag}>"
                )
