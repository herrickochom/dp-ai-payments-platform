#!/usr/bin/env python3
"""Shared helpers for the split PDM synthetic source generators."""

from __future__ import annotations

import json
import hashlib
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

SCENARIO_SUCCESS = "SUCCESSFUL"
SCENARIO_PENDING = "PENDING"
SCENARIO_DELAYED = "DELAYED"
SCENARIO_REJECTED = "REJECTED"
SCENARIO_FAILED = "FAILED"
SCENARIO_VALIDATION = "BENEFICIARY_VALIDATION_FAILED"
SCENARIO_PROVIDER = "DOWNSTREAM_PROVIDER_FAILED"
SCENARIO_AGENT = "AGENT_FINAL_MILE_FAILED"

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
    "Soroti": (1.7146, 33.6111),
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
    validate_no_nulls(payload, str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def write_xml(path: Path, content: bytes) -> None:
    validate_xml_no_empty_text(content, str(path))
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


LIFECYCLE_STAGE_FIELDS: Dict[str, str] = {
    "application": "application_date",
    "approval": "approval_date",
    "verification": "verification_date",
    "disbursement": "disbursement_date",
    "cashout": "cashout_date",
}


def lifecycle_date(loan: Mapping[str, Any], stage: str) -> date:
    """Return an authoritative PDMIS lifecycle date for a downstream event."""
    try:
        field = LIFECYCLE_STAGE_FIELDS[stage]
    except KeyError as exc:
        raise ValueError(f"Unsupported lifecycle stage: {stage}") from exc
    value = loan.get(field)
    if value in (None, "", "NOT_APPLICABLE"):
        raise ValueError(f"{loan.get('loan_id')}: missing lifecycle date {field}")
    return parse_iso_date(str(value))


def event_timestamp(
    loan: Mapping[str, Any],
    stage: str = "approval",
    hour: int = 10,
    minute: int = 0,
) -> str:
    base = lifecycle_date(loan, stage)
    dt = datetime.combine(base, time(hour=hour), tzinfo=UGANDA_EAT)
    dt = dt.replace(minute=minute)
    return dt.isoformat(timespec="seconds")


def event_date(loan: Mapping[str, Any], stage: str = "approval") -> str:
    return lifecycle_date(loan, stage).isoformat()


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


def disbursement_eligible(loan: Mapping[str, Any]) -> bool:
    """Return whether a loan has an authoritative completed disbursement."""
    return (
        str(loan["loan_status"]) == "DISBURSED"
        and loan.get("disbursement_date") not in (None, "", "NOT_APPLICABLE")
        and float(loan.get("amount_disbursed", 0)) > 0
    )


def payment_amount(loan: Mapping[str, Any]) -> int:
    """Return principal actually disbursed; approval amounts are not payments."""
    if not disbursement_eligible(loan):
        return 0
    amount = int(loan["amount_disbursed"])
    return amount


def authoritative_as_of_date(loans: Sequence[Mapping[str, Any]]) -> str:
    """Return the single source observation date, rejecting mixed snapshots."""
    values = {str(loan["as_of_date"]) for loan in loans}
    if len(values) != 1:
        raise ValueError(
            "PDMIS loans must contain exactly one authoritative as_of_date; "
            f"found {sorted(values)}"
        )
    return next(iter(values))


def validate_loan_lifecycle_contract(loan: Mapping[str, Any]) -> None:
    """Reject lifecycle states that could fabricate a funds-movement event."""
    loan_id = str(loan["loan_id"])
    status = str(loan["loan_status"])
    amount_disbursed = float(loan["amount_disbursed"])
    disbursement_value = loan.get("disbursement_date")
    cashout_value = loan.get("cashout_date")

    if status == "DISBURSED":
        if amount_disbursed <= 0:
            raise ValueError(f"{loan_id}: DISBURSED loan must have amount_disbursed > 0")
        lifecycle = [
            lifecycle_date(loan, stage)
            for stage in (
                "application",
                "approval",
                "verification",
                "disbursement",
                "cashout",
            )
        ]
        as_of = parse_iso_date(str(loan["as_of_date"]))
        if lifecycle != sorted(lifecycle) or lifecycle[-1] > as_of:
            raise ValueError(
                f"{loan_id}: lifecycle dates must be ordered and no later than as_of_date"
            )
        return

    if status not in {"APPROVED", "REJECTED"}:
        raise ValueError(f"{loan_id}: unsupported loan_status {status!r}")
    if amount_disbursed != 0:
        raise ValueError(f"{loan_id}: {status} loan must have amount_disbursed = 0")
    if disbursement_value not in (None, "", "NOT_APPLICABLE"):
        raise ValueError(f"{loan_id}: {status} loan cannot have disbursement_date")
    if cashout_value not in (None, "", "NOT_APPLICABLE"):
        raise ValueError(f"{loan_id}: {status} loan cannot have cashout_date")


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


def payment_scenario(loan: Mapping[str, Any]) -> str:
    """Return a deterministic business outcome for an attempted payment."""
    if str(loan["loan_status"]) == "DISBURSED":
        return SCENARIO_SUCCESS
    if str(loan["loan_status"]) != "APPROVED":
        raise ValueError(f"{loan['loan_id']}: no payment exists for a rejected loan")
    return {
        5: SCENARIO_VALIDATION,
        7: SCENARIO_PENDING,
        10: SCENARIO_PROVIDER,
        12: SCENARIO_FAILED,
        14: SCENARIO_DELAYED,
        15: SCENARIO_REJECTED,
        17: SCENARIO_AGENT,
        18: SCENARIO_PENDING,
    }.get(loan_serial(loan) % 20, SCENARIO_PENDING)


def scenario_status(scenario: str) -> str:
    if scenario == SCENARIO_SUCCESS:
        return STATUS_ACSC
    if scenario in {SCENARIO_PENDING, SCENARIO_DELAYED}:
        return STATUS_PDNG
    return STATUS_RJCT


def status_reason(scenario: str) -> Tuple[str, str]:
    """ISO external status reason code and synthetic explanation."""
    return {
        SCENARIO_SUCCESS: ("ACSC", "Payment settled successfully"),
        SCENARIO_PENDING: ("PDNG", "Payment is awaiting provider confirmation"),
        SCENARIO_DELAYED: ("PDNG", "Payment accepted with delayed settlement"),
        SCENARIO_REJECTED: ("MS03", "Payment rejected by the processing institution"),
        SCENARIO_FAILED: ("AG01", "Payment transaction failed during processing"),
        SCENARIO_VALIDATION: ("AC01", "Beneficiary account validation failed"),
        SCENARIO_PROVIDER: ("RR04", "Downstream mobile-money provider unavailable"),
        SCENARIO_AGENT: ("AG01", "Agent or final-mile cash-out failed"),
    }[scenario]


def intermediary_for_loan(loan: Mapping[str, Any]) -> Dict[str, str]:
    """Stable PDM processing institution used across related messages."""
    if loan_serial(loan) % 2:
        return {"name": "Uganda Post Office", "bic": "UGPOUGKA"}
    return {"name": "Pearl Bank Uganda", "bic": "PRBLUGKA"}


def stable_uuid(namespace: str, value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"pdm://{namespace}/{value}"))


def stable_uetr(namespace: str, value: str) -> str:
    """Return a deterministic UUID with the UUIDv4 bit pattern required by UETR."""
    raw = bytearray(hashlib.sha256(f"pdm://{namespace}/{value}".encode()).digest()[:16])
    raw[6] = (raw[6] & 0x0F) | 0x40
    raw[8] = (raw[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(raw)))


def technical_event_fields(context: Mapping[str, Any], event_type: str) -> Dict[str, Any]:
    """Return deterministic, source-owned observability for one payment journey."""
    loan = context["loan"]
    beneficiary = context["beneficiary"]
    loan_id = str(loan["loan_id"])
    serial = loan_serial(loan)
    network = network_for_account(payment_account(loan, beneficiary))
    provider = "MTN_MOMO" if network == "MTN" else "AIRTEL_MONEY"
    intermediary = (
        "UGANDA_POST"
        if context["intermediary"]["name"] == "Uganda Post Office"
        else "PEARL_BANK"
    )

    if event_type == "PMN":
        stage, channel = "payment-routing", "BANK"
        source, target = "BOU_PAYMENT_GATEWAY", intermediary
        service, operation, component = "payment-routing", "route", "pmn-router"
        node, host, hour, latency = "bou-routing-01", "pmn-01", 11, 180 + serial % 70
    elif event_type == "PLM":
        stage, channel = "mobile-money-credit", "MOBILE_MONEY"
        source, target = intermediary, provider
        service, operation, component = "mobile-money-credit", "credit", "plm-provider-adapter"
        node, host, hour, latency = f"{network.lower()}-credit-01", "plm-01", 12, 320 + serial % 130
    else:
        raise ValueError(f"Unsupported technical event type: {event_type}")

    scenario = str(context["scenario"])
    status = "COMPLETED"
    error_code = error_category = None
    timeout = event_type == "PLM" and serial % 11 == 0
    retries = 1 if event_type == "PLM" and serial % 7 == 0 else 0
    if scenario in {SCENARIO_PENDING, SCENARIO_DELAYED}:
        status = scenario
        latency = 45_000 if scenario == SCENARIO_DELAYED else 5_000
    elif scenario == SCENARIO_VALIDATION:
        status, error_code, error_category = "FAILED", "AC01", "VALIDATION"
        stage, service, operation = "payment-validation", "payment-validation", "validate"
    elif scenario == SCENARIO_PROVIDER and event_type == "PLM":
        status, error_code, error_category = "FAILED", "PROVIDER_UNAVAILABLE", "DOWNSTREAM_PROVIDER"
    elif scenario == SCENARIO_AGENT and event_type == "PLM":
        status, error_code, error_category = "FAILED", "AGENT_CASHOUT_FAILED", "FINAL_MILE"
        stage, channel = "agent-cashout", "AGENT_NETWORK"
        source, target = provider, "AGENT_NETWORK"
        service, operation, component = "agent-cashout", "cashout", "plm-agent-adapter"
    elif scenario in {SCENARIO_FAILED, SCENARIO_REJECTED}:
        status, error_code, error_category = "FAILED", "PROCESSING_REJECTED", "PAYMENT_PROCESSING"
    if timeout:
        status, error_code, error_category, latency = "TIMED_OUT", "TECHNICAL_TIMEOUT", "TECHNICAL", 30_000
    elif retries:
        status = "RETRYING" if status == "COMPLETED" else status

    occurred = datetime.fromisoformat(event_timestamp(loan, "disbursement", hour))
    processed = occurred + timedelta(milliseconds=latency)
    event_key = f"{loan_id}/{event_type}/{stage}"
    fields = {
        "EventId": stable_uuid("technical-event", event_key),
        "MessageId": f"{event_type}-{stage.upper()}-{loan_id}",
        "EventFamily": "TECHNICAL_PAYMENT_EVENT",
        "EventType": event_type,
        "CorrelationId": stable_uuid("correlation", loan_id),
        "PaymentInstructionId": f"INSTR-{loan_id}",
        "EndToEndId": loan_id,
        "TransactionId": context["lifecycle"]["vpm_transaction_id"],
        "UETR": stable_uetr("uetr-vpm", loan_id),
        "BusinessReference": loan_id,
        "XTrace": stable_uuid("trace", loan_id),
        "XChannel": channel,
        "XSourceSystem": source,
        "XTargetSystem": target,
        "XService": service,
        "XOperation": operation,
        "XComponent": component,
        "XNode": node,
        "XHost": host,
        "TechnicalStage": stage,
        "TechnicalStatus": status,
        "EventTimestamp": occurred.isoformat(timespec="milliseconds"),
        "ProcessingTimestamp": processed.isoformat(timespec="milliseconds"),
        "XLatencyMs": latency,
        "XRetryCount": retries,
        "XTimeoutIndicator": str(timeout).lower(),
        "XErrorCode": error_code,
        "XErrorCategory": error_category,
    }
    if event_type == "PMN":
        fields.update({
            "XPaymentRoute": f"PDMIS>BOU_PAYMENT_GATEWAY>{intermediary}",
            "XOriginatingInstitution": "Bank of Uganda",
            "XIntermediaryInstitution": context["intermediary"]["name"],
            "XRouteDecision": "REJECTED" if status == "FAILED" else "SELECTED",
            "XValidationStatus": "FAILED" if scenario == SCENARIO_VALIDATION else "VALIDATED",
            "XSubmissionStatus": "REJECTED" if status == "FAILED" else "ACCEPTED",
        })
    else:
        credit_status = "FAILED" if scenario == SCENARIO_PROVIDER else (
            "PENDING" if status in {"PENDING", "DELAYED", "RETRYING", "TIMED_OUT"}
            else "CREDITED"
        )
        cashout_status = "FAILED" if scenario == SCENARIO_AGENT else (
            "COMPLETED" if scenario == SCENARIO_SUCCESS else "NOT_STARTED"
        )
        fields.update({
            "XProvider": "MTN Mobile Money" if network == "MTN" else "Airtel Money",
            "XNetwork": network,
            "XBeneficiarySa": str(beneficiary["beneficiary_id"]),
            "XWalletReference": payment_account(loan, beneficiary),
            "XProviderTransactionId": context["lifecycle"][
                "mtn_transaction_id" if network == "MTN" else "airtel_transaction_id"
            ],
            "XCreditStatus": credit_status,
            "XAgentReference": f"AGENT-{(serial - 1) % 50 + 1:04d}",
            "XCashoutStatus": cashout_status,
        })
    return fields


def network_for_index(index_zero_based: int) -> str:
    return "MTN" if index_zero_based % 2 == 0 else "AIRTEL"


def network_for_account(account: str) -> str:
    """Resolve the synthetic mobile network from the credited MSISDN."""
    digits = "".join(character for character in str(account) if character.isdigit())
    if digits.startswith("25677"):
        return "MTN"
    if digits.startswith("25678"):
        return "AIRTEL"
    raise ValueError(f"Unsupported synthetic mobile-money account: {account!r}")


# ---------------------------------------------------------------------------
# Creditor-account substitution scenarios
# ---------------------------------------------------------------------------
# A small deterministic share of beneficiaries received their PDM wallet on a
# third-party MSISDN (a relative's wallet). Downstream, the payment facts flag
# these as `is_account_substituted` and the beneficiary identity alerts raise
# them to HIGH risk. The rule must be shared by every generator so the XML
# messages and the wallet JSON stay consistent per loan.
SUBSTITUTION_MODULUS = 12
SUBSTITUTED_PHONE_PREFIX = "25678"


def loan_serial(loan: Mapping[str, Any]) -> int:
    try:
        return int(str(loan["loan_id"]).rsplit("-", 1)[-1])
    except (KeyError, ValueError) as exc:
        raise ValueError(
            f"Cannot derive numeric serial from loan_id: {loan.get('loan_id')!r}"
        ) from exc


def is_account_substituted(loan: Mapping[str, Any]) -> bool:
    return loan_serial(loan) % SUBSTITUTION_MODULUS == 0


def payment_account(loan: Mapping[str, Any], beneficiary: Mapping[str, Any]) -> str:
    """Wallet MSISDN that actually received the disbursement for this loan."""
    if is_account_substituted(loan):
        serial = loan_serial(loan)
        account = f"{SUBSTITUTED_PHONE_PREFIX}{serial % 10_000_000:07d}"
        if account != str(beneficiary["phone"]):
            return account
    return str(beneficiary["phone"])


def load_pdmis_context(count: int | None = None) -> List[Dict[str, Any]]:
    beneficiaries = load_json(PDMIS_ROOT / "beneficiaries.json")
    loans = load_json(PDMIS_ROOT / "loans.json")
    saccos = load_json(PDMIS_ROOT / "saccos.json")
    authoritative_as_of_date(loans)

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
                "application_date",
                "verification_date",
                "disbursement_date",
                "cashout_date",
                "amount_requested",
                "amount_approved",
                "amount_disbursed",
                "loan_status",
                "business_plan_id",
                "project_type",
            ),
        )
        validate_loan_lifecycle_contract(loan)

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

        if str(loan["loan_status"]) == "REJECTED":
            continue

        scenario = payment_scenario(loan)
        payment_loan = dict(loan)
        if str(loan["loan_status"]) == "APPROVED":
            planned = lifecycle_date(loan, "verification") + timedelta(days=1)
            payment_loan["disbursement_date"] = planned.isoformat()
            payment_loan["cashout_date"] = (planned + timedelta(days=1)).isoformat()

        contexts.append(
            {
                "loan": payment_loan,
                "beneficiary": beneficiary,
                "sacco": sacco,
                "status": scenario_status(scenario),
                "scenario": scenario,
                "amount": (
                    payment_amount(loan)
                    if disbursement_eligible(loan)
                    else int(loan["amount_approved"])
                ),
                "intermediary": intermediary_for_loan(loan),
                "lifecycle": lifecycle_context(str(loan["loan_id"])),
            }
        )

    if count is not None:
        if count < 1:
            raise ValueError("--count must be at least 1")
        contexts = contexts[:count]

    validate_no_nulls(contexts)
    return contexts


def disbursement_contexts(count: int | None = None) -> List[Dict[str, Any]]:
    """Contexts for records that represent movement of disbursed funds."""
    return [
        context
        for context in load_pdmis_context(count)
        if disbursement_eligible(context["loan"])
    ]


def payment_contexts(count: int | None = None) -> List[Dict[str, Any]]:
    """All payment attempts, including pending and unsuccessful outcomes."""
    return load_pdmis_context(count)


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
