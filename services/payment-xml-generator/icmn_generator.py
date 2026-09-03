#!/usr/bin/env python3
"""Generate ICMN VPM and PMN PAIN.001 messages from PDMIS source data."""

from __future__ import annotations

import argparse
from pathlib import Path
from xml.etree import ElementTree as ET

from common_generator import (
    DATA_ROOT,
    PAIN001_NS,
    child,
    clean_directory,
    eligible_contexts,
    event_date,
    event_timestamp,
    stable_uuid,
    validate_xml_no_empty_text,
    write_xml,
)


def add_technical_attrs(parent: ET.Element, loan_id: str, stage: str) -> None:
    attrs = {
        "x-correlationId": stable_uuid("correlation", loan_id),
        "x-traceId": stable_uuid("trace", loan_id),
        "x-spanId": f"SPAN-{stage}-{loan_id}",
        "x-parentSpanId": f"PARENT-{loan_id}",
        "x-sampled": "1",
        "x-flags": "0x01",
        "x-tenantId": "PDM-UGANDA",
        "x-environment": "development",
        "x-version": "v1.0",
        "x-messageType": stage,
        "x-messageVersion": "1.0.0",
        "x-processingNode": "pdm-local-node-01",
        "x-requestId": f"REQ-{loan_id}",
        "x-processingPriority": "5",
        "x-retryCount": "0",
        "x-timeout": "120s",
        "x-maxRetries": "3",
        "x-backoffDelay": "500ms",
        "x-circuitBreaker": "CLOSED",
        "x-rateLimiter": "50",
        "x-bulkhead": "10",
        "x-retryPolicy": "EXPONENTIAL",
        "x-fallbackEnabled": "true",
        "x-timeoutThreshold": "15000ms",
        "x-queueDepth": "0",
        "x-processingTime": "250ms",
        "x-throughput": "50/s",
        "x-latencyP95": "350ms",
        "x-errorRate": "0.01",
        "x-successRate": "0.99",
        "x-healthStatus": "HEALTHY",
    }
    for key, value in attrs.items():
        child(parent, key, value)


def generate_vpm(context: dict) -> bytes:
    loan = context["loan"]
    beneficiary = context["beneficiary"]
    sacco = context["sacco"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    loan_id = loan["loan_id"]

    root = ET.Element("Document", xmlns=PAIN001_NS)
    init = child(root, "CstmrCdtTrfInitn")

    header = child(init, "GrpHdr")
    child(header, "MsgId", lifecycle["vpm_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, 1))
    child(header, "NbOfTxs", "1")
    child(header, "CtrlSum", amount)

    pmt_inf = child(init, "PmtInf")
    child(pmt_inf, "PmtInfId", f"PMT-{loan_id}")
    child(pmt_inf, "PmtMtd", "TRF")
    child(pmt_inf, "BtchBookg", "false")
    child(pmt_inf, "NbOfTxs", "1")
    child(pmt_inf, "CtrlSum", amount)
    child(pmt_inf, "ReqdExctnDt", event_date(loan, 2))

    debtor = child(pmt_inf, "Dbtr")
    child(debtor, "Nm", sacco["name"])
    debtor_acct = child(pmt_inf, "DbtrAcct")
    debtor_id = child(child(debtor_acct, "Id"), "Othr")
    child(debtor_id, "Id", sacco["wendi_account"])
    child(debtor_id, "Issr", "WENDI")

    tx = child(pmt_inf, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "InstrId", f"INSTR-{loan_id}")
    child(pmt_id, "EndToEndId", loan_id)
    child(pmt_id, "TxId", lifecycle["vpm_transaction_id"])
    child(pmt_id, "UETR", stable_uuid("uetr-vpm", loan_id))

    amt = child(tx, "Amt")
    child(amt, "InstdAmt", amount, Ccy="UGX")

    creditor = child(tx, "Cdtr")
    child(creditor, "Nm", beneficiary["name"])
    creditor_acct = child(tx, "CdtrAcct")
    creditor_id = child(child(creditor_acct, "Id"), "Othr")
    child(creditor_id, "Id", beneficiary["phone"])
    child(creditor_id, "Issr", "MOBILE")
    child(creditor_id, "SchmeNm", "MSISDN")

    remit = child(tx, "RmtInf")
    child(remit, "Ustrd", f"PDM Loan {loan_id} - {loan['project_type']}")

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"ICMN VPM {loan_id}")
    return xml


def generate_pmn(context: dict) -> bytes:
    loan = context["loan"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    loan_id = loan["loan_id"]

    root = ET.Element("Document", xmlns=PAIN001_NS)
    init = child(root, "CstmrCdtTrfInitn")

    header = child(init, "GrpHdr")
    child(header, "MsgId", lifecycle["pmn_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, 1, 11))
    child(header, "NbOfTxs", "1")
    child(header, "CtrlSum", amount)
    add_technical_attrs(header, loan_id, "PMN")

    pmt_inf = child(init, "PmtInf")
    child(pmt_inf, "PmtInfId", f"PMN-PMT-{loan_id}")
    child(pmt_inf, "PmtMtd", "TRF")
    child(pmt_inf, "NbOfTxs", "1")
    child(pmt_inf, "CtrlSum", amount)
    child(pmt_inf, "ReqdExctnDt", event_date(loan, 2))
    add_technical_attrs(pmt_inf, loan_id, "PMN-PMT")

    tx = child(pmt_inf, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "InstrId", f"PMN-INSTR-{loan_id}")
    child(pmt_id, "EndToEndId", loan_id)
    child(pmt_id, "TxId", lifecycle["pmn_transaction_id"])
    add_technical_attrs(tx, loan_id, "PMN-TX")

    amt = child(tx, "Amt")
    child(amt, "InstdAmt", amount, Ccy="UGX")

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"ICMN PMN {loan_id}")
    return xml


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate ICMN source messages.")
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()

    vpm_dir = DATA_ROOT / "icmn" / "vpm" / "pain001"
    pmn_dir = DATA_ROOT / "icmn" / "pmn" / "pain001"

    if args.clean:
        clean_directory(vpm_dir)
        clean_directory(pmn_dir)

    contexts = eligible_contexts(args.count)
    for seq, context in enumerate(contexts, 1):
        write_xml(vpm_dir / f"vpm_{seq:04d}.xml", generate_vpm(context))
        write_xml(pmn_dir / f"pmn_{seq:04d}.xml", generate_pmn(context))

    print(f"ICMN generation PASS: {len(contexts)} VPM + {len(contexts)} PMN messages")


if __name__ == "__main__":
    main()
