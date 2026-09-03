#!/usr/bin/env python3
"""Generate MTN/Airtel PACS.008 and PACS.002 messages from PDMIS data."""

from __future__ import annotations

import argparse
from xml.etree import ElementTree as ET

from common_generator import (
    DATA_ROOT,
    PACS002_NS,
    PACS008_NS,
    STATUS_ACSC,
    child,
    clean_directory,
    eligible_contexts,
    event_timestamp,
    network_for_index,
    validate_xml_no_empty_text,
    write_xml,
)


def generate_pacs008(context: dict, network: str) -> bytes:
    loan = context["loan"]
    beneficiary = context["beneficiary"]
    sacco = context["sacco"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    loan_id = loan["loan_id"]
    network_key = network.lower()

    root = ET.Element("Document", xmlns=PACS008_NS)
    transfer = child(root, "FIToFICstmrCdtTrf")

    header = child(transfer, "GrpHdr")
    child(header, "MsgId", lifecycle[f"{network_key}_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, 3))
    child(header, "NbOfTxs", "1")
    child(header, "CtrlSum", amount)

    tx = child(transfer, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "EndToEndId", loan_id)
    child(pmt_id, "TxId", lifecycle[f"{network_key}_transaction_id"])

    amt = child(tx, "Amt")
    child(amt, "InstdAmt", amount, Ccy="UGX")

    debtor_agent = child(tx, "DbtrAgt")
    debtor_fi = child(debtor_agent, "FinInstnId")
    child(debtor_fi, "BICFI", "PSBL")
    child(debtor_fi, "Nm", "PostBank Uganda")

    creditor_agent = child(tx, "CdtrAgt")
    creditor_fi = child(creditor_agent, "FinInstnId")
    child(creditor_fi, "BICFI", "MTN" if network == "MTN" else "AIRT")
    child(creditor_fi, "Nm", f"{network} Mobile Money")

    debtor = child(tx, "Dbtr")
    child(debtor, "Nm", sacco["name"])
    debtor_acct = child(tx, "DbtrAcct")
    debtor_id = child(child(debtor_acct, "Id"), "Othr")
    child(debtor_id, "Id", sacco["wendi_account"])
    child(debtor_id, "Issr", "WENDI")

    creditor = child(tx, "Cdtr")
    child(creditor, "Nm", beneficiary["name"])
    creditor_acct = child(tx, "CdtrAcct")
    creditor_id = child(child(creditor_acct, "Id"), "Othr")
    child(creditor_id, "Id", beneficiary["phone"])
    child(creditor_id, "Issr", network)
    child(creditor_id, "SchmeNm", "MSISDN")

    remit = child(tx, "RmtInf")
    child(remit, "Ustrd", f"PDM Loan {loan_id}")

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"{network} PACS008 {loan_id}")
    return xml


def generate_pacs002(context: dict, network: str) -> bytes:
    loan = context["loan"]
    lifecycle = context["lifecycle"]
    status_code = context["status"]
    loan_id = loan["loan_id"]
    network_key = network.lower()

    root = ET.Element("Document", xmlns=PACS002_NS)
    report = child(root, "FIToFIPmtStsRpt")

    header = child(report, "GrpHdr")
    child(header, "MsgId", f"{network}-PACS002-{loan_id}")
    child(header, "CreDtTm", event_timestamp(loan, 3, 12))

    status = child(report, "TxInfAndSts")
    original_end = child(status, "OrgnlEndToEndId")
    child(original_end, "EndToEndId", loan_id)
    original_tx = child(status, "OrgnlTxId")
    child(original_tx, "TxId", lifecycle[f"{network_key}_transaction_id"])
    child(status, "TxSts", status_code)
    child(status, "StsReqId", f"STATUS-{network}-{loan_id}")

    settlement = child(status, "SttlmInf")
    method = child(settlement, "SttlmMtd")
    child(method, "Cd", "CLRG")
    child(settlement, "ClrSys", "UNIS")
    child(status, "AccptncDtTm", event_timestamp(loan, 3, 12))
    child(status, "AcctSvcrRef", f"ASR-{network}-{loan_id}")
    child(status, "ClrSysRef", f"CLR-{network}-{loan_id}")

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"{network} PACS002 {loan_id}")
    return xml


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate mobile-network source messages.")
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()

    contexts = eligible_contexts(args.count)
    counts = {"MTN": 0, "AIRTEL": 0}

    for network in counts:
        if args.clean:
            clean_directory(DATA_ROOT / "mobile_networks" / network.lower() / "pacs008")
            clean_directory(DATA_ROOT / "mobile_networks" / network.lower() / "pacs002")

    for index, context in enumerate(contexts):
        network = network_for_index(index)
        counts[network] += 1
        seq = index + 1
        base = DATA_ROOT / "mobile_networks" / network.lower()
        write_xml(
            base / "pacs008" / f"pacs008_{network.lower()}_{seq:04d}.xml",
            generate_pacs008(context, network),
        )
        write_xml(
            base / "pacs002" / f"pacs002_{network.lower()}_{seq:04d}.xml",
            generate_pacs002(context, network),
        )

    print(
        "Mobile generation PASS: "
        f"MTN={counts['MTN']} settlement/status pairs, "
        f"AIRTEL={counts['AIRTEL']} settlement/status pairs"
    )


if __name__ == "__main__":
    main()
