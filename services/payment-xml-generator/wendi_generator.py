#!/usr/bin/env python3
"""Generate Wendi PAIN/CAMT XML and wendi_transactions.json from PDMIS data."""

from __future__ import annotations

import argparse
from collections import defaultdict
from xml.etree import ElementTree as ET

from common_generator import (
    CAMT052_NS,
    CAMT053_NS,
    CAMT054_NS,
    DATA_ROOT,
    PAIN001_NS,
    PAIN002_NS,
    PDMIS_ROOT,
    STATUS_ACSC,
    STATUS_PDNG,
    child,
    clean_directory,
    eligible_contexts,
    event_date,
    event_timestamp,
    load_json,
    stable_uuid,
    validate_no_nulls,
    validate_xml_no_empty_text,
    write_json,
    write_xml,
)


def wallet_status(status: str) -> str:
    return {
        STATUS_ACSC: "SUCCESSFUL",
        STATUS_PDNG: "PENDING",
    }[status]


def generate_pain001(context: dict) -> bytes:
    loan = context["loan"]
    beneficiary = context["beneficiary"]
    sacco = context["sacco"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    loan_id = loan["loan_id"]

    root = ET.Element("Document", xmlns=PAIN001_NS)
    init = child(root, "CstmrCdtTrfInitn")

    header = child(init, "GrpHdr")
    child(header, "MsgId", lifecycle["wendi_pain001_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, 2, 14))
    child(header, "NbOfTxs", "1")
    child(header, "CtrlSum", amount)

    pmt_inf = child(init, "PmtInf")
    child(pmt_inf, "PmtInfId", f"WENDI-PMT-{loan_id}")
    child(pmt_inf, "PmtMtd", "TRF")
    child(pmt_inf, "BtchBookg", "false")
    child(pmt_inf, "NbOfTxs", "1")
    child(pmt_inf, "CtrlSum", amount)
    child(pmt_inf, "ReqdExctnDt", event_date(loan, 3))

    debtor = child(pmt_inf, "Dbtr")
    child(debtor, "Nm", sacco["name"])
    debtor_acct = child(pmt_inf, "DbtrAcct")
    debtor_id = child(child(debtor_acct, "Id"), "Othr")
    child(debtor_id, "Id", sacco["wendi_account"])
    child(debtor_id, "Issr", "WENDI")

    tx = child(pmt_inf, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "InstrId", f"WENDI-INSTR-{loan_id}")
    child(pmt_id, "EndToEndId", loan_id)
    child(pmt_id, "TxId", lifecycle["wendi_tx_id"])
    child(pmt_id, "UETR", stable_uuid("uetr-wendi", loan_id))

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
    validate_xml_no_empty_text(xml, f"Wendi PAIN001 {loan_id}")
    return xml


def generate_pain002(context: dict) -> bytes:
    loan = context["loan"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    status = context["status"]
    loan_id = loan["loan_id"]

    root = ET.Element("Document", xmlns=PAIN002_NS)
    report = child(root, "CstmrPmtStsRpt")

    header = child(report, "GrpHdr")
    child(header, "MsgId", lifecycle["wendi_pain002_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, 3))

    group = child(report, "OrgnlGrpInfAndSts")
    child(group, "OrgnlMsgId", lifecycle["wendi_pain001_message_id"])
    child(group, "OrgnlMsgNmId", "pain.001.001.09")
    child(group, "OrgnlNbOfTxs", "1")
    child(group, "OrgnlCtrlSum", amount)
    child(group, "GrpSts", status)

    pmt = child(report, "OrgnlPmtInfAndSts")
    child(pmt, "OrgnlPmtInfId", f"WENDI-PMT-{loan_id}")
    child(pmt, "TxSts", status)
    child(pmt, "OrgnlInstrId", f"WENDI-INSTR-{loan_id}")
    child(pmt, "OrgnlEndToEndId", loan_id)
    child(pmt, "OrgnlTxId", lifecycle["wendi_tx_id"])
    child(pmt, "OrgnlUETR", stable_uuid("uetr-wendi", loan_id))

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"Wendi PAIN002 {loan_id}")
    return xml


def generate_camt053(context: dict) -> bytes:
    loan = context["loan"]
    sacco = context["sacco"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    loan_id = loan["loan_id"]

    root = ET.Element("Document", xmlns=CAMT053_NS)
    stmt = child(root, "BkToCstmrStmt")
    header = child(stmt, "GrpHdr")
    child(header, "MsgId", lifecycle["wendi_statement_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, 4))

    statement = child(stmt, "Stmt")
    child(statement, "Id", f"STMT-{loan_id}-{event_date(loan, 4)}")
    child(statement, "CreDtTm", event_timestamp(loan, 4))

    account = child(statement, "Acct")
    other = child(child(account, "Id"), "Othr")
    child(other, "Id", sacco["wendi_account"])
    child(other, "Issr", "WENDI")

    opening = child(statement, "Bal")
    child(child(opening, "Tp"), "CdOrPrtry", "OPBD")
    child(opening, "Amt", "0.00", Ccy="UGX")
    child(opening, "CdtDbtInd", "CRDT")
    child(opening, "Dt", event_date(loan, 3))

    closing = child(statement, "Bal")
    child(child(closing, "Tp"), "CdOrPrtry", "CLBD")
    child(closing, "Amt", amount, Ccy="UGX")
    child(closing, "CdtDbtInd", "CRDT")
    child(closing, "Dt", event_date(loan, 4))

    entry = child(statement, "Ntry")
    child(entry, "NtryRef", f"NTRY-{loan_id}")
    child(entry, "Amt", amount, Ccy="UGX")
    child(entry, "CdtDbtInd", "CRDT")
    child(entry, "Sts", "BOOK")
    details = child(entry, "NtryDtls")
    tx_details = child(details, "TxDtls")
    refs = child(tx_details, "Refs")
    child(refs, "EndToEndId", loan_id)
    child(refs, "TxId", lifecycle["vpm_transaction_id"])
    remit = child(tx_details, "RmtInf")
    child(remit, "Ustrd", f"PDM Loan {loan_id} - {loan['project_type']}")

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"Wendi CAMT053 {loan_id}")
    return xml


def generate_camt054(context: dict) -> bytes:
    loan = context["loan"]
    beneficiary = context["beneficiary"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    loan_id = loan["loan_id"]

    root = ET.Element("Document", xmlns=CAMT054_NS)
    notif = child(root, "BkToCstmrDbtCdtNtfctn")
    header = child(notif, "GrpHdr")
    child(header, "MsgId", lifecycle["wendi_notification_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, 4))

    notification = child(notif, "Ntfctn")
    child(notification, "Id", f"NTF-{loan_id}")
    child(notification, "CreDtTm", event_timestamp(loan, 4))

    account = child(notification, "Acct")
    other = child(child(account, "Id"), "Othr")
    child(other, "Id", beneficiary["phone"])
    child(other, "Issr", "WENDI")

    entry = child(notification, "Ntry")
    child(entry, "Amt", amount, Ccy="UGX")
    child(entry, "CdtDbtInd", "CRDT")
    child(entry, "Sts", "BOOK")
    tx_details = child(child(entry, "NtryDtls"), "TxDtls")
    refs = child(tx_details, "Refs")
    child(refs, "EndToEndId", loan_id)
    child(refs, "TxId", lifecycle["vpm_transaction_id"])
    remit = child(tx_details, "RmtInf")
    child(remit, "Ustrd", f"PDM Loan {loan_id} disbursement to {beneficiary['name']}")

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"Wendi CAMT054 {loan_id}")
    return xml


def generate_camt052(sacco: dict, total: int) -> bytes:
    root = ET.Element("Document", xmlns=CAMT052_NS)
    report = child(root, "BkToCstmrAcctRpt")
    header = child(report, "GrpHdr")
    child(header, "MsgId", f"WENDI-INTRA-{sacco['sacco_id']}")
    child(header, "CreDtTm", "2026-09-03T17:00:00+03:00")

    account = child(report, "Acct")
    other = child(child(account, "Id"), "Othr")
    child(other, "Id", sacco["wendi_account"])
    child(other, "Issr", "WENDI")

    balance = child(report, "Bal")
    child(child(balance, "Tp"), "CdOrPrtry", "XPCD")
    child(balance, "Amt", f"{total:.2f}", Ccy="UGX")
    child(balance, "CdtDbtInd", "CRDT")
    child(balance, "Dt", "2026-09-03")

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"Wendi CAMT052 {sacco['sacco_id']}")
    return xml


def transaction_record(context: dict, agent_id: str) -> dict:
    loan = context["loan"]
    beneficiary = context["beneficiary"]
    sacco = context["sacco"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    status = context["status"]
    loan_id = loan["loan_id"]

    numeric = int(loan_id.split("-")[-1])
    return {
        "event_id": f"WENDI-EVT-{loan_id}",
        "loan_id": loan_id,
        "beneficiary_id": beneficiary["beneficiary_id"],
        "beneficiary_name": beneficiary["name"],
        "sacco_id": sacco["sacco_id"],
        "event_timestamp": event_timestamp(loan, 3, 13),
        "event_type": "DISBURSEMENT",
        "source_account": sacco["wendi_account"],
        "source_account_type": "WALLET",
        "destination_account": beneficiary["phone"],
        "destination_account_type": "MSISDN",
        "amount": amount,
        "currency": "UGX",
        "transaction_status": wallet_status(status),
        "wendi_transaction_id": lifecycle["wendi_tx_id"],
        "agent_id": agent_id,
        "device_id": f"DEV-{numeric:08d}",
        "ip_address": f"41.210.{numeric % 255}.{(numeric % 253) + 1}",
        "user_agent": "Walletek/2.4.1 (Android 14)",
        "metadata": {
            "source_system": "WENDI",
            "transaction_type": "PAYMENT",
            "is_assisted": False,
            "processing_time_ms": 200 + (numeric % 1800),
        },
        "created_at": event_timestamp(loan, 3, 13),
        "updated_at": event_timestamp(loan, 3, 14),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Wendi source data.")
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()

    dirs = {
        "pain001": DATA_ROOT / "wendi" / "pain001",
        "pain002": DATA_ROOT / "wendi" / "pain002",
        "camt052": DATA_ROOT / "wendi" / "camt052",
        "camt053": DATA_ROOT / "wendi" / "camt053",
        "camt054": DATA_ROOT / "wendi" / "camt054",
    }

    if args.clean:
        for directory in dirs.values():
            clean_directory(directory)

    contexts = eligible_contexts(args.count)
    saccos = load_json(PDMIS_ROOT / "saccos.json")
    sacco_index = {row["sacco_id"]: idx + 1 for idx, row in enumerate(saccos)}
    totals = defaultdict(int)
    transactions = []
    success_count = 0

    for seq, context in enumerate(contexts, 1):
        write_xml(dirs["pain001"] / f"pain001_{seq:04d}.xml", generate_pain001(context))
        write_xml(dirs["pain002"] / f"pain002_{seq:04d}.xml", generate_pain002(context))

        agent_id = f"AGENT-{sacco_index[context['sacco']['sacco_id']]:04d}"
        transactions.append(transaction_record(context, agent_id))

        if context["status"] == STATUS_ACSC:
            success_count += 1
            totals[context["sacco"]["sacco_id"]] += context["amount"]
            write_xml(dirs["camt053"] / f"camt053_{seq:04d}.xml", generate_camt053(context))
            write_xml(dirs["camt054"] / f"camt054_{seq:04d}.xml", generate_camt054(context))

    for report_number, sacco in enumerate(saccos, 1):
        write_xml(
            dirs["camt052"] / f"camt052_report_{report_number:03d}.xml",
            generate_camt052(sacco, totals[sacco["sacco_id"]]),
        )

    validate_no_nulls(transactions, "wendi_transactions")
    write_json(DATA_ROOT / "wendi" / "wendi_transactions.json", transactions)

    print(
        "Wendi generation PASS: "
        f"{len(contexts)} PAIN001, {len(contexts)} PAIN002, "
        f"{success_count} CAMT053, {success_count} CAMT054, "
        f"{len(saccos)} CAMT052, {len(transactions)} transaction records"
    )


if __name__ == "__main__":
    main()
