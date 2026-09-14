#!/usr/bin/env python3
"""Generate MTN/Airtel PACS.008 and PACS.002 messages from PDMIS data."""

from __future__ import annotations

import argparse
from xml.etree import ElementTree as ET

from generator_common import (
    DATA_ROOT,
    PACS002_NS,
    PACS008_NS,
    child,
    clean_directory,
    payment_contexts,
    event_date,
    event_timestamp,
    network_for_account,
    payment_account,
    stable_uetr,
    status_reason,
    validate_xml_no_empty_text,
    write_xml,
)

CREDITOR_AGENT_BIC = {"MTN": "MTNMUGKA", "AIRTEL": "AIRUUGKA"}
CREDITOR_AGENT_NAME = {
    "MTN": "MTN Mobile Money",
    "AIRTEL": "Airtel Money",
}
UGANDA_POSTAL = {
    "AdrLine": "Plot 37 Kampala Road",
    "TwnNm": "Kampala",
    "CtrySubDvsn": "Central",
    "Ctry": "UG",
    "PstCd": "00256",
}


def add_postal_address(parent: ET.Element) -> None:
    address = child(parent, "PstlAdr")
    for key, value in UGANDA_POSTAL.items():
        child(address, key, value)


def generate_pacs008(context: dict, network: str) -> bytes:
    loan = context["loan"]
    beneficiary = context["beneficiary"]
    sacco = context["sacco"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    loan_id = loan["loan_id"]
    network_key = network.lower()
    intermediary = context["intermediary"]

    root = ET.Element("Document", xmlns=PACS008_NS)
    transfer = child(root, "FIToFICstmrCdtTrf")

    header = child(transfer, "GrpHdr")
    child(header, "MsgId", lifecycle[f"{network_key}_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, "disbursement"))
    child(header, "NbOfTxs", "1")
    settlement = child(header, "SttlmInf")
    child(settlement, "SttlmMtd", "CLRG")

    tx = child(transfer, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "InstrId", f"{network}-INSTR-{loan_id}")
    child(pmt_id, "EndToEndId", loan_id)
    child(pmt_id, "TxId", lifecycle[f"{network_key}_transaction_id"])
    child(pmt_id, "UETR", stable_uetr(f"uetr-{network_key}", loan_id))
    child(tx, "IntrBkSttlmAmt", amount, Ccy="UGX")
    child(tx, "IntrBkSttlmDt", event_date(loan, "disbursement"))
    child(tx, "InstdAmt", amount, Ccy="UGX")
    child(tx, "ChrgBr", "SHAR")

    purpose = child(tx, "Purp")
    child(purpose, "Prtry", "PDM_DISBURSEMENT")

    debtor = child(tx, "Dbtr")
    child(debtor, "Nm", sacco["name"])
    add_postal_address(debtor)
    ultimate_debtor = child(tx, "UltmtDbtr")
    child(ultimate_debtor, "Nm", "Bank of Uganda")
    debtor_acct = child(tx, "DbtrAcct")
    debtor_id = child(child(debtor_acct, "Id"), "Othr")
    child(debtor_id, "Id", sacco["wendi_account"])
    child(debtor_id, "Issr", "WENDI")
    child(debtor_id, "SchmeNm", "WENDI")
    debtor_agent = child(tx, "DbtrAgt")
    debtor_fi = child(debtor_agent, "FinInstnId")
    child(debtor_fi, "BICFI", intermediary["bic"])
    child(debtor_fi, "Nm", intermediary["name"])
    add_postal_address(debtor_fi)

    creditor = child(tx, "Cdtr")
    child(creditor, "Nm", beneficiary["name"])
    add_postal_address(creditor)
    ultimate_creditor = child(tx, "UltmtCdtr")
    child(ultimate_creditor, "Nm", beneficiary["name"])
    creditor_acct = child(tx, "CdtrAcct")
    creditor_id = child(child(creditor_acct, "Id"), "Othr")
    child(creditor_id, "Id", payment_account(loan, beneficiary))
    child(creditor_id, "Issr", network)
    child(creditor_id, "SchmeNm", "MSISDN")
    creditor_agent = child(tx, "CdtrAgt")
    creditor_fi = child(creditor_agent, "FinInstnId")
    child(creditor_fi, "BICFI", CREDITOR_AGENT_BIC[network])
    child(creditor_fi, "Nm", CREDITOR_AGENT_NAME[network])
    add_postal_address(creditor_fi)

    remit = child(tx, "RmtInf")
    child(remit, "Ustrd", f"PDM Loan {loan_id}")
    child(remit, "Ustrd", f"Business plan {loan['business_plan_id']}")

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"{network} PACS008 {loan_id}")
    return xml


def generate_pacs002(context: dict, network: str) -> bytes:
    loan = context["loan"]
    lifecycle = context["lifecycle"]
    status_code = context["status"]
    loan_id = loan["loan_id"]
    network_key = network.lower()

    reason_code, reason_detail = status_reason(context["scenario"])

    root = ET.Element("Document", xmlns=PACS002_NS)
    report = child(root, "FIToFIPmtStsRpt")

    header = child(report, "GrpHdr")
    child(header, "MsgId", f"{network}-PACS002-{loan_id}")
    child(header, "CreDtTm", event_timestamp(loan, "disbursement", 12))

    original_group = child(report, "OrgnlGrpInfAndSts")
    child(original_group, "OrgnlMsgId", lifecycle[f"{network_key}_message_id"])
    child(original_group, "OrgnlMsgNmId", "pacs.008.001.08")

    status = child(report, "TxInfAndSts")
    child(status, "OrgnlInstrId", f"{network}-INSTR-{loan_id}")
    child(status, "OrgnlEndToEndId", loan_id)
    child(status, "OrgnlTxId", lifecycle[f"{network_key}_transaction_id"])
    child(status, "TxSts", status_code)
    child(status, "StsReqId", f"STATUS-{network}-{loan_id}")
    reason_info = child(status, "StsRsnInf")
    child(child(reason_info, "Rsn"), "Cd", reason_code)
    child(reason_info, "AddtlInf", reason_detail)
    instructing_agent = child(status, "InstgAgt")
    instructing_fi = child(instructing_agent, "FinInstnId")
    child(instructing_fi, "BICFI", CREDITOR_AGENT_BIC[network])
    child(instructing_fi, "Nm", CREDITOR_AGENT_NAME[network])
    child(status, "AccptncDtTm", event_timestamp(loan, "disbursement", 12))
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

    contexts = payment_contexts(args.count)
    counts = {"MTN": 0, "AIRTEL": 0}

    for network in counts:
        if args.clean:
            clean_directory(DATA_ROOT / "mobile_networks" / network.lower() / "pacs008")
            clean_directory(DATA_ROOT / "mobile_networks" / network.lower() / "pacs002")

    for index, context in enumerate(contexts):
        network = network_for_account(
            payment_account(context["loan"], context["beneficiary"])
        )
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
