#!/usr/bin/env python3
"""Generate ICMN VPM and PMN PAIN.001 messages from PDMIS source data."""

from __future__ import annotations

import argparse
from xml.etree import ElementTree as ET

from generator_common import (
    DATA_ROOT,
    PAIN001_NS,
    child,
    clean_directory,
    payment_contexts,
    event_date,
    event_timestamp,
    network_for_account,
    payment_account,
    stable_uetr,
    technical_event_fields,
    validate_xml_no_empty_text,
    write_xml,
)

CREDITOR_AGENT_BIC = {"MTN": "MTNMUGKA", "AIRTEL": "AIRUUGKA"}
CREDITOR_AGENT_NAME = {
    "MTN": "MTN Mobile Money",
    "AIRTEL": "Airtel Money",
}

def beneficiary_network(loan: dict, beneficiary: dict) -> str:
    return network_for_account(payment_account(loan, beneficiary))


def generate_vpm(context: dict) -> bytes:
    loan = context["loan"]
    beneficiary = context["beneficiary"]
    sacco = context["sacco"]
    lifecycle = context["lifecycle"]
    amount = context["amount"]
    loan_id = loan["loan_id"]
    network = beneficiary_network(loan, beneficiary)
    intermediary = context["intermediary"]

    root = ET.Element("Document", xmlns=PAIN001_NS)
    init = child(root, "CstmrCdtTrfInitn")

    header = child(init, "GrpHdr")
    child(header, "MsgId", lifecycle["vpm_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, "verification"))
    child(header, "NbOfTxs", "1")
    child(header, "CtrlSum", amount)
    child(child(header, "InitgPty"), "Nm", "Bank of Uganda")

    pmt_inf = child(init, "PmtInf")
    child(pmt_inf, "PmtInfId", f"PMT-{loan_id}")
    child(pmt_inf, "PmtMtd", "TRF")
    child(pmt_inf, "BtchBookg", "false")
    child(pmt_inf, "NbOfTxs", "1")
    child(pmt_inf, "CtrlSum", amount)
    child(child(pmt_inf, "ReqdExctnDt"), "Dt", event_date(loan, "disbursement"))

    debtor = child(pmt_inf, "Dbtr")
    child(debtor, "Nm", sacco["name"])
    debtor_acct = child(pmt_inf, "DbtrAcct")
    debtor_id = child(child(debtor_acct, "Id"), "Othr")
    child(debtor_id, "Id", sacco["wendi_account"])
    child(child(debtor_id, "SchmeNm"), "Prtry", "WENDI")
    child(debtor_id, "Issr", "WENDI")
    debtor_agent = child(pmt_inf, "DbtrAgt")
    debtor_fi = child(debtor_agent, "FinInstnId")
    child(debtor_fi, "BICFI", intermediary["bic"])
    child(debtor_fi, "Nm", intermediary["name"])
    child(child(pmt_inf, "UltmtDbtr"), "Nm", "Bank of Uganda")

    tx = child(pmt_inf, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "InstrId", f"INSTR-{loan_id}")
    child(pmt_id, "EndToEndId", loan_id)
    child(pmt_id, "UETR", stable_uetr("uetr-vpm", loan_id))

    amt = child(tx, "Amt")
    child(amt, "InstdAmt", amount, Ccy="UGX")

    creditor_agent = child(tx, "CdtrAgt")
    creditor_fi = child(creditor_agent, "FinInstnId")
    child(creditor_fi, "BICFI", CREDITOR_AGENT_BIC[network])
    child(creditor_fi, "Nm", CREDITOR_AGENT_NAME[network])
    creditor = child(tx, "Cdtr")
    child(creditor, "Nm", beneficiary["name"])
    creditor_acct = child(tx, "CdtrAcct")
    creditor_id = child(child(creditor_acct, "Id"), "Othr")
    child(creditor_id, "Id", payment_account(loan, beneficiary))
    child(child(creditor_id, "SchmeNm"), "Prtry", "MSISDN")
    child(creditor_id, "Issr", network)

    child(child(tx, "Purp"), "Prtry", "PDM_DISBURSEMENT")

    remit = child(tx, "RmtInf")
    child(
        remit,
        "Ustrd",
        f"PDM loan {loan_id}; plan {loan['business_plan_id']}; SACCO {sacco['sacco_id']}",
    )

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"ICMN VPM {loan_id}")
    return xml


def generate_pmn(context: dict) -> bytes:
    """Generate a separate technical correlation event, not ISO 20022 XML."""
    loan = context["loan"]
    loan_id = loan["loan_id"]
    root = ET.Element("TechnicalPaymentEvent")
    for name, value in technical_event_fields(context, "PMN").items():
        if value is not None:
            child(root, name, value)

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

    contexts = payment_contexts(args.count)
    for seq, context in enumerate(contexts, 1):
        write_xml(vpm_dir / f"vpm_{seq:04d}.xml", generate_vpm(context))
        write_xml(pmn_dir / f"pmn_{seq:04d}.xml", generate_pmn(context))

    print(f"ICMN generation PASS: {len(contexts)} VPM + {len(contexts)} PMN messages")


if __name__ == "__main__":
    main()
