#!/usr/bin/env python3
"""Generate CPO PSN and PLM PAIN.002 status messages from PDMIS data."""

from __future__ import annotations

import argparse
from xml.etree import ElementTree as ET

from generator_common import (
    DATA_ROOT,
    PAIN002_NS,
    child,
    clean_directory,
    payment_contexts,
    event_timestamp,
    stable_uetr,
    status_reason,
    technical_event_fields,
    validate_xml_no_empty_text,
    write_xml,
)

def generate_psn(context: dict) -> bytes:
    loan = context["loan"]
    lifecycle = context["lifecycle"]
    status = context["status"]
    amount = context["amount"]
    loan_id = loan["loan_id"]
    reason_code, reason_detail = status_reason(context["scenario"])

    root = ET.Element("Document", xmlns=PAIN002_NS)
    report = child(root, "CstmrPmtStsRpt")

    header = child(report, "GrpHdr")
    child(header, "MsgId", lifecycle["psn_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, "disbursement"))
    child(child(header, "InitgPty"), "Nm", context["intermediary"]["name"])

    group = child(report, "OrgnlGrpInfAndSts")
    child(group, "OrgnlMsgId", lifecycle["vpm_message_id"])
    child(group, "OrgnlMsgNmId", "pain.001.001.09")
    child(group, "OrgnlNbOfTxs", "1")
    child(group, "OrgnlCtrlSum", amount)
    child(group, "GrpSts", status)

    pmt = child(report, "OrgnlPmtInfAndSts")
    child(pmt, "OrgnlPmtInfId", f"PMT-{loan_id}")
    child(pmt, "TxSts", status)
    reason = child(pmt, "StsRsnInf")
    child(child(reason, "Rsn"), "Cd", reason_code)
    child(reason, "AddtlInf", reason_detail)
    child(pmt, "OrgnlInstrId", f"INSTR-{loan_id}")
    child(pmt, "OrgnlEndToEndId", loan_id)
    child(pmt, "OrgnlTxId", lifecycle["vpm_transaction_id"])
    child(pmt, "OrgnlUETR", stable_uetr("uetr-vpm", loan_id))

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"CPO PSN {loan_id}")
    return xml


def generate_plm(context: dict) -> bytes:
    """Generate a separate technical correlation event, not ISO 20022 XML."""
    loan = context["loan"]
    loan_id = loan["loan_id"]
    root = ET.Element("TechnicalPaymentEvent")
    for name, value in technical_event_fields(context, "PLM").items():
        if value is not None:
            child(root, name, value)

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"CPO PLM {loan_id}")
    return xml


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate CPO source messages.")
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--clean", action="store_true")
    args = parser.parse_args()

    psn_dir = DATA_ROOT / "cpo" / "psn" / "pain002"
    plm_dir = DATA_ROOT / "cpo" / "plm" / "pain002"

    if args.clean:
        clean_directory(psn_dir)
        clean_directory(plm_dir)

    contexts = payment_contexts(args.count)
    for seq, context in enumerate(contexts, 1):
        write_xml(psn_dir / f"psn_{seq:04d}.xml", generate_psn(context))
        write_xml(plm_dir / f"plm_{seq:04d}.xml", generate_plm(context))

    print(f"CPO generation PASS: {len(contexts)} PSN + {len(contexts)} PLM messages")


if __name__ == "__main__":
    main()
