#!/usr/bin/env python3
"""Generate MTN/Airtel PACS.008 and PACS.002 messages from PDMIS data."""

from __future__ import annotations

import argparse
from xml.etree import ElementTree as ET

from generator_common import (
    DATA_ROOT,
    PACS002_NS,
    PACS008_NS,
    STATUS_ACSC,
    child,
    clean_directory,
    disbursement_contexts,
    event_date,
    event_timestamp,
    network_for_account,
    payment_account,
    stable_uuid,
    validate_xml_no_empty_text,
    write_xml,
)

DEBTOR_AGENT_BIC = "PSBL"
DEBTOR_AGENT_NAME = "PostBank Uganda"
CREDITOR_AGENT_BIC = {"MTN": "MTN", "AIRTEL": "AIRT"}
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


def add_technical_attrs(parent: ET.Element, context: dict, stage: str) -> None:
    """Transaction-level x-* technical attributes expected by staging."""
    loan = context["loan"]
    loan_id = loan["loan_id"]
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
        "x-processingNode": "mobile-local-node-01",
        "x-requestId": f"REQ-{loan_id}",
        "x-processingPriority": "5",
        "x-retryCount": "0",
        "x-timeout": "120s",
        "x-timestamp": event_timestamp(loan, "disbursement"),
        "x-deadline": event_timestamp(loan, "cashout"),
    }
    for key, value in attrs.items():
        child(parent, key, value)


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

    root = ET.Element("Document", xmlns=PACS008_NS)
    transfer = child(root, "FIToFICstmrCdtTrf")

    header = child(transfer, "GrpHdr")
    child(header, "MsgId", lifecycle[f"{network_key}_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, "disbursement"))
    child(header, "NbOfTxs", "1")
    child(header, "CtrlSum", amount)
    initg_pty = child(header, "InitgPty")
    child(initg_pty, "Nm", DEBTOR_AGENT_NAME)
    initg_id = child(child(initg_pty, "Id"), "OrgId")
    initg_othr = child(initg_id, "Othr")
    child(initg_othr, "Id", "PSBL-UG-PDM")
    child(initg_othr, "Issr", "PSBL")

    tx = child(transfer, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "InstrId", f"{network}-INSTR-{loan_id}")
    child(pmt_id, "EndToEndId", loan_id)
    child(pmt_id, "TxId", lifecycle[f"{network_key}_transaction_id"])
    child(pmt_id, "UETR", stable_uuid(f"uetr-{network_key}", loan_id))
    child(pmt_id, "ClrSysRef", f"CLR-{network}-{loan_id}")
    add_technical_attrs(tx, context, f"{network}-PACS008")

    amt = child(tx, "Amt")
    child(amt, "InstdAmt", amount, Ccy="UGX")
    child(amt, "EqvtAmt", f"{amount / 3800.0:.2f}", Ccy="USD")
    child(amt, "CntrValAmt", amount, Ccy="UGX")
    child(amt, "ChrgAmt", "2500.00", Ccy="UGX")
    child(tx, "ChrgBr", "SHAR")

    purpose = child(tx, "Purp")
    child(purpose, "Cd", "OTLC")
    child(purpose, "Prtry", "PDM_DISBURSEMENT")

    debtor = child(tx, "Dbtr")
    child(debtor, "Nm", sacco["name"])
    add_postal_address(debtor)
    ultimate_debtor = child(tx, "UltmtDbtr")
    child(ultimate_debtor, "Nm", "PDM Secretariat Uganda")
    debtor_acct = child(tx, "DbtrAcct")
    debtor_id = child(child(debtor_acct, "Id"), "Othr")
    child(debtor_id, "Id", sacco["wendi_account"])
    child(debtor_id, "Issr", "WENDI")
    child(debtor_id, "SchmeNm", "WENDI")
    debtor_agent = child(tx, "DbtrAgt")
    debtor_fi = child(debtor_agent, "FinInstnId")
    child(debtor_fi, "BICFI", DEBTOR_AGENT_BIC)
    child(debtor_fi, "Nm", DEBTOR_AGENT_NAME)
    add_postal_address(debtor_fi)

    creditor = child(tx, "Cdtr")
    child(creditor, "Nm", beneficiary["name"])
    add_postal_address(creditor)
    ultimate_creditor = child(tx, "UltmtCdtr")
    child(ultimate_creditor, "Nm", beneficiary["name"])
    related_creditor = child(child(tx, "RltdPties"), "Cdtr")
    child(related_creditor, "Nm", beneficiary["name"])
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
    structured = child(remit, "Strd")
    child(structured, "CdtrRefInf", f"PDM-{loan['business_plan_id']}")
    referred_document = child(structured, "RfrdDocInf")
    child(child(referred_document, "Tp"), "Cd", "CINV")
    child(referred_document, "Nb", loan["business_plan_id"])
    child(referred_document, "Dt", event_date(loan, "approval"))

    regulatory = child(tx, "RgltryRptg")
    regulatory_info = child(regulatory, "Inf")
    child(child(regulatory_info, "Tp"), "Cd", "PDS1")
    child(regulatory_info, "Id", f"REG-{network}-{loan_id}")
    child(regulatory_info, "Dt", event_date(loan, "disbursement"))
    child(regulatory_info, "Amt", amount, Ccy="UGX")

    supplementary = child(tx, "SplmtryData")
    child(supplementary, "Id", f"SPL-{loan_id}")
    envelope = child(supplementary, "Envlp")
    child(envelope, "Any", f'{{"loan_id":"{loan_id}","network":"{network}"}}')

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"{network} PACS008 {loan_id}")
    return xml


def generate_pacs002(context: dict, network: str) -> bytes:
    loan = context["loan"]
    lifecycle = context["lifecycle"]
    status_code = context["status"]
    loan_id = loan["loan_id"]
    network_key = network.lower()

    if status_code == STATUS_ACSC:
        reason_code, reason_detail = "ACSC", "Settlement completed on the mobile money rail"
    else:
        reason_code, reason_detail = "PDNG", "Settlement pending on the mobile money rail"

    root = ET.Element("Document", xmlns=PACS002_NS)
    report = child(root, "FIToFIPmtStsRpt")

    header = child(report, "GrpHdr")
    child(header, "MsgId", f"{network}-PACS002-{loan_id}")
    child(header, "CreDtTm", event_timestamp(loan, "disbursement", 12))

    original_group = child(report, "OrgnlGrpInfAndSts")
    child(original_group, "OrgnlMsgId", lifecycle[f"{network_key}_message_id"])
    child(original_group, "OrgnlMsgNmId", "pacs.008.001.08")

    status = child(report, "TxInfAndSts")
    original_end = child(status, "OrgnlEndToEndId")
    child(original_end, "EndToEndId", loan_id)
    original_tx = child(status, "OrgnlTxId")
    child(original_tx, "TxId", lifecycle[f"{network_key}_transaction_id"])
    child(status, "TxSts", status_code)
    child(status, "StsReqId", f"STATUS-{network}-{loan_id}")
    status_reason = child(status, "StsRsnInf")
    child(child(status_reason, "Rsn"), "Cd", reason_code)
    child(status_reason, "AddtlInf", reason_detail)
    instructing_agent = child(status, "InstgAgt")
    instructing_fi = child(instructing_agent, "FinInstnId")
    child(instructing_fi, "BICFI", CREDITOR_AGENT_BIC[network])
    child(instructing_fi, "Nm", CREDITOR_AGENT_NAME[network])
    add_technical_attrs(status, context, f"{network}-PACS002")

    settlement = child(status, "SttlmInf")
    method = child(settlement, "SttlmMtd")
    child(method, "Cd", "CLRG")
    child(settlement, "ClrSys", "UNIS")
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

    contexts = disbursement_contexts(args.count)
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
