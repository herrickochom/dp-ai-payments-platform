#!/usr/bin/env python3
"""Generate CPO PSN and PLM PAIN.002 status messages from PDMIS data."""

from __future__ import annotations

import argparse
from xml.etree import ElementTree as ET

from common_generator import (
    DATA_ROOT,
    PAIN002_NS,
    STATUS_ACSC,
    STATUS_PDNG,
    child,
    clean_directory,
    eligible_contexts,
    event_timestamp,
    stable_uuid,
    validate_xml_no_empty_text,
    write_xml,
)


def processing_status(status: str) -> str:
    return {
        STATUS_ACSC: "COMPLETED",
        STATUS_PDNG: "PROCESSING",
    }[status]


def generate_psn(context: dict) -> bytes:
    loan = context["loan"]
    lifecycle = context["lifecycle"]
    status = context["status"]
    amount = context["amount"]
    loan_id = loan["loan_id"]

    root = ET.Element("Document", xmlns=PAIN002_NS)
    report = child(root, "CstmrPmtStsRpt")

    header = child(report, "GrpHdr")
    child(header, "MsgId", lifecycle["psn_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, 2))

    group = child(report, "OrgnlGrpInfAndSts")
    child(group, "OrgnlMsgId", lifecycle["vpm_message_id"])
    child(group, "OrgnlMsgNmId", "pain.001.001.09")
    child(group, "OrgnlNbOfTxs", "1")
    child(group, "OrgnlCtrlSum", amount)
    child(group, "GrpSts", status)

    pmt = child(report, "OrgnlPmtInfAndSts")
    child(pmt, "OrgnlPmtInfId", f"PMT-{loan_id}")
    child(pmt, "TxSts", status)
    child(pmt, "OrgnlInstrId", f"INSTR-{loan_id}")
    child(pmt, "OrgnlEndToEndId", loan_id)
    child(pmt, "OrgnlTxId", lifecycle["vpm_transaction_id"])
    child(pmt, "OrgnlUETR", stable_uuid("uetr-vpm", loan_id))

    xml = ET.tostring(root, encoding="utf-8", xml_declaration=True)
    validate_xml_no_empty_text(xml, f"CPO PSN {loan_id}")
    return xml


def add_plm_attrs(parent: ET.Element, context: dict, stage: str) -> None:
    loan = context["loan"]
    status = context["status"]
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
        "x-messageType": "PLM",
        "x-messageVersion": "1.0.0",
        "x-processingNode": "cpo-local-node-01",
        "x-requestId": f"REQ-{loan_id}",
        "x-processingStatus": processing_status(status),
        "x-errorCode": "NONE",
        "x-retryAttempt": "0",
        "x-systemLatency": "350ms",
        "x-priority": "3",
        "x-lifecycleStage": "SETTLED" if status == STATUS_ACSC else "ROUTED",
        "x-eventType": "COMPLETION" if status == STATUS_ACSC else "STATUS_CHANGE",
        "x-acknowledgment": "ACK" if status == STATUS_ACSC else "PENDING",
        "x-retryCount": "0",
        "x-timeout": "120s",
        "x-recovery": "AUTO",
        "x-rollback": "false",
        "x-compensation": "false",
        "x-sagaId": stable_uuid("saga", loan_id),
        "x-transactionId": stable_uuid("transaction", loan_id),
        "x-orchestrator": "cpo-01",
        "x-stepId": stage,
        "x-statusDetail": "SUCCESS" if status == STATUS_ACSC else "PARTIAL",
        "x-resolution": "COMPLETED" if status == STATUS_ACSC else "PENDING",
    }
    for key, value in attrs.items():
        child(parent, key, value)


def generate_plm(context: dict) -> bytes:
    loan = context["loan"]
    lifecycle = context["lifecycle"]
    status = context["status"]
    loan_id = loan["loan_id"]

    root = ET.Element("Document", xmlns=PAIN002_NS)
    report = child(root, "CstmrPmtStsRpt")

    header = child(report, "GrpHdr")
    child(header, "MsgId", lifecycle["plm_message_id"])
    child(header, "CreDtTm", event_timestamp(loan, 2, 11))
    add_plm_attrs(header, context, "HEADER")

    group = child(report, "OrgnlGrpInfAndSts")
    child(group, "OrgnlMsgId", lifecycle["pmn_message_id"])
    child(group, "OrgnlMsgNmId", "pain.001.001.09")
    child(group, "OrgnlNbOfTxs", "1")
    child(group, "GrpSts", status)
    add_plm_attrs(group, context, "GROUP")

    pmt = child(report, "OrgnlPmtInfAndSts")
    child(pmt, "OrgnlPmtInfId", f"PMN-PMT-{loan_id}")
    child(pmt, "TxSts", status)
    add_plm_attrs(pmt, context, "PAYMENT")
    child(pmt, "OrgnlInstrId", f"PMN-INSTR-{loan_id}")
    child(pmt, "OrgnlEndToEndId", loan_id)
    child(pmt, "OrgnlTxId", lifecycle["pmn_transaction_id"])

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

    contexts = eligible_contexts(args.count)
    for seq, context in enumerate(contexts, 1):
        write_xml(psn_dir / f"psn_{seq:04d}.xml", generate_psn(context))
        write_xml(plm_dir / f"plm_{seq:04d}.xml", generate_plm(context))

    print(f"CPO generation PASS: {len(contexts)} PSN + {len(contexts)} PLM messages")


if __name__ == "__main__":
    main()
