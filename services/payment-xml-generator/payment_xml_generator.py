#!/usr/bin/env python3
"""
Synthetic ISO20022 Payment Event Generator (Dev Scale)
Generates XML files for:
- ICMM/VPM/PAIN.001 - Business Payment Initiation
- ICMM/PMN/PAIN.001 - Technical Lifecycle (x-* attributes)
- CPO/PSN/PAIN.002 - Business Status Notification
- CPO/PLM/PAIN.002 - Technical Lifecycle (x-* attributes)

Each system has different message structures and attributes:
- VPM: Standard PAIN.001 with business fields
- PMN: Technical PAIN.001 with x-* attributes (correlationId, traceId, etc.)
- PSN: Standard PAIN.002 with status fields
- PLM: Technical PAIN.002 with x-* attributes (errorCode, retryCount, etc.)
"""

import os
import random
import uuid
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from pathlib import Path
import argparse
import logging

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
# Use DATA_ROOT environment variable if set, otherwise fallback to default
DATA_ROOT = os.getenv("DATA_ROOT", "/home/hochom/projects/dp-ai-payments-platform/data")
BASE_DIR = Path(DATA_ROOT)

# Payment Systems Configuration
PAYMENT_SYSTEMS = {
    "vpm": {
        "category": "icmn",
        "msg_type": "pain001",
        "description": "Virtual Payment Message - Business Initiation",
        "has_technical_attrs": False,
        "xmlns": "urn:iso:std:iso:20022:tech:xsd:pain.001.001.09",
        "root_tag": "CstmrCdtTrfInitn",
        "system_code": "ICM_VPM"
    },
    "pmn": {
        "category": "icmn",
        "msg_type": "pain001",
        "description": "Payment Management Notification - Technical Lifecycle",
        "has_technical_attrs": True,
        "xmlns": "urn:iso:std:iso:20022:tech:xsd:pain.001.001.09",
        "root_tag": "CstmrCdtTrfInitn",
        "system_code": "ICM_PMN"
    },
    "psn": {
        "category": "cpo",
        "msg_type": "pain002",
        "description": "Payment Service Notification - Business Status",
        "has_technical_attrs": False,
        "xmlns": "urn:iso:std:iso:20022:tech:xsd:pain.002.001.12",
        "root_tag": "CstmrPmtStsRpt",
        "system_code": "CPO_PSN"
    },
    "plm": {
        "category": "cpo",
        "msg_type": "pain002",
        "description": "Payment Lifecycle Management - Technical Lifecycle",
        "has_technical_attrs": True,
        "xmlns": "urn:iso:std:iso:20022:tech:xsd:pain.002.001.12",
        "root_tag": "CstmrPmtStsRpt",
        "system_code": "CPO_PLM"
    }
}

# ISO 20022 Status Codes
STATUS_CODES = {
    "ACSP": "AcceptedSettlementInProcess",
    "ACSC": "AcceptedSettlementCompleted",
    "RJCT": "Rejected",
    "PDNG": "Pending",
    "PART": "PartiallyAccepted",
    "ACTC": "AcceptedTechnicalValidation"
}

# Reason Codes
REASON_CODES = {
    "AC04": "Account number invalid",
    "AG01": "Transaction forbidden",
    "MS03": "Not sufficient funds",
    "FF01": "File format invalid",
    "AM04": "Amount exceeds limit",
    "DT01": "Invalid date",
    "BE01": "Inconsistent beneficiary"
}

# ------------------------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------------------------
def setup_logging(verbose=False):
    logger = logging.getLogger("payment_generator")
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    ))
    logger.addHandler(handler)
    return logger

logger = setup_logging()

# ------------------------------------------------------------------------------
# Data Generation Utilities
# ------------------------------------------------------------------------------
def rand_amount():
    """Generate random payment amount"""
    return round(random.uniform(10, 50000), 2)

def rand_currency():
    """Generate random currency code"""
    return random.choice(["GBP", "EUR", "USD", "CHF", "JPY"])

def rand_account():
    """Generate random account number"""
    return f"{random.randint(10000000, 99999999)}"

def rand_sort_code():
    """Generate random sort code"""
    return f"{random.randint(10,99)}-{random.randint(10,99)}-{random.randint(10,99)}"

def rand_status():
    """Generate random payment status"""
    return random.choice(list(STATUS_CODES.keys()))

def rand_reason_code():
    """Generate random reason code"""
    return random.choice(list(REASON_CODES.keys()))

def rand_event_id():
    """Generate random UUID"""
    return str(uuid.uuid4())

def rand_timestamp():
    """Generate current timestamp"""
    return datetime.now().isoformat()

def rand_timestamp_offset(days=30):
    """Generate timestamp with random offset"""
    offset = random.randint(-days, days)
    dt = datetime.now() + timedelta(days=offset)
    return dt.isoformat()

def rand_bic():
    """Generate random BIC code"""
    banks = ["BARC", "HSBC", "LLOY", "RBOS", "SANT", "NATW"]
    return f"{random.choice(banks)}GB2L"

def rand_iban(country="GB"):
    """Generate a syntactically plausible development IBAN."""
    bank = "NWBK" if country == "GB" else "BNPA"
    return f"{country}{random.randint(10, 99)}{bank}{random.randint(10**14, 10**15 - 1)}"

# ------------------------------------------------------------------------------
# Technical Attributes (x-*)
# ------------------------------------------------------------------------------
def generate_technical_attrs(system_code):
    """Generate x-* technical attributes for PMN and PLM systems"""
    return {
        "x-correlationId": str(uuid.uuid4()),
        "x-traceId": str(uuid.uuid4()),
        "x-spanId": f"span-{random.randint(1000,9999)}",
        "x-parentSpanId": f"parent-{random.randint(1000,9999)}",
        "x-sampled": random.choice(["0", "1"]),
        "x-flags": hex(random.randint(0, 255)),
        "x-tenantId": f"tenant-{random.randint(1,5)}",
        "x-environment": random.choice(["dev", "test", "staging", "prod"]),
        "x-version": f"v{random.randint(1,3)}.{random.randint(0,9)}",
        "x-messageType": system_code,
        "x-messageVersion": f"{random.randint(1,3)}.0.{random.randint(0,9)}",
        "x-processingNode": f"node-{random.randint(1,10)}",
        "x-requestId": str(uuid.uuid4())[:8],
        "x-timestamp": datetime.now().isoformat()
    }

# ------------------------------------------------------------------------------
# PAIN.001 Generators
# ------------------------------------------------------------------------------
def generate_pain001_vpm(event_id, sequence_num):
    """Generate PAIN.001 for VPM (Business Initiation)"""
    root = ET.Element("Document")
    root.set("xmlns", PAYMENT_SYSTEMS["vpm"]["xmlns"])
    
    cstmr = ET.SubElement(root, "CstmrCdtTrfInitn")
    
    # Group Header
    grp_hdr = ET.SubElement(cstmr, "GrpHdr")
    ET.SubElement(grp_hdr, "MsgId").text = f"MSG-{event_id[:8]}"
    ET.SubElement(grp_hdr, "CreDtTm").text = rand_timestamp()
    ET.SubElement(grp_hdr, "NbOfTxs").text = "1"
    initiating_party = ET.SubElement(grp_hdr, "InitgPty")
    ET.SubElement(initiating_party, "Nm").text = "VPM_Business_Initiation"
    
    # Payment Information
    pmt_inf = ET.SubElement(cstmr, "PmtInf")
    ET.SubElement(pmt_inf, "PmtInfId").text = f"PMT-{event_id[:8]}"
    ET.SubElement(pmt_inf, "PmtMtd").text = "TRF"
    ET.SubElement(pmt_inf, "BtchBookg").text = "false"
    execution_date = ET.SubElement(pmt_inf, "ReqdExctnDt")
    ET.SubElement(execution_date, "Dt").text = (datetime.now() + timedelta(days=1)).date().isoformat()
    
    # Debtor
    dbtr = ET.SubElement(pmt_inf, "Dbtr")
    ET.SubElement(dbtr, "Nm").text = f"Synthetic Debtor Ltd - {random.randint(1,100)}"
    
    dbtr_acct = ET.SubElement(pmt_inf, "DbtrAcct")
    dbtr_id = ET.SubElement(dbtr_acct, "Id")
    ET.SubElement(dbtr_id, "IBAN").text = rand_iban("GB")
    
    dbtr_agt = ET.SubElement(pmt_inf, "DbtrAgt")
    dbtr_fin = ET.SubElement(dbtr_agt, "FinInstnId")
    ET.SubElement(dbtr_fin, "BICFI").text = rand_bic()
    
    # Credit Transfer
    cdt_trf = ET.SubElement(pmt_inf, "CdtTrfTxInf")
    payment_id = ET.SubElement(cdt_trf, "PmtId")
    ET.SubElement(payment_id, "InstrId").text = f"INS-{event_id[:8]}-{sequence_num:03d}"
    ET.SubElement(payment_id, "EndToEndId").text = f"E2E-{event_id[:8]}-{sequence_num:03d}"
    
    # Amount
    amt = ET.SubElement(cdt_trf, "Amt")
    ET.SubElement(amt, "InstdAmt", Ccy=rand_currency()).text = str(rand_amount())
    
    # Creditor
    cdtr = ET.SubElement(cdt_trf, "Cdtr")
    ET.SubElement(cdtr, "Nm").text = f"Synthetic Creditor Ltd - {random.randint(1,100)}"
    
    cdtr_acct = ET.SubElement(cdt_trf, "CdtrAcct")
    cdtr_id = ET.SubElement(cdtr_acct, "Id")
    ET.SubElement(cdtr_id, "IBAN").text = rand_iban("FR")
    
    cdtr_agt = ET.SubElement(cdt_trf, "CdtrAgt")
    cdtr_fin = ET.SubElement(cdtr_agt, "FinInstnId")
    ET.SubElement(cdtr_fin, "BICFI").text = "BNPAFRPP"
    
    # Business Attributes (VPM specific)
    ET.SubElement(cdt_trf, "Purp").text = random.choice(["SALA", "SUPP", "TRAD", "INVS"])
    remittance = ET.SubElement(cdt_trf, "RmtInf")
    ET.SubElement(remittance, "Ustrd").text = f"INV-{datetime.now():%Y}-{random.randint(1000, 9999)}"
    
    # Invoice references
    for i in range(1, 6):
        inv = ET.SubElement(cdt_trf, f"InvRef{i}")
        ET.SubElement(inv, "Ref").text = f"INV-{random.randint(10000,99999)}"
    
    return ET.tostring(root, encoding="unicode")

def generate_pain001_pmn(event_id, sequence_num):
    """Generate PAIN.001 for PMN (Technical Lifecycle with x-* attributes)"""
    root = ET.Element("Document")
    root.set("xmlns", PAYMENT_SYSTEMS["pmn"]["xmlns"])
    
    cstmr = ET.SubElement(root, "CstmrCdtTrfInitn")
    
    # Generate technical attributes
    tech_attrs = generate_technical_attrs("PMN")
    
    # Group Header with technical attributes
    grp_hdr = ET.SubElement(cstmr, "GrpHdr")
    ET.SubElement(grp_hdr, "MsgId").text = f"TECH-{event_id[:8]}"
    ET.SubElement(grp_hdr, "CreDtTm").text = rand_timestamp()
    ET.SubElement(grp_hdr, "NbOfTxs").text = "1"
    initiating_party = ET.SubElement(grp_hdr, "InitgPty")
    ET.SubElement(initiating_party, "Nm").text = "PMN_Technical_Lifecycle"
    
    # Add x-* attributes to group header
    for key, value in tech_attrs.items():
        ET.SubElement(grp_hdr, key).text = value
    
    # Payment Information
    pmt_inf = ET.SubElement(cstmr, "PmtInf")
    ET.SubElement(pmt_inf, "PmtInfId").text = f"TECH-PMT-{event_id[:8]}"
    ET.SubElement(pmt_inf, "PmtMtd").text = "TRF"
    ET.SubElement(pmt_inf, "BtchBookg").text = "false"
    execution_date = ET.SubElement(pmt_inf, "ReqdExctnDt")
    ET.SubElement(execution_date, "Dt").text = datetime.now().date().isoformat()
    
    # Debtor
    dbtr = ET.SubElement(pmt_inf, "Dbtr")
    ET.SubElement(dbtr, "Nm").text = "Technical Debtor Service"
    
    dbtr_acct = ET.SubElement(pmt_inf, "DbtrAcct")
    dbtr_id = ET.SubElement(dbtr_acct, "Id")
    ET.SubElement(dbtr_id, "IBAN").text = rand_iban("GB")
    
    dbtr_agt = ET.SubElement(pmt_inf, "DbtrAgt")
    dbtr_fin = ET.SubElement(dbtr_agt, "FinInstnId")
    ET.SubElement(dbtr_fin, "BICFI").text = rand_bic()
    
    # Credit Transfer
    cdt_trf = ET.SubElement(pmt_inf, "CdtTrfTxInf")
    payment_id = ET.SubElement(cdt_trf, "PmtId")
    ET.SubElement(payment_id, "InstrId").text = f"INS-{event_id[:8]}-{sequence_num:03d}"
    ET.SubElement(payment_id, "EndToEndId").text = f"E2E-{event_id[:8]}-{sequence_num:03d}"
    
    # Amount
    amt = ET.SubElement(cdt_trf, "Amt")
    ET.SubElement(amt, "InstdAmt", Ccy=rand_currency()).text = str(rand_amount())
    
    # Creditor
    cdtr = ET.SubElement(cdt_trf, "Cdtr")
    ET.SubElement(cdtr, "Nm").text = "Technical Creditor Service"
    
    cdtr_acct = ET.SubElement(cdt_trf, "CdtrAcct")
    cdtr_id = ET.SubElement(cdtr_acct, "Id")
    ET.SubElement(cdtr_id, "IBAN").text = rand_iban("FR")
    
    cdtr_agt = ET.SubElement(cdt_trf, "CdtrAgt")
    cdtr_fin = ET.SubElement(cdtr_agt, "FinInstnId")
    ET.SubElement(cdtr_fin, "BICFI").text = "BNPAFRPP"
    
    # Technical attributes on payment info
    ET.SubElement(cdt_trf, "x-processingPriority").text = str(random.randint(1, 10))
    ET.SubElement(cdt_trf, "x-retryCount").text = str(random.randint(0, 3))
    ET.SubElement(cdt_trf, "x-timeout").text = f"{random.randint(30, 300)}s"
    remittance = ET.SubElement(cdt_trf, "RmtInf")
    ET.SubElement(remittance, "Ustrd").text = f"INV-{datetime.now():%Y}-{random.randint(1000, 9999)}"
    
    return ET.tostring(root, encoding="unicode")

# ------------------------------------------------------------------------------
# PAIN.002 Generators
# ------------------------------------------------------------------------------
def add_pain002_transaction_status(payment_info, event_id, sequence_num):
    """Add the PAIN.002 transaction-status and original-transaction reference."""
    tx = ET.SubElement(payment_info, "TxInfAndSts")
    ET.SubElement(tx, "OrgnlInstrId").text = f"INS-{event_id[:8]}-{sequence_num:03d}"
    ET.SubElement(tx, "OrgnlEndToEndId").text = f"E2E-{event_id[:8]}-{sequence_num:03d}"
    ET.SubElement(tx, "OrgnlTxId").text = f"TX-{event_id[:8]}-{sequence_num:03d}"
    ET.SubElement(tx, "OrgnlUETR").text = str(uuid.uuid4())
    ET.SubElement(tx, "TxSts").text = rand_status()
    reason = ET.SubElement(tx, "StsRsnInf")
    reason_code = ET.SubElement(reason, "Rsn")
    ET.SubElement(reason_code, "Cd").text = rand_reason_code()
    ET.SubElement(reason, "AddtlInf").text = "Synthetic transaction status detail"
    ET.SubElement(tx, "AccptncDtTm").text = rand_timestamp()
    ET.SubElement(tx, "AcctSvcrRef").text = f"ASR-{event_id[:10]}"
    ET.SubElement(tx, "ClrSysRef").text = f"CLR-{random.randint(100000, 999999)}"

    original_ref = ET.SubElement(tx, "OrgnlTxRef")
    amount = ET.SubElement(original_ref, "Amt")
    ET.SubElement(amount, "InstdAmt", Ccy=rand_currency()).text = str(rand_amount())
    execution_date = ET.SubElement(original_ref, "ReqdExctnDt")
    ET.SubElement(execution_date, "Dt").text = (datetime.now() + timedelta(days=1)).date().isoformat()
    debtor = ET.SubElement(original_ref, "Dbtr")
    debtor_party = ET.SubElement(debtor, "Pty")
    ET.SubElement(debtor_party, "Nm").text = "ABC Ltd"
    debtor_acct = ET.SubElement(original_ref, "DbtrAcct")
    debtor_id = ET.SubElement(debtor_acct, "Id")
    ET.SubElement(debtor_id, "IBAN").text = rand_iban("GB")
    debtor_agent = ET.SubElement(original_ref, "DbtrAgt")
    debtor_fin = ET.SubElement(debtor_agent, "FinInstnId")
    ET.SubElement(debtor_fin, "BICFI").text = "NWBKGB2L"
    creditor = ET.SubElement(original_ref, "Cdtr")
    creditor_party = ET.SubElement(creditor, "Pty")
    ET.SubElement(creditor_party, "Nm").text = "XYZ Ltd"
    creditor_acct = ET.SubElement(original_ref, "CdtrAcct")
    creditor_id = ET.SubElement(creditor_acct, "Id")
    ET.SubElement(creditor_id, "IBAN").text = rand_iban("FR")
    creditor_agent = ET.SubElement(original_ref, "CdtrAgt")
    creditor_fin = ET.SubElement(creditor_agent, "FinInstnId")
    ET.SubElement(creditor_fin, "BICFI").text = "BNPAFRPP"
    remittance = ET.SubElement(original_ref, "RmtInf")
    ET.SubElement(remittance, "Ustrd").text = f"INV-{datetime.now():%Y}-{random.randint(1000, 9999)}"

def generate_pain002_psn(event_id, sequence_num):
    """Generate PAIN.002 for PSN (Business Status Notification)"""
    root = ET.Element("Document")
    root.set("xmlns", PAYMENT_SYSTEMS["psn"]["xmlns"])
    
    sts = ET.SubElement(root, "CstmrPmtStsRpt")
    
    # Group Header
    grp_hdr = ET.SubElement(sts, "GrpHdr")
    ET.SubElement(grp_hdr, "MsgId").text = f"STS-{event_id[:8]}"
    ET.SubElement(grp_hdr, "CreDtTm").text = rand_timestamp()
    initiating_party = ET.SubElement(grp_hdr, "InitgPty")
    ET.SubElement(initiating_party, "Nm").text = "PSN_Business_Status"
    
    # Original Group Info
    orgnl_grp = ET.SubElement(sts, "OrgnlGrpInfAndSts")
    ET.SubElement(orgnl_grp, "OrgnlMsgId").text = f"MSG-{random.randint(100000,999999)}"
    ET.SubElement(orgnl_grp, "OrgnlMsgNmId").text = "pain.001.001.09"
    ET.SubElement(orgnl_grp, "OrgnlCreDtTm").text = rand_timestamp_offset(1)
    ET.SubElement(orgnl_grp, "OrgnlNbOfTxs").text = "1"
    ET.SubElement(orgnl_grp, "GrpSts").text = rand_status()
    group_reason = ET.SubElement(orgnl_grp, "StsRsnInf")
    group_reason_code = ET.SubElement(group_reason, "Rsn")
    ET.SubElement(group_reason_code, "Cd").text = rand_reason_code()
    ET.SubElement(group_reason, "AddtlInf").text = "Synthetic group status detail"
    
    # Payment Info and Status
    tx_inf = ET.SubElement(sts, "OrgnlPmtInfAndSts")
    ET.SubElement(tx_inf, "OrgnlPmtInfId").text = f"PMT-{event_id[:8]}"
    ET.SubElement(tx_inf, "PmtInfSts").text = rand_status()
    ET.SubElement(tx_inf, "TxSts").text = rand_status()
    
    # Status Reason with details
    sts_rsn = ET.SubElement(tx_inf, "StsRsnInf")
    ET.SubElement(sts_rsn, "Rsn").text = rand_reason_code()
    
    # Additional Info
    ET.SubElement(sts_rsn, "AddtlInf").text = REASON_CODES.get(rand_reason_code(), "No additional info")
    
    # Business attributes (PSN specific)
    ET.SubElement(tx_inf, "SettlementStatus").text = random.choice(["SETTLED", "PENDING", "FAILED"])
    ET.SubElement(tx_inf, "BusinessDate").text = datetime.now().strftime("%Y-%m-%d")
    add_pain002_transaction_status(tx_inf, event_id, sequence_num)
    
    return ET.tostring(root, encoding="unicode")

def generate_pain002_plm(event_id, sequence_num):
    """Generate PAIN.002 for PLM (Technical Lifecycle with x-* attributes)"""
    root = ET.Element("Document")
    root.set("xmlns", PAYMENT_SYSTEMS["plm"]["xmlns"])
    
    sts = ET.SubElement(root, "CstmrPmtStsRpt")
    
    # Generate technical attributes
    tech_attrs = generate_technical_attrs("PLM")
    
    # Group Header with technical attributes
    grp_hdr = ET.SubElement(sts, "GrpHdr")
    ET.SubElement(grp_hdr, "MsgId").text = f"TECH-STS-{event_id[:8]}"
    ET.SubElement(grp_hdr, "CreDtTm").text = rand_timestamp()
    initiating_party = ET.SubElement(grp_hdr, "InitgPty")
    ET.SubElement(initiating_party, "Nm").text = "PLM_Technical_Lifecycle"
    
    # Add x-* attributes to group header
    for key, value in tech_attrs.items():
        ET.SubElement(grp_hdr, key).text = value
    
    # Original Group Info
    orgnl_grp = ET.SubElement(sts, "OrgnlGrpInfAndSts")
    ET.SubElement(orgnl_grp, "OrgnlMsgId").text = f"TECH-{random.randint(100000,999999)}"
    ET.SubElement(orgnl_grp, "OrgnlMsgNmId").text = "pain.001.001.09"
    ET.SubElement(orgnl_grp, "OrgnlCreDtTm").text = rand_timestamp_offset(1)
    ET.SubElement(orgnl_grp, "OrgnlNbOfTxs").text = "1"
    ET.SubElement(orgnl_grp, "GrpSts").text = rand_status()
    group_reason = ET.SubElement(orgnl_grp, "StsRsnInf")
    group_reason_code = ET.SubElement(group_reason, "Rsn")
    ET.SubElement(group_reason_code, "Cd").text = random.choice(["TECH001", "TECH002", "TECH003"])
    ET.SubElement(group_reason, "AddtlInf").text = "Synthetic technical group status detail"
    
    # Technical Status
    tx_inf = ET.SubElement(sts, "OrgnlPmtInfAndSts")
    ET.SubElement(tx_inf, "OrgnlPmtInfId").text = f"TECH-PMT-{event_id[:8]}"
    ET.SubElement(tx_inf, "PmtInfSts").text = rand_status()
    ET.SubElement(tx_inf, "TxSts").text = rand_status()
    
    # Technical error details
    sts_rsn = ET.SubElement(tx_inf, "StsRsnInf")
    ET.SubElement(sts_rsn, "Rsn").text = random.choice(["TECH001", "TECH002", "TECH003"])
    ET.SubElement(sts_rsn, "AddtlInf").text = random.choice([
        "Technical validation failed",
        "System timeout",
        "Processing error",
        "Queue overflow"
    ])
    
    # Technical attributes (PLM specific)
    ET.SubElement(tx_inf, "x-processingStatus").text = random.choice(["PROCESSING", "COMPLETED", "FAILED"])
    ET.SubElement(tx_inf, "x-errorCode").text = f"ERR-{random.randint(100,999)}"
    ET.SubElement(tx_inf, "x-retryAttempt").text = str(random.randint(0, 5))
    ET.SubElement(tx_inf, "x-systemLatency").text = f"{random.randint(50, 5000)}ms"
    ET.SubElement(tx_inf, "x-priority").text = str(random.randint(1, 5))
    add_pain002_transaction_status(tx_inf, event_id, sequence_num)
    
    return ET.tostring(root, encoding="unicode")

# ------------------------------------------------------------------------------
# Generator Dispatcher
# ------------------------------------------------------------------------------
GENERATORS = {
    "vpm": generate_pain001_vpm,
    "pmn": generate_pain001_pmn,
    "psn": generate_pain002_psn,
    "plm": generate_pain002_plm,
}

# ------------------------------------------------------------------------------
# File Writer
# ------------------------------------------------------------------------------
def write_xml(path, xml_content):
    """Write XML content to file with proper encoding"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(xml_content)

def get_output_path(system, sequence_num):
    """Get the correct output path for a system"""
    config = PAYMENT_SYSTEMS[system]
    category = config["category"]
    msg_type = config["msg_type"]
    
    # Create filename
    filename = f"{system}_{sequence_num:04d}.xml"
    
    # Build path: data/{category}/{system}/{msg_type}/{filename}
    path = BASE_DIR / category / system / msg_type / filename
    return path

# ------------------------------------------------------------------------------
# Main Generator
# ------------------------------------------------------------------------------
def generate_events(count=20, systems=None):
    """Generate synthetic payment events for specified systems"""
    if systems is None:
        systems = list(PAYMENT_SYSTEMS.keys())
    
    total_generated = 0
    
    logger.info(f"Starting payment event generation...")
    logger.info(f"Systems: {', '.join(systems)}")
    logger.info(f"Count per system: {count}")
    logger.info(f"Base directory: {BASE_DIR}")
    
    for system in systems:
        if system not in PAYMENT_SYSTEMS:
            logger.warning(f"Unknown system: {system}, skipping")
            continue
        
        config = PAYMENT_SYSTEMS[system]
        generator = GENERATORS.get(system)
        
        if not generator:
            logger.warning(f"No generator for system: {system}, skipping")
            continue
        
        logger.info(f"Generating {count} events for {system} ({config['description']})")
        
        for i in range(1, count + 1):
            event_id = rand_event_id()
            
            # Generate XML
            xml_content = generator(event_id, i)
            
            # Get output path
            output_path = get_output_path(system, i)
            
            # Write file
            write_xml(output_path, xml_content)
            total_generated += 1
            
            if i % 5 == 0:
                logger.debug(f"  Generated {i}/{count} for {system}")
        
        logger.info(f"  ✅ Completed {count} events for {system} at {output_path.parent}")
    
    logger.info(f"✅ Completed generating {total_generated} synthetic payment events")
    
    # Print summary
    print_summary()

# ------------------------------------------------------------------------------
# Summary
# ------------------------------------------------------------------------------
def print_summary():
    """Print summary of generated files"""
    print("\n" + "=" * 70)
    print("📊 GENERATION SUMMARY")
    print("=" * 70)
    
    for system, config in PAYMENT_SYSTEMS.items():
        category = config["category"]
        msg_type = config["msg_type"]
        path = BASE_DIR / category / system / msg_type
        
        if path.exists():
            files = list(path.glob("*.xml"))
            print(f"\n{system.upper()} ({config['description']})")
            print(f"  Path: {path}")
            print(f"  Files: {len(files)}")
            
            # Show sample
            if files:
                sample = files[0]
                size = sample.stat().st_size
                print(f"  Sample: {sample.name} ({size:,} bytes)")
                
                # Show if technical attributes are present
                if config["has_technical_attrs"]:
                    print(f"  🔧 Technical attributes: Yes (x-* attributes)")
                else:
                    print(f"  📊 Business attributes: Yes")
    
    print("\n" + "=" * 70)

# ------------------------------------------------------------------------------
# CLI Entry Point
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic ISO20022 payment events"
    )
    parser.add_argument(
        "--count", "-c",
        type=int,
        default=20,
        help="Number of events to generate per system (default: 20)"
    )
    parser.add_argument(
        "--systems", "-s",
        nargs="+",
        choices=list(PAYMENT_SYSTEMS.keys()),
        help="Systems to generate (default: all)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output"
    )
    
    args = parser.parse_args()
    
    # Configure logging
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    
    # Generate events
    generate_events(args.count, args.systems)

if __name__ == "__main__":
    main()
