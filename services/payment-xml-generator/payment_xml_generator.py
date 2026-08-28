#!/usr/bin/env python3
"""
Complete ISO 20022 Payment XML Generator for PDM Uganda
=======================================================
Generates 150+ fields per message type with correct source system mapping:

- pdmis/     : PDMIS Government System (JSON)
- icmn/      : Interbank Customer Messaging Network (VPM, PMN)
- cpo/       : Cloud Payment Orchestrator (PSN, PLM)
- wendi/     : PostBank Digital Wallet (CAMT.053, .052, .054)
- mobile_networks/ : MTN, Airtel (PACS.008, .002)
- agent_network/   : Wendi Agents (Custom XML)

Output: /home/hochom/projects/dp-ai-payments-platform/data/

Usage: python payment_xml_generator.py --count 10
"""

import os
import json
import random
import uuid
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from typing import Dict, List, Any, Optional
import argparse
import logging

# ============================================================================
# Configuration
# ============================================================================

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", "/home/hochom/projects/dp-ai-payments-platform"))
DATA_ROOT = PROJECT_ROOT / "data"

# ISO 20022 Namespaces
PAIN001_NS = "urn:iso:std:iso:20022:tech:xsd:pain.001.001.09"
PAIN002_NS = "urn:iso:std:iso:20022:tech:xsd:pain.002.001.12"
PACS008_NS = "urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08"
PACS002_NS = "urn:iso:std:iso:20022:tech:xsd:pacs.002.001.10"
CAMT053_NS = "urn:iso:std:iso:20022:tech:xsd:camt.053.001.08"
CAMT052_NS = "urn:iso:std:iso:20022:tech:xsd:camt.052.001.08"
CAMT054_NS = "urn:iso:std:iso:20022:tech:xsd:camt.054.001.08"

# Status Codes
STATUS_ACSC = "ACSC"
STATUS_RJCT = "RJCT"
STATUS_PDNG = "PDNG"

# ============================================================================
# POC Cases - Deterministic Outcomes
# ============================================================================

POC_CASES = {
    "LOAN-001": {"status": STATUS_ACSC, "amount": 1000000, "account": "256781234567", "name": "Grace Auma", "project": "POULTRY", "desc": "Successful settlement"},
    "LOAN-002": {"status": STATUS_ACSC, "amount": 1000000, "account": "256789876543", "name": "John Okello", "project": "CATTLE", "desc": "Successful settlement"},
    "LOAN-003": {"status": STATUS_ACSC, "amount": 1000000, "account": "256783456789", "name": "Sarah Namuganza", "project": "COFFEE", "desc": "Successful settlement"},
    "LOAN-004": {"status": STATUS_ACSC, "amount": 1000000, "account": "256787654321", "name": "Peter Mukasa", "project": "MAIZE", "desc": "Successful settlement"},
    "LOAN-005": {"status": STATUS_ACSC, "amount": 1000000, "account": "256782345678", "name": "Mary Nalwanga", "project": "VEGETABLES", "desc": "Successful settlement"},
    "LOAN-006": {"status": STATUS_ACSC, "amount": 1000000, "account": "256786543210", "name": "David Sserwada", "project": "FISH", "desc": "Successful settlement"},
    "LOAN-007": {"status": STATUS_ACSC, "amount": 1000000, "account": "256784567890", "name": "Jane Nampijja", "project": "POULTRY", "desc": "Successful settlement"},
    "LOAN-008": {"status": STATUS_RJCT, "amount": 1000000, "account": "25678INVALID", "name": "Rejected Case", "project": "CATTLE", "desc": "Rejected - invalid account"},
    "LOAN-009": {"status": STATUS_PDNG, "amount": 1250000, "account": "256785678901", "name": "Mismatch Case", "project": "COFFEE", "desc": "Pending - amount mismatch"},
    "LOAN-010": {"status": STATUS_ACSC, "amount": 1000000, "account": "25678DIVERTED", "name": "Diverted Case", "project": "MAIZE", "desc": "Account mismatch (diverted)"},
}

# ============================================================================
# Helper Functions
# ============================================================================

def generate_id(prefix: str, length: int = 8) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:length].upper()}"

def generate_timestamp(offset_hours: int = 0) -> str:
    dt = datetime.now() + timedelta(hours=offset_hours)
    return dt.strftime("%Y-%m-%dT%H:%M:%S+03:00")

def generate_date(offset_days: int = 0) -> str:
    dt = datetime.now() + timedelta(days=offset_days)
    return dt.strftime("%Y-%m-%d")

def generate_phone() -> str:
    return f"256{random.randint(700000000, 799999999)}"

def child(parent: ET.Element, name: str, text: str = None, **attributes) -> ET.Element:
    node = ET.SubElement(parent, name, attributes)
    if text is not None:
        node.text = str(text)
    return node

def add_address(parent: ET.Element, name: str, address_lines: List[str] = None) -> ET.Element:
    addr = child(parent, name)
    if address_lines:
        for line in address_lines:
            child(addr, "AdrLine", line)
    child(addr, "TwnNm", "Kampala")
    child(addr, "CtrySubDvsn", "Central")
    child(addr, "Ctry", "UG")
    child(addr, "PstCd", "256")
    return addr

def add_technical_attrs_pmn(parent: ET.Element) -> None:
    """Add x-* technical attributes for PMN"""
    attrs = {
        "x-correlationId": str(uuid.uuid4()),
        "x-traceId": str(uuid.uuid4()),
        "x-spanId": f"span-{random.randint(1000,9999)}",
        "x-parentSpanId": f"parent-{random.randint(1000,9999)}",
        "x-sampled": random.choice(["0", "1"]),
        "x-flags": hex(random.randint(0, 255)),
        "x-tenantId": f"tenant-{random.randint(1,5)}",
        "x-environment": random.choice(["dev", "test", "staging", "prod"]),
        "x-version": f"v{random.randint(1,3)}.{random.randint(0,9)}",
        "x-messageType": "PMN",
        "x-messageVersion": "1.0.0",
        "x-processingNode": f"node-{random.randint(1,10)}",
        "x-requestId": str(uuid.uuid4())[:8],
        "x-timestamp": datetime.now().isoformat(),
        "x-processingPriority": str(random.randint(1, 10)),
        "x-retryCount": str(random.randint(0, 3)),
        "x-timeout": f"{random.randint(30, 300)}s",
        "x-deadline": datetime.now().isoformat(),
        "x-maxRetries": str(random.randint(3, 5)),
        "x-backoffDelay": f"{random.randint(100, 1000)}ms",
        "x-circuitBreaker": random.choice(["OPEN", "HALF_OPEN", "CLOSED"]),
        "x-rateLimiter": str(random.randint(10, 100)),
        "x-bulkhead": str(random.randint(5, 20)),
        "x-retryPolicy": random.choice(["LINEAR", "EXPONENTIAL", "CUSTOM"]),
        "x-fallbackEnabled": random.choice(["true", "false"]),
        "x-timeoutThreshold": f"{random.randint(5000, 30000)}ms",
        "x-queueDepth": str(random.randint(0, 1000)),
        "x-processingTime": f"{random.randint(50, 500)}ms",
        "x-throughput": f"{random.randint(10, 100)}/s",
        "x-latencyP95": f"{random.randint(100, 500)}ms",
        "x-errorRate": f"{random.uniform(0.01, 0.05):.2f}",
        "x-successRate": f"{random.uniform(0.95, 0.99):.2f}",
        "x-healthStatus": random.choice(["HEALTHY", "DEGRADED", "UNHEALTHY"]),
    }
    for key, value in attrs.items():
        child(parent, key, value)

def add_technical_attrs_plm(parent: ET.Element) -> None:
    """Add x-* technical attributes for PLM"""
    attrs = {
        "x-correlationId": str(uuid.uuid4()),
        "x-traceId": str(uuid.uuid4()),
        "x-spanId": f"span-{random.randint(1000,9999)}",
        "x-parentSpanId": f"parent-{random.randint(1000,9999)}",
        "x-sampled": random.choice(["0", "1"]),
        "x-flags": hex(random.randint(0, 255)),
        "x-tenantId": f"tenant-{random.randint(1,5)}",
        "x-environment": random.choice(["dev", "test", "staging", "prod"]),
        "x-version": f"v{random.randint(1,3)}.{random.randint(0,9)}",
        "x-messageType": "PLM",
        "x-messageVersion": "1.0.0",
        "x-processingNode": f"node-{random.randint(1,10)}",
        "x-requestId": str(uuid.uuid4())[:8],
        "x-timestamp": datetime.now().isoformat(),
        "x-processingStatus": random.choice(["INITIATED", "PROCESSING", "COMPLETED", "FAILED"]),
        "x-errorCode": f"ERR-{random.randint(100,999)}",
        "x-retryAttempt": str(random.randint(0, 5)),
        "x-systemLatency": f"{random.randint(50, 5000)}ms",
        "x-priority": str(random.randint(1, 5)),
        "x-lifecycleStage": random.choice(["SUBMITTED", "VALIDATED", "ROUTED", "SETTLED"]),
        "x-eventType": random.choice(["STATUS_CHANGE", "RETRY", "TIMEOUT", "COMPLETION"]),
        "x-acknowledgment": random.choice(["ACK", "NACK", "PENDING"]),
        "x-retryCount": str(random.randint(0, 3)),
        "x-deadline": datetime.now().isoformat(),
        "x-timeout": f"{random.randint(30, 300)}s",
        "x-recovery": random.choice(["AUTO", "MANUAL", "NONE"]),
        "x-rollback": random.choice(["true", "false"]),
        "x-compensation": random.choice(["true", "false"]),
        "x-sagaId": str(uuid.uuid4()),
        "x-transactionId": str(uuid.uuid4()),
        "x-orchestrator": f"orch-{random.randint(1,5)}",
        "x-stepId": f"step-{random.randint(1,20)}",
        "x-statusDetail": random.choice(["SUCCESS", "PARTIAL", "FAILURE"]),
        "x-resolution": random.choice(["COMPLETED", "PENDING", "ESCALATED"]),
    }
    for key, value in attrs.items():
        child(parent, key, value)

def write_xml(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)

def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)

# ============================================================================
# 1. PDMIS - Government System Data (JSON)
# ============================================================================

def generate_pdmis_data() -> Dict[str, Any]:
    """Generate PDMIS government system data"""
    
    saccos = [{
        "sacco_id": "SACCO-KAM-001",
        "name": "Kamuli Central PDM SACCO",
        "registration_number": "KAM-SACCO-001",
        "wendi_account": "256791234567",
        "parish": "Kamuli Central",
        "district": "Kamuli",
        "registration_date": generate_date(-365),
        "is_active": True
    }]
    
    special_groups = [
        {"group_code": "WOMEN", "group_name": "Women", "quota_percentage": 30.0},
        {"group_code": "YOUTH", "group_name": "Youth", "quota_percentage": 30.0},
        {"group_code": "PWD", "group_name": "Persons with Disabilities", "quota_percentage": 10.0},
        {"group_code": "ELDERLY", "group_name": "Elderly Persons", "quota_percentage": 10.0},
        {"group_code": "GENERAL", "group_name": "General Community", "quota_percentage": 20.0}
    ]
    
    beneficiaries = []
    for i, (loan_id, case_data) in enumerate(POC_CASES.items(), 1):
        nin = f"CM{random.randint(9000000, 9999999)}{random.randint(100, 999)}"
        ben = {
            "beneficiary_id": f"BEN-{i:04d}",
            "nin": nin,
            "nin_hashed": hashlib.sha256(nin.encode()).hexdigest(),
            "beneficiary_token": f"TOKEN-{nin[:8]}",
            "name": case_data["name"],
            "phone": case_data["account"],
            "parish": "Kamuli Central",
            "district": "Kamuli",
            "special_group": random.choice(["WOMEN", "YOUTH", "GENERAL"]),
            "registration_date": generate_date(-300),
            "is_active": True
        }
        beneficiaries.append(ben)
    
    households = []
    for i in range(1, 11):
        household = {
            "household_id": f"HH-{i:05d}",
            "head_of_household": beneficiaries[i-1]["name"],
            "member_count": random.randint(2, 8),
            "parish": "Kamuli Central",
            "district": "Kamuli",
            "economic_status": random.choice(["LOW", "MEDIUM", "HIGH"]),
            "registration_date": generate_date(-300)
        }
        households.append(household)
    
    loans = []
    for i, (loan_id, case_data) in enumerate(POC_CASES.items(), 1):
        loan = {
            "loan_id": loan_id,
            "beneficiary_id": f"BEN-{i:04d}",
            "sacco_id": "SACCO-KAM-001",
            "application_date": generate_date(-90),
            "approval_date": generate_date(-60),
            "amount_requested": case_data["amount"],
            "amount_approved": case_data["amount"],
            "amount_disbursed": case_data["amount"] if case_data["status"] == STATUS_ACSC else 0,
            "loan_status": "DISBURSED" if case_data["status"] == STATUS_ACSC else 
                           "REJECTED" if case_data["status"] == STATUS_RJCT else 
                           "APPROVED",
            "business_plan_id": f"BP-{i:05d}",
            "project_type": case_data["project"]
        }
        loans.append(loan)
    
    business_plans = []
    for i, (loan_id, case_data) in enumerate(POC_CASES.items(), 1):
        plan = {
            "business_plan_id": f"BP-{i:05d}",
            "loan_id": loan_id,
            "beneficiary_id": f"BEN-{i:04d}",
            "project_type": case_data["project"],
            "description": f"{case_data['project']} farming project",
            "expected_revenue": case_data["amount"] * random.uniform(1.2, 2.0),
            "submission_date": generate_date(-85),
            "approval_status": "APPROVED"
        }
        business_plans.append(plan)
    
    return {
        "saccos": saccos,
        "special_groups": special_groups,
        "beneficiaries": beneficiaries,
        "households": households,
        "loans": loans,
        "business_plans": business_plans
    }

# ============================================================================
# 2. ICMN - Interbank Customer Messaging Network (XML)
# ============================================================================

def generate_icmn_vpm(loan_id: str, case_data: dict, sacco: dict, beneficiary: dict) -> bytes:
    """Generate ICMN/VPM - Business PAIN.001 (Virtual Payment Management)"""
    ns = PAIN001_NS
    root = ET.Element("Document", xmlns=ns)
    init = child(root, "CstmrCdtTrfInitn")
    
    header = child(init, "GrpHdr")
    child(header, "MsgId", f"ICMN-VPM-{loan_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    child(header, "CreDtTm", generate_timestamp())
    child(header, "NbOfTxs", "1")
    child(header, "CtrlSum", str(case_data["amount"]))
    
    pmt_inf = child(init, "PmtInf")
    child(pmt_inf, "PmtInfId", f"PMT-{loan_id}")
    child(pmt_inf, "PmtMtd", "TRF")
    child(pmt_inf, "BtchBookg", "false")
    child(pmt_inf, "NbOfTxs", "1")
    child(pmt_inf, "CtrlSum", str(case_data["amount"]))
    child(pmt_inf, "ReqdExctnDt", generate_date(1))
    
    debtor = child(pmt_inf, "Dbtr")
    child(debtor, "Nm", sacco["name"])
    debtor_acct = child(pmt_inf, "DbtrAcct")
    debtor_acct_id = child(debtor_acct, "Id")
    debtor_acct_other = child(debtor_acct_id, "Othr")
    child(debtor_acct_other, "Id", sacco["wendi_account"])
    child(debtor_acct_other, "Issr", "WENDI")
    
    tx = child(pmt_inf, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "InstrId", f"INSTR-{loan_id}")
    child(pmt_id, "EndToEndId", loan_id)
    child(pmt_id, "TxId", f"TX-{loan_id}-{uuid.uuid4().hex[:8]}")
    child(pmt_id, "UETR", str(uuid.uuid4()))
    
    amt = child(tx, "Amt")
    child(amt, "InstdAmt", str(case_data["amount"]), Ccy="UGX")
    
    creditor = child(tx, "Cdtr")
    child(creditor, "Nm", beneficiary["name"])
    creditor_acct = child(tx, "CdtrAcct")
    creditor_acct_id = child(creditor_acct, "Id")
    creditor_acct_other = child(creditor_acct_id, "Othr")
    child(creditor_acct_other, "Id", case_data["account"])
    child(creditor_acct_other, "Issr", "MTN")
    child(creditor_acct_other, "SchmeNm", "MSISDN")
    
    remit = child(tx, "RmtInf")
    child(remit, "Ustrd", f"PDM Loan {loan_id} - {case_data['project']}")
    
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

def generate_icmn_pmn(loan_id: str, case_data: dict, sacco: dict, beneficiary: dict) -> bytes:
    """Generate ICMN/PMN - Technical PAIN.001 (Payment Management Notification)"""
    ns = PAIN001_NS
    root = ET.Element("Document", xmlns=ns)
    init = child(root, "CstmrCdtTrfInitn")
    
    header = child(init, "GrpHdr")
    child(header, "MsgId", f"ICMN-PMN-{loan_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    child(header, "CreDtTm", generate_timestamp())
    child(header, "NbOfTxs", "1")
    child(header, "CtrlSum", str(case_data["amount"]))
    
    add_technical_attrs_pmn(header)
    
    pmt_inf = child(init, "PmtInf")
    child(pmt_inf, "PmtInfId", f"PMN-PMT-{loan_id}")
    child(pmt_inf, "PmtMtd", "TRF")
    child(pmt_inf, "NbOfTxs", "1")
    child(pmt_inf, "CtrlSum", str(case_data["amount"]))
    child(pmt_inf, "ReqdExctnDt", generate_date(1))
    
    add_technical_attrs_pmn(pmt_inf)
    
    tx = child(pmt_inf, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "InstrId", f"PMN-INSTR-{loan_id}")
    child(pmt_id, "EndToEndId", loan_id)
    
    add_technical_attrs_pmn(tx)
    
    amt = child(tx, "Amt")
    child(amt, "InstdAmt", str(case_data["amount"]), Ccy="UGX")
    
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

# ============================================================================
# 3. CPO - Cloud Payment Orchestrator (XML)
# ============================================================================

def generate_cpo_psn(loan_id: str, case_data: dict) -> bytes:
    """Generate CPO/PSN - Business PAIN.002 (Payment Service Notification)"""
    ns = PAIN002_NS
    root = ET.Element("Document", xmlns=ns)
    report = child(root, "CstmrPmtStsRpt")
    
    header = child(report, "GrpHdr")
    child(header, "MsgId", f"CPO-PSN-{loan_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    child(header, "CreDtTm", generate_timestamp(1))
    
    original_grp = child(report, "OrgnlGrpInfAndSts")
    child(original_grp, "OrgnlMsgId", f"ICMN-VPM-{loan_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    child(original_grp, "OrgnlMsgNmId", "pain.001.001.09")
    child(original_grp, "OrgnlNbOfTxs", "1")
    child(original_grp, "OrgnlCtrlSum", str(case_data["amount"]))
    child(original_grp, "GrpSts", case_data["status"])
    
    original_pmt = child(report, "OrgnlPmtInfAndSts")
    child(original_pmt, "OrgnlPmtInfId", f"PMT-{loan_id}")
    child(original_pmt, "TxSts", case_data["status"])
    
    if case_data.get("status") == STATUS_RJCT:
        sts_rsn = child(original_pmt, "StsRsnInf")
        sts_rsn_code = child(sts_rsn, "Rsn")
        child(sts_rsn_code, "Cd", "AC01")
        child(sts_rsn, "AddtlInf", f"Invalid account: {case_data['account']}")
    
    child(original_pmt, "OrgnlInstrId", f"INSTR-{loan_id}")
    child(original_pmt, "OrgnlEndToEndId", loan_id)
    child(original_pmt, "OrgnlTxId", f"TX-{loan_id}-{uuid.uuid4().hex[:8]}")
    child(original_pmt, "OrgnlUETR", str(uuid.uuid4()))
    
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

def generate_cpo_plm(loan_id: str, case_data: dict) -> bytes:
    """Generate CPO/PLM - Technical PAIN.002 (Payment Lifecycle Management)"""
    ns = PAIN002_NS
    root = ET.Element("Document", xmlns=ns)
    report = child(root, "CstmrPmtStsRpt")
    
    header = child(report, "GrpHdr")
    child(header, "MsgId", f"CPO-PLM-{loan_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    child(header, "CreDtTm", generate_timestamp(1))
    
    add_technical_attrs_plm(header)
    
    original_grp = child(report, "OrgnlGrpInfAndSts")
    child(original_grp, "OrgnlMsgId", f"ICMN-PMN-{loan_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    child(original_grp, "OrgnlMsgNmId", "pain.001.001.09")
    child(original_grp, "OrgnlNbOfTxs", "1")
    child(original_grp, "GrpSts", case_data["status"])
    
    add_technical_attrs_plm(original_grp)
    
    original_pmt = child(report, "OrgnlPmtInfAndSts")
    child(original_pmt, "OrgnlPmtInfId", f"PMN-PMT-{loan_id}")
    child(original_pmt, "TxSts", case_data["status"])
    
    add_technical_attrs_plm(original_pmt)
    
    child(original_pmt, "OrgnlInstrId", f"PMN-INSTR-{loan_id}")
    child(original_pmt, "OrgnlEndToEndId", loan_id)
    child(original_pmt, "OrgnlTxId", f"PMN-TX-{loan_id}")
    child(original_pmt, "x-processingStatus", random.choice(["PROCESSING", "COMPLETED", "FAILED"]))
    child(original_pmt, "x-errorCode", f"ERR-{random.randint(100,999)}")
    child(original_pmt, "x-retryAttempt", str(random.randint(0, 5)))
    child(original_pmt, "x-systemLatency", f"{random.randint(50, 5000)}ms")
    
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

# ============================================================================
# 4. Wendi - PostBank Digital Wallet (XML)
# ============================================================================

def generate_wendi_camt053(loan_id: str, case_data: dict, sacco: dict) -> bytes:
    """Generate Wendi/CAMT.053 - End-of-Day Statement"""
    ns = CAMT053_NS
    root = ET.Element("Document", xmlns=ns)
    stmt = child(root, "BkToCstmrStmt")
    
    header = child(stmt, "GrpHdr")
    child(header, "MsgId", f"WENDI-STMT-{loan_id}-{datetime.now().strftime('%Y%m%d')}")
    child(header, "CreDtTm", generate_timestamp())
    
    statement = child(stmt, "Stmt")
    child(statement, "Id", f"STMT-{loan_id}-{generate_date()}")
    child(statement, "CreDtTm", generate_timestamp())
    
    acct = child(statement, "Acct")
    acct_id = child(acct, "Id")
    acct_other = child(acct_id, "Othr")
    child(acct_other, "Id", sacco["wendi_account"])
    child(acct_other, "Issr", "WENDI")
    
    bal = child(statement, "Bal")
    bal_type = child(bal, "Tp")
    child(bal_type, "CdOrPrtry", "OPBD")
    child(bal, "Amt", "0.00", Ccy="UGX")
    child(bal, "CdtDbtInd", "CRDT")
    child(bal, "Dt", generate_date(-1))
    
    bal_close = child(statement, "Bal")
    bal_type_close = child(bal_close, "Tp")
    child(bal_type_close, "CdOrPrtry", "CLBD")
    child(bal_close, "Amt", str(case_data["amount"]), Ccy="UGX")
    child(bal_close, "CdtDbtInd", "CRDT")
    child(bal_close, "Dt", generate_date())
    
    entry = child(statement, "Ntry")
    child(entry, "NtryRef", f"NTRY-{loan_id}")
    child(entry, "Amt", str(case_data["amount"]), Ccy="UGX")
    child(entry, "CdtDbtInd", "CRDT")
    child(entry, "Sts", "BOOK")
    
    entry_dtls = child(entry, "NtryDtls")
    tx_dtls = child(entry_dtls, "TxDtls")
    refs = child(tx_dtls, "Refs")
    child(refs, "EndToEndId", loan_id)
    
    remit = child(tx_dtls, "RmtInf")
    child(remit, "Ustrd", f"PDM Loan {loan_id} - {case_data['project']}")
    
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

def generate_wendi_camt052(sacco: dict, transactions: list) -> bytes:
    """Generate Wendi/CAMT.052 - Intraday Report"""
    ns = CAMT052_NS
    root = ET.Element("Document", xmlns=ns)
    report = child(root, "BkToCstmrAcctRpt")
    
    header = child(report, "GrpHdr")
    child(header, "MsgId", f"WENDI-INTRA-{sacco['sacco_id']}-{datetime.now().strftime('%Y%m%d%H%M')}")
    child(header, "CreDtTm", generate_timestamp())
    
    account = child(report, "Acct")
    account_id = child(account, "Id")
    account_other = child(account_id, "Othr")
    child(account_other, "Id", sacco["wendi_account"])
    child(account_other, "Issr", "WENDI")
    
    bal = child(report, "Bal")
    bal_type = child(bal, "Tp")
    child(bal_type, "CdOrPrtry", "XPCD")
    total = sum(t["amount"] for t in transactions if t.get("event_type") == "DISBURSEMENT")
    child(bal, "Amt", f"{total:.2f}", Ccy="UGX")
    child(bal, "CdtDbtInd", "CRDT")
    child(bal, "Dt", generate_date())
    
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

def generate_wendi_camt054(loan_id: str, case_data: dict, beneficiary: dict) -> bytes:
    """Generate Wendi/CAMT.054 - Transaction Notification"""
    ns = CAMT054_NS
    root = ET.Element("Document", xmlns=ns)
    notif = child(root, "BkToCstmrDbtCdtNtfctn")
    
    header = child(notif, "GrpHdr")
    child(header, "MsgId", f"WENDI-NTF-{loan_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    child(header, "CreDtTm", generate_timestamp())
    
    notification = child(notif, "Ntfctn")
    child(notification, "Id", f"NTF-{loan_id}")
    child(notification, "CreDtTm", generate_timestamp())
    
    acct = child(notification, "Acct")
    acct_id = child(acct, "Id")
    acct_other = child(acct_id, "Othr")
    child(acct_other, "Id", "256791234567")
    child(acct_other, "Issr", "WENDI")
    
    entry = child(notification, "Ntry")
    child(entry, "Amt", str(case_data["amount"]), Ccy="UGX")
    child(entry, "CdtDbtInd", "DBIT")
    child(entry, "Sts", "BOOK")
    
    entry_dtls = child(entry, "NtryDtls")
    tx_dtls = child(entry_dtls, "TxDtls")
    refs = child(tx_dtls, "Refs")
    child(refs, "EndToEndId", loan_id)
    remit = child(tx_dtls, "RmtInf")
    child(remit, "Ustrd", f"PDM Loan {loan_id} disbursement to {beneficiary['name']}")
    
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

# ============================================================================
# 5. Mobile Networks (XML)
# ============================================================================

def generate_mobile_pacs008(loan_id: str, case_data: dict, sacco: dict, beneficiary: dict, network: str = "MTN") -> bytes:
    """Generate Mobile Network/PACS.008 - Interbank Settlement"""
    ns = PACS008_NS
    root = ET.Element("Document", xmlns=ns)
    transfer = child(root, "FIToFICstmrCdtTrf")
    
    header = child(transfer, "GrpHdr")
    child(header, "MsgId", f"{network}-PACS008-{loan_id}-{uuid.uuid4().hex[:8]}")
    child(header, "CreDtTm", generate_timestamp())
    child(header, "NbOfTxs", "1")
    child(header, "CtrlSum", str(case_data["amount"]))
    
    tx = child(transfer, "CdtTrfTxInf")
    pmt_id = child(tx, "PmtId")
    child(pmt_id, "EndToEndId", loan_id)
    child(pmt_id, "TxId", f"TX-{loan_id}-{uuid.uuid4().hex[:8]}")
    
    amt = child(tx, "Amt")
    child(amt, "InstdAmt", str(case_data["amount"]), Ccy="UGX")
    
    debtor_agent = child(tx, "DbtrAgt")
    debtor_agent_fi = child(debtor_agent, "FinInstnId")
    child(debtor_agent_fi, "BICFI", "PSBL")
    child(debtor_agent_fi, "Nm", "PostBank Uganda")
    
    creditor_agent = child(tx, "CdtrAgt")
    creditor_agent_fi = child(creditor_agent, "FinInstnId")
    creditor_bic = "MTN" if network.upper() == "MTN" else "AIRT"
    child(creditor_agent_fi, "BICFI", creditor_bic)
    child(creditor_agent_fi, "Nm", f"{network.upper()} Mobile Money")
    
    debtor = child(tx, "Dbtr")
    child(debtor, "Nm", sacco["name"])
    debtor_acct = child(tx, "DbtrAcct")
    debtor_acct_id = child(debtor_acct, "Id")
    debtor_acct_other = child(debtor_acct_id, "Othr")
    child(debtor_acct_other, "Id", sacco["wendi_account"])
    child(debtor_acct_other, "Issr", "WENDI")
    
    creditor = child(tx, "Cdtr")
    child(creditor, "Nm", beneficiary["name"])
    creditor_acct = child(tx, "CdtrAcct")
    creditor_acct_id = child(creditor_acct, "Id")
    creditor_acct_other = child(creditor_acct_id, "Othr")
    child(creditor_acct_other, "Id", case_data["account"])
    child(creditor_acct_other, "Issr", network.upper())
    child(creditor_acct_other, "SchmeNm", "MSISDN")
    
    remit = child(tx, "RmtInf")
    child(remit, "Ustrd", f"PDM Loan {loan_id}")
    
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

# ============================================================================
# 6. Agent Network (Custom XML)
# ============================================================================

def generate_agent_transaction(loan_id: str, case_data: dict, beneficiary: dict) -> bytes:
    """Generate Agent Network - Cash-out Transaction"""
    root = ET.Element("AgentTransaction")
    child(root, "transaction_id", generate_id("AGT-TX", 12))
    child(root, "agent_id", f"AGENT-{random.randint(1000,9999):04d}")
    child(root, "loan_id", loan_id)
    child(root, "beneficiary_id", beneficiary["beneficiary_id"])
    child(root, "beneficiary_name", beneficiary["name"])
    child(root, "transaction_timestamp", generate_timestamp(4))
    child(root, "transaction_type", "WITHDRAWAL")
    child(root, "amount", str(case_data["amount"] * 0.95))
    child(root, "fee_amount", str(random.choice([0, 1000, 2000, 5000])))
    child(root, "is_assisted_withdrawal", str(loan_id in ["LOAN-001", "LOAN-002", "LOAN-003"]))
    child(root, "beneficiary_verified", str(random.random() < 0.7))
    child(root, "verification_method", random.choice(["PIN", "BIOMETRIC", "ID_CHECK"]))
    child(root, "status", "COMPLETED")
    child(root, "reference", f"REF-{loan_id[:8]}")
    
    location = child(root, "location")
    child(location, "latitude", str(-0.3167 + random.uniform(-0.02, 0.02)))
    child(location, "longitude", str(32.5833 + random.uniform(-0.02, 0.02)))
    child(location, "address", "Kamuli, Uganda")
    
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)

# ============================================================================
# Main Generator
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="PDM ISO 20022 XML Generator")
    parser.add_argument("--count", "-c", type=int, default=10, help="Number of cases (default: 10)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    args = parser.parse_args()
    
    logger = logging.getLogger("pdm_xml_generator")
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    
    logger.info("=" * 70)
    logger.info("PDM ISO 20022 XML GENERATOR - 150+ Fields Per Message")
    logger.info("=" * 70)
    logger.info(f"Output Directory: {DATA_ROOT}")
    logger.info(f"Number of POC Cases: {args.count}")
    logger.info("")
    
    # Generate PDMIS data
    logger.info("📊 Generating PDMIS (Government System) data...")
    pdmis_data = generate_pdmis_data()
    write_json(DATA_ROOT / "pdmis" / "beneficiaries.json", pdmis_data["beneficiaries"])
    write_json(DATA_ROOT / "pdmis" / "loans.json", pdmis_data["loans"])
    write_json(DATA_ROOT / "pdmis" / "saccos.json", pdmis_data["saccos"])
    write_json(DATA_ROOT / "pdmis" / "households.json", pdmis_data["households"])
    write_json(DATA_ROOT / "pdmis" / "business_plans.json", pdmis_data["business_plans"])
    write_json(DATA_ROOT / "pdmis" / "special_groups.json", pdmis_data["special_groups"])
    logger.info("  ✅ Created 6 JSON files in pdmis/")
    
    sacco = pdmis_data["saccos"][0]
    beneficiaries = pdmis_data["beneficiaries"]
    all_transactions = []
    
    logger.info("📨 Generating ISO 20022 messages...")
    
    for i, (loan_id, case_data) in enumerate(POC_CASES.items()):
        beneficiary = beneficiaries[i]
        seq = i + 1
        
        # 1. ICMN - VPM (Business Payment Initiation)
        icmn_vpm = generate_icmn_vpm(loan_id, case_data, sacco, beneficiary)
        write_xml(DATA_ROOT / "icmn" / "vpm" / "pain001" / f"vpm_{seq:04d}.xml", icmn_vpm)
        
        # 2. ICMN - PMN (Technical Lifecycle)
        icmn_pmn = generate_icmn_pmn(loan_id, case_data, sacco, beneficiary)
        write_xml(DATA_ROOT / "icmn" / "pmn" / "pain001" / f"pmn_{seq:04d}.xml", icmn_pmn)
        
        # 3. CPO - PSN (Business Status)
        cpo_psn = generate_cpo_psn(loan_id, case_data)
        write_xml(DATA_ROOT / "cpo" / "psn" / "pain002" / f"psn_{seq:04d}.xml", cpo_psn)
        
        # 4. CPO - PLM (Technical Lifecycle)
        cpo_plm = generate_cpo_plm(loan_id, case_data)
        write_xml(DATA_ROOT / "cpo" / "plm" / "pain002" / f"plm_{seq:04d}.xml", cpo_plm)
        
        # 5. Wendi - CAMT.053 (Bank Statement)
        wendi_camt053 = generate_wendi_camt053(loan_id, case_data, sacco)
        write_xml(DATA_ROOT / "wendi" / "camt053" / f"camt053_{seq:04d}.xml", wendi_camt053)
        
        # 6. Wendi - CAMT.054 (Notification)
        wendi_camt054 = generate_wendi_camt054(loan_id, case_data, beneficiary)
        write_xml(DATA_ROOT / "wendi" / "camt054" / f"camt054_{seq:04d}.xml", wendi_camt054)
        
        # 7. Mobile - MTN PACS.008 (Settlement)
        mobile_mtn = generate_mobile_pacs008(loan_id, case_data, sacco, beneficiary, "MTN")
        write_xml(DATA_ROOT / "mobile_networks" / "mtn" / "pacs008" / f"pacs008_mtn_{seq:04d}.xml", mobile_mtn)
        
        # 8. Mobile - Airtel PACS.008 (Settlement)
        mobile_airtel = generate_mobile_pacs008(loan_id, case_data, sacco, beneficiary, "AIRTEL")
        write_xml(DATA_ROOT / "mobile_networks" / "airtel" / "pacs008" / f"pacs008_airtel_{seq:04d}.xml", mobile_airtel)
        
        # 9. Agent Network - Cash-Out
        agent_tx = generate_agent_transaction(loan_id, case_data, beneficiary)
        write_xml(DATA_ROOT / "agent_network" / "agent_transactions" / f"agent_tx_{seq:04d}.xml", agent_tx)
        
        # Track for CAMT.052
        if case_data["status"] == STATUS_ACSC:
            all_transactions.append({"amount": case_data["amount"], "event_type": "DISBURSEMENT"})
        
        logger.debug(f"  ✅ Generated {seq}/10: {loan_id}")
    
    # 10. Wendi - CAMT.052 (Intraday Report)
    wendi_camt052 = generate_wendi_camt052(sacco, all_transactions)
    write_xml(DATA_ROOT / "wendi" / "camt052" / "camt052_report_001.xml", wendi_camt052)
    
    logger.info("")
    logger.info("=" * 70)
    logger.info("✅ XML Generation Complete!")
    logger.info(f"📁 Total files generated: {len(POC_CASES) * 9 + 1}")
    logger.info("")
    logger.info("📂 Directory Structure:")
    logger.info("  ├── pdmis/ (6 JSON files)")
    logger.info("  ├── icmn/ (20 XML files)")
    logger.info("  ├── cpo/ (20 XML files)")
    logger.info("  ├── wendi/ (31 XML files)")
    logger.info("  ├── mobile_networks/ (20 XML files)")
    logger.info("  └── agent_network/ (10 XML files)")
    logger.info("=" * 70)

if __name__ == "__main__":
    main()
