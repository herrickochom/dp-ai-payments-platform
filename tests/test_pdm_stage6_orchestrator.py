import copy
import unittest
from pathlib import Path
from lxml import etree
from pdm_stage6_orchestrator import build,OrchestrationError

ROOT=Path(__file__).resolve().parent.parent
XSD=ROOT/'contracts/iso20022/xsd'
class TestStage6(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.x1=etree.XMLSchema(etree.parse(str(XSD/'pain.001.001.09.xsd')))
        cls.x2=etree.XMLSchema(etree.parse(str(XSD/'pain.002.001.12.xsd')))
    def run_manifest(self,m): return build(m,self.x1,self.x2)
    def base(self):
        return dict(sacco_ids=['S1'],funding_allocations=[],funding_instructions=[],funding_statuses=[],funding_credits=[],loans=[],disbursement_instructions=[],disbursement_statuses=[])
    def test_empty_is_no_funding(self):
        r=self.run_manifest(self.base());self.assertEqual(r['messages'],{});self.assertEqual(r['funding_reconciliation'],{})
    def test_orphan_status_rejected(self):
        m=self.base();m['funding_statuses']=[dict(status_id='S',instruction_id='X',status='ACCP',status_date='2026-11-17',created_at='2026-11-17T12:00:00Z')]
        with self.assertRaises(Exception): self.run_manifest(m)
    def test_unauthorized_loan_rejected(self):
        m=self.base();m['loans']=[dict(loan_id='L',beneficiary_id='B',sacco_id='S1',approval_status='APPROVED',amount_approved_ugx=100)]
        m['disbursement_instructions']=[dict(instruction_id='I',loan_id='L',beneficiary_id='B',sacco_id='S1',authorization='PENDING',amount_ugx='50')]
        with self.assertRaisesRegex(OrchestrationError,'authorization'):self.run_manifest(m)
    def test_duplicate_loan_rejected(self):
        m=self.base();m['loans']=[dict(loan_id='L'),dict(loan_id='L')]
        with self.assertRaisesRegex(OrchestrationError,'duplicate loan_id'):self.run_manifest(m)
    def test_missing_evidence_lists_rejected(self):
        m=self.base();del m['funding_credits']
        with self.assertRaises(OrchestrationError):self.run_manifest(m)
