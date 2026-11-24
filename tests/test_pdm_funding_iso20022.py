import unittest
from pathlib import Path
from lxml import etree
from pdm_funding_iso20022 import pain001,pain002,FundingMessageError
BASE=Path(__file__).resolve().parents[1]/'contracts/iso20022/xsd'
D=dict(allocation_id='ALLOC-001',instruction_id='INST-001',sacco_id='SACCO-001',amount_ugx='30000000',instruction_date='2026-11-17',created_at='2026-11-17T09:00:00Z',execution_date='2026-11-17',initiating_party='TEST GOVERNMENT FUNDING ENTITY',debtor_name='TEST GOVERNMENT ACCOUNT',debtor_account='TEST-DEBTOR-001',debtor_agent_bic='ABCDUGKA',creditor_name='TEST SACCO',creditor_account='TEST-CREDITOR-001',creditor_agent_bic='EFGHUGKA',uetr='123e4567-e89b-42d3-a456-426614174000')
class FundingIsoTests(unittest.TestCase):
    def test_pain001_schema(self):
        x=etree.XMLSchema(etree.parse(str(BASE/'pain.001.001.09.xsd'))); doc=etree.fromstring(pain001(D)); x.assertValid(doc)
    def test_pain002_schema(self):
        x=etree.XMLSchema(etree.parse(str(BASE/'pain.002.001.12.xsd'))); doc=etree.fromstring(pain002(D,'STATUS-001','ACSP','2026-11-17T10:00:00Z')); x.assertValid(doc)
    def test_rejection_schema(self):
        x=etree.XMLSchema(etree.parse(str(BASE/'pain.002.001.12.xsd'))); x.assertValid(etree.fromstring(pain002(D,'STATUS-002','RJCT','2026-11-17T10:00:00Z','AC01')))
    def test_no_loan_context(self):
        with self.assertRaises(FundingMessageError): pain001({'loan_id':'LOAN-1'})
    def test_no_inferred_accounts(self):
        d=D.copy(); d.pop('creditor_account');
        with self.assertRaises(FundingMessageError): pain001(d)
