import unittest
from pathlib import Path
from lxml import etree
from pdm_wendi_disbursement_iso20022 import pain001,pain002,DisbursementContractError
BASE=Path(__file__).resolve().parents[1]/'contracts/iso20022/xsd'
D=dict(loan_id='LOAN-001',beneficiary_id='BEN-001',sacco_id='SACCO-001',instruction_id='WDI-001',amount_ugx='1000000',created_at='2026-11-17T09:00:00Z',execution_date='2026-11-17',initiating_party='TEST SACCO',debtor_name='TEST SACCO',debtor_account='TEST-SACCO-ACCOUNT',debtor_agent_bic='ABCDUGKA',creditor_name='TEST BENEFICIARY',creditor_account='TEST-MOBILE-ACCOUNT',creditor_agent_bic='EFGHUGKA',uetr='123e4567-e89b-42d3-a456-426614174000')
class WendiDisbursementTests(unittest.TestCase):
    def schema(self,name,xml):
        etree.XMLSchema(etree.parse(str(BASE/name))).assertValid(etree.fromstring(xml))
    def test_pain001_xsd(self): self.schema('pain.001.001.09.xsd',pain001(D))
    def test_pain002_accepted_xsd(self): self.schema('pain.002.001.12.xsd',pain002(D,'STAT-001','ACSP','2026-11-17T10:00:00Z'))
    def test_pain002_rejected_xsd(self): self.schema('pain.002.001.12.xsd',pain002(D,'STAT-002','RJCT','2026-11-17T10:00:00Z','AC01'))
    def test_correlated_ids(self):
        a=etree.fromstring(pain001(D));b=etree.fromstring(pain002(D,'STAT-001','ACSP','2026-11-17T10:00:00Z'))
        for key in ('loan_id','instruction_id','uetr'):
            self.assertIn(D[key].encode(),etree.tostring(a))
            self.assertIn(D[key].encode(),etree.tostring(b))
    def test_missing_account_rejected(self):
        d=D.copy();d.pop('creditor_account')
        with self.assertRaises(DisbursementContractError): pain001(d)
    def test_invalid_amount_rejected(self):
        d=D.copy();d['amount_ugx']='-1'
        with self.assertRaises(DisbursementContractError): pain001(d)
    def test_rejection_reason_required(self):
        with self.assertRaises(DisbursementContractError): pain002(D,'STAT-002','RJCT','2026-11-17T10:00:00Z')
    def test_no_loan_only_generation(self):
        with self.assertRaises(DisbursementContractError): pain001({'loan_id':'LOAN-001'})
