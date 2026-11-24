import importlib.util
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1] / 'services/payment-xml-generator/pdm_membership_contract.py'
spec = importlib.util.spec_from_file_location('pdm_membership_contract', MODULE)
contract = importlib.util.module_from_spec(spec)
spec.loader.exec_module(contract)

B = [{'beneficiary_id':'B1','nin':'N1'}, {'beneficiary_id':'B2','nin':'N1'}]
S = [{'sacco_id':'S1'}]
L = [{'loan_id':'L1','beneficiary_id':'B1','sacco_id':'S1'}]
M = [{'membership_id':'M1','beneficiary_id':'B1','sacco_id':'S1','status':'ACTIVE','effective_from':'2026-01-01'}]
G = [{'group_id':'G1','sacco_id':'S1'}]
GM = [{'group_membership_id':'GM1','group_id':'G1','beneficiary_id':'B1'}]

class MembershipContractTests(unittest.TestCase):
    def test_existing_loans_without_membership_allowed(self):
        self.assertEqual(contract.validate_memberships(B,S,L)['memberships'],0)
    def test_duplicate_nin_preserved(self):
        self.assertEqual(contract.validate_memberships(B,S,L)['duplicate_nin_groups'],1)
    def test_valid_group(self):
        self.assertEqual(contract.validate_memberships(B,S,L,M,G,GM)['group_memberships'],1)
    def test_unknown_loan_beneficiary(self):
        with self.assertRaises(contract.MembershipContractError):
            contract.validate_memberships(B,S,[dict(L[0],beneficiary_id='X')])
    def test_unknown_membership_sacco(self):
        with self.assertRaises(contract.MembershipContractError):
            contract.validate_memberships(B,S,L,[dict(M[0],sacco_id='X')])
    def test_group_without_membership(self):
        with self.assertRaises(contract.MembershipContractError):
            contract.validate_memberships(B,S,L,(),G,GM)
    def test_duplicate_membership_id(self):
        with self.assertRaises(contract.MembershipContractError):
            contract.validate_memberships(B,S,L,M+M)
    def test_overlap(self):
        extra=dict(M[0],membership_id='M2',effective_from='2026-02-01')
        with self.assertRaises(contract.MembershipContractError):
            contract.validate_memberships(B,S,L,M+[extra])
    def test_invalid_dates(self):
        with self.assertRaises(contract.MembershipContractError):
            contract.validate_memberships(B,S,L,[dict(M[0],effective_to='2025-01-01')])

if __name__ == '__main__':
    unittest.main()
