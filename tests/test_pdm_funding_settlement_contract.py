import unittest
from pdm_funding_settlement_contract import reconcile_funding, SettlementContractError

A=[dict(allocation_id='A1',sacco_id='S1',amount_ugx=100,allocation_date='2026-01-01',funding_status='ALLOCATED')]
I=[dict(instruction_id='I1',allocation_id='A1',amount_ugx=70,instruction_date='2026-01-02')]
S=[dict(status_id='T1',instruction_id='I1',status='ACCEPTED',status_date='2026-01-03')]
C=[dict(credit_id='C1',instruction_id='I1',sacco_id='S1',amount_ugx=40,credit_date='2026-01-04')]

class SettlementTests(unittest.TestCase):
    def run_contract(self, **kwargs):
        args=dict(sacco_ids={'S1'},allocations=A,instructions=I,statuses=S,credits=C)
        args.update(kwargs)
        return reconcile_funding(**args)
    def test_empty_evidence(self):
        self.assertEqual(reconcile_funding({'S1'}),{})
    def test_partial_credit(self):
        self.assertEqual(self.run_contract()['A1'],dict(sacco_id='S1',allocated_ugx=100,instructed_ugx=70,credited_ugx=40,uncredited_allocation_ugx=60))
    def test_status_not_credit(self):
        self.assertEqual(self.run_contract(credits=[])['A1']['credited_ugx'],0)
    def test_unknown_sacco(self):
        with self.assertRaises(SettlementContractError): self.run_contract(sacco_ids={'S2'})
    def test_over_instruction(self):
        with self.assertRaises(SettlementContractError): self.run_contract(instructions=I+[dict(instruction_id='I2',allocation_id='A1',amount_ugx=40,instruction_date='2026-01-03')])
    def test_duplicate_credit(self):
        with self.assertRaises(SettlementContractError): self.run_contract(credits=C+C)
    def test_over_credit(self):
        with self.assertRaises(SettlementContractError): self.run_contract(credits=[dict(C[0],amount_ugx=71)])
    def test_rejected_credit(self):
        with self.assertRaises(SettlementContractError): self.run_contract(statuses=[dict(S[0],status='REJECTED')])
    def test_wrong_credit_sacco(self):
        with self.assertRaises(SettlementContractError): self.run_contract(credits=[dict(C[0],sacco_id='S2')])
    def test_multiple_allocations(self):
        a=A+[dict(A[0],allocation_id='A2')]
        self.assertEqual(len(self.run_contract(allocations=a)),2)
    def test_conflicting_terminal_statuses(self):
        with self.assertRaises(SettlementContractError): self.run_contract(statuses=S+[dict(S[0],status_id='T2',status='REJECTED')])
    def test_credit_without_status(self):
        self.assertEqual(self.run_contract(statuses=[])['A1']['credited_ugx'],40)
    def test_invalid_money(self):
        with self.assertRaises(SettlementContractError): self.run_contract(allocations=[dict(A[0],amount_ugx='NaN')])
    def test_credit_before_instruction(self):
        with self.assertRaises(SettlementContractError): self.run_contract(credits=[dict(C[0],credit_date='2026-01-01')])

if __name__=='__main__': unittest.main()
