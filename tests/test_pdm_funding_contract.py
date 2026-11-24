import json
import tempfile
import unittest
from pathlib import Path
from pdm_funding_contract import load_funding_allocations

class FundingContractTests(unittest.TestCase):
    def check(self, rows, sacco_ids={'S-1'}):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'allocations.json'
            path.write_text(json.dumps(rows), encoding='utf-8')
            return load_funding_allocations(path, sacco_ids)

    def valid(self):
        return dict(allocation_id='F-1', sacco_id='S-1', amount_ugx=30000000,
                    allocation_date='2026-11-17', funding_status='ALLOCATED')

    def test_valid_independent_allocation(self):
        self.assertEqual(len(self.check([self.valid()])), 1)

    def test_unknown_sacco(self):
        row = self.valid(); row['sacco_id'] = 'UNKNOWN'
        with self.assertRaises(ValueError): self.check([row])

    def test_duplicate_allocation(self):
        with self.assertRaises(ValueError): self.check([self.valid(), self.valid()])

    def test_invalid_amount(self):
        row = self.valid(); row['amount_ugx'] = -1
        with self.assertRaises(ValueError): self.check([row])

    def test_invalid_status(self):
        row = self.valid(); row['funding_status'] = 'PAID_TO_BENEFICIARY'
        with self.assertRaises(ValueError): self.check([row])

    def test_empty_dataset_allowed(self):
        self.assertEqual(self.check([]), [])

if __name__ == '__main__': unittest.main()
