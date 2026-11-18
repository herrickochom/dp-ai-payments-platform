"""Tests for evidence-based CAMT account posting contract."""

import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PATH = (
    ROOT
    / "services/payment-xml-generator"
    / "pdm_account_posting_contract.py"
)

spec = importlib.util.spec_from_file_location(
    "pdm_account_posting_contract", PATH
)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)

Error = module.AccountPostingContractError

# Illustrative test fixture only; not a production transaction code.
TEST_CODE = "TEST_ONLY_CODE"


def posting(**changes):
    row = {
        "posting_id": "POST-001",
        "account_id": "ACCOUNT-001",
        "account_owner_id": "OWNER-001",
        "payment_transaction_id": "TX-001",
        "loan_id": "LOAN-001",
        "event_type": "WALLET_CREDIT",
        "amount_ugx": "100000",
        "credit_debit_indicator": "CRDT",
        "booking_datetime": "2026-11-18T10:00:00+00:00",
        "value_date": "2026-11-18",
        "account_servicer_reference": "ASR-001",
        "bank_transaction_code": TEST_CODE,
    }
    row.update(changes)
    return row


def validate(rows):
    return module._validate_postings_base_v1(
        rows,
        governed_bank_transaction_codes={TEST_CODE},
    )


class AccountPostingContractTests(unittest.TestCase):

    def test_valid_evidenced_posting(self):
        rows = validate([posting()])
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0]["amount_ugx"]), "100000")

    def test_missing_posting_evidence_rejected(self):
        row = posting()
        del row["account_id"]
        with self.assertRaises(Error):
            validate([row])

    def test_duplicate_posting_rejected(self):
        with self.assertRaises(Error):
            validate([posting(), posting()])

    def test_ungoverned_code_rejected(self):
        with self.assertRaises(Error):
            validate([
                posting(bank_transaction_code="UNKNOWN")
            ])

    def test_invalid_direction_rejected(self):
        with self.assertRaises(Error):
            validate([
                posting(credit_debit_indicator="UNKNOWN")
            ])

    def test_invalid_amount_rejected(self):
        with self.assertRaises(Error):
            validate([posting(amount_ugx="-100")])

    def test_missing_timezone_rejected(self):
        with self.assertRaises(Error):
            validate([
                posting(
                    booking_datetime="2026-11-18T10:00:00"
                )
            ])

    def test_statement_reconciliation(self):
        rows = validate([posting()])
        result = module._reconcile_statement_base_v1(
            account_id="ACCOUNT-001",
            period_start="2026-11-18T00:00:00+00:00",
            period_end="2026-11-19T00:00:00+00:00",
            opening_balance_ugx="50000",
            closing_balance_ugx="150000",
            postings=rows,
        )
        self.assertEqual(
            str(result["closing_balance_ugx"]),
            "150000",
        )

    def test_unreconciled_statement_rejected(self):
        rows = validate([posting()])
        with self.assertRaises(Error):
            module._reconcile_statement_base_v1(
                account_id="ACCOUNT-001",
                period_start="2026-11-18T00:00:00+00:00",
                period_end="2026-11-19T00:00:00+00:00",
                opening_balance_ugx="50000",
                closing_balance_ugx="999999",
                postings=rows,
            )

    def test_cross_account_posting_rejected(self):
        rows = validate([posting()])
        with self.assertRaises(Error):
            module._reconcile_statement_base_v1(
                account_id="DIFFERENT-ACCOUNT",
                period_start="2026-11-18T00:00:00+00:00",
                period_end="2026-11-19T00:00:00+00:00",
                opening_balance_ugx="0",
                closing_balance_ugx="100000",
                postings=rows,
            )


if __name__ == "__main__":
    unittest.main()


# Phase 2 tests: synthetic fixtures, never production evidence.

class AccountPostingPhase2Tests(unittest.TestCase):

    CODE = {
        "domain": "TEST",
        "family": "T001",
        "subfamily": "T002",
    }

    GOVERNED = {("TEST", "T001", "T002")}

    def evidence(self, **changes):
        row = posting()
        row["bank_transaction_code"] = dict(self.CODE)
        row.update(changes)
        return row

    def validated(self, rows):
        return module.validate_postings_v1(
            rows,
            governed_bank_transaction_codes=self.GOVERNED,
            account_ownership={"ACCOUNT-001": "OWNER-001"},
            authoritative_posting_ids={"POST-001"},
        )

    def test_structured_code_accepted(self):
        rows = self.validated([self.evidence()])
        self.assertEqual(
            rows[0]["bank_transaction_code"],
            self.CODE,
        )

    def test_unapproved_code_rejected(self):
        with self.assertRaises(Error):
            self.validated([
                self.evidence(
                    bank_transaction_code={
                        "domain": "XXXX",
                        "family": "T001",
                        "subfamily": "T002",
                    }
                )
            ])

    def test_missing_authoritative_evidence_rejected(self):
        with self.assertRaises(Error):
            self.validated([
                self.evidence(posting_id="UNVERIFIED")
            ])

    def test_account_owner_mismatch_rejected(self):
        with self.assertRaises(Error):
            self.validated([
                self.evidence(account_owner_id="OTHER")
            ])

    def test_duplicate_postings_rejected(self):
        with self.assertRaises(Error):
            self.validated([
                self.evidence(),
                self.evidence(),
            ])

    def test_reconciled_statement_v1(self):
        result = module.reconcile_statement_v1(
            account_id="ACCOUNT-001",
            account_owner_id="OWNER-001",
            period_start="2026-11-18T00:00:00+00:00",
            period_end="2026-11-19T00:00:00+00:00",
            opening_balance_ugx="50000",
            closing_balance_ugx="150000",
            postings=[self.evidence()],
            governed_bank_transaction_codes=self.GOVERNED,
            account_ownership={"ACCOUNT-001": "OWNER-001"},
            authoritative_posting_ids={"POST-001"},
        )
        self.assertEqual(result["posting_count"], 1)

    def test_unreconciled_statement_v1_rejected(self):
        with self.assertRaises(Error):
            module.reconcile_statement_v1(
                account_id="ACCOUNT-001",
                account_owner_id="OWNER-001",
                period_start="2026-11-18T00:00:00+00:00",
                period_end="2026-11-19T00:00:00+00:00",
                opening_balance_ugx="50000",
                closing_balance_ugx="999999",
                postings=[self.evidence()],
                governed_bank_transaction_codes=self.GOVERNED,
                account_ownership={"ACCOUNT-001": "OWNER-001"},
                authoritative_posting_ids={"POST-001"},
            )
