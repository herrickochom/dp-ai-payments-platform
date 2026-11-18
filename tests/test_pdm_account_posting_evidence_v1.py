"""Regression tests for Wendi financial evidence enforcement."""

import sys
import unittest
from pathlib import Path

BASE = (
    Path(__file__).resolve().parents[1]
    / "services"
    / "payment-xml-generator"
)
sys.path.insert(0, str(BASE))

from pdm_account_posting_evidence_v1 import (
    MissingAuthoritativePostingEvidence,
    require_authoritative_posting_evidence_v1,
)
from wendi_generator import generate_camt053, generate_camt054


class EvidenceBoundaryTests(unittest.TestCase):

    def test_missing_evidence(self):
        with self.assertRaises(MissingAuthoritativePostingEvidence):
            require_authoritative_posting_evidence_v1(
                {}, document_type="camt.053"
            )

    def test_incomplete_evidence(self):
        with self.assertRaises(MissingAuthoritativePostingEvidence):
            require_authoritative_posting_evidence_v1(
                {"account_posting_evidence_v1": {"account_id": "A"}},
                document_type="camt.054",
            )

    def test_camt053_fails_closed(self):
        with self.assertRaises(MissingAuthoritativePostingEvidence):
            generate_camt053({})

    def test_camt054_fails_closed(self):
        with self.assertRaises(MissingAuthoritativePostingEvidence):
            generate_camt054({})

    def test_unsupported_document(self):
        with self.assertRaises(ValueError):
            require_authoritative_posting_evidence_v1(
                {}, document_type="pain.001"
            )


if __name__ == "__main__":
    unittest.main()
