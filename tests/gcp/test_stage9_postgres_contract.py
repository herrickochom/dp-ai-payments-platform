"""Offline Stage 9 schema contract checks; no database or cloud connection."""
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
MIG = ROOT / 'orchestration/gcp_composer/migrations/008_gcp_dataflow_submission_outbox.sql'
BASE = ROOT / 'orchestration/gcp_composer/migrations/007_gcp_payment_execution_ledger.sql'

class Stage9PostgresContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = MIG.read_text()
        cls.base = BASE.read_text()

    def test_additive_table(self):
        self.assertIn('CREATE TABLE IF NOT EXISTS gcp_dataflow_submission_intents', self.sql)
        self.assertIn('REFERENCES gcp_payment_executions(execution_id)', self.sql)

    def test_attempt_identity(self):
        self.assertIn('PRIMARY KEY (execution_id, attempt_number)', self.sql)
        self.assertIn('UNIQUE (project_id,region,job_name)', self.sql)

    def test_uncertainty_persisted(self):
        self.assertIn("'SUBMISSION_UNCERTAIN'", self.sql)
        self.assertIn('uncertainty_started_at timestamptz', self.sql)

    def test_due_reconciliation(self):
        self.assertIn('next_reconcile_at timestamptz', self.sql)
        self.assertIn('gcp_submission_reconcile_due_idx', self.sql)

    def test_evidence_gate(self):
        self.assertIn('gcp_submission_completed_evidence', self.sql)
        self.assertIn('evidence_digest IS NOT NULL', self.sql)

    def test_preserve_existing_ledger(self):
        self.assertIn('CREATE TABLE IF NOT EXISTS gcp_payment_executions', self.base)
        self.assertNotIn('DROP TABLE', self.sql.upper())
        self.assertNotIn('ALTER TABLE gcp_payment_executions', self.sql)

if __name__ == '__main__':
    unittest.main()
