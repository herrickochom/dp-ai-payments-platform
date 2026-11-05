'''Offline Stage 7 contract checks. These do not prove live PostgreSQL behaviour.'''
import ast
import pathlib
import unittest
ROOT=pathlib.Path(__file__).resolve().parents[3]
RT=ROOT/'orchestration/gcp_composer/runtime/gcp_postgres_ledger.py'
SQL=ROOT/'orchestration/gcp_composer/migrations/001_payment_execution_ledger.sql'
class Stage7ContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.src=RT.read_text();cls.sql=SQL.read_text();cls.tree=ast.parse(cls.src)
    def test_queue_unchanged_and_separate(self):
        self.assertIn('gcp_payment_executions',self.sql)
        self.assertNotIn('ALTER TABLE batch_executions',self.sql)
        self.assertNotIn('ALTER TABLE transform_runs',self.sql)
    def test_claim_skips_locked(self): self.assertIn('FOR UPDATE SKIP LOCKED',self.src)
    def test_database_time_fencing(self): self.assertIn('lease_expires_at>=clock_timestamp()',self.src)
    def test_expiry_quarantines(self):
        self.assertIn("status='RECONCILIATION_REQUIRED'",self.src)
        self.assertNotIn("SET status='QUEUED'",self.src)
    def test_no_cloud_calls(self):
        for token in ('google.cloud','subprocess','terraform apply','dataflow.projects.locations.jobs.create'):
            self.assertNotIn(token,self.src)
    def test_external_evidence_required(self):
        self.assertIn("evidence.get('verified') is not True",self.src)
        self.assertIn("evidence.get('bigquery_verified') is not True",self.src)
    def test_parameterized_sql(self): self.assertIn('%s',self.src)
    def test_job_id_unique(self): self.assertIn('dataflow_job_id text UNIQUE',self.sql)
    def test_no_scheduler_or_deployment(self): self.assertNotIn('schedule_interval',self.src)
if __name__=='__main__': unittest.main()
