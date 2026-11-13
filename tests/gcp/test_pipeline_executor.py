import importlib.util
import sys
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[2] / 'pipelines/gcp/raw_to_bronze/pipeline_executor.py'
spec = importlib.util.spec_from_file_location('pipeline_executor', MODULE)
m = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = m
spec.loader.exec_module(m)

A=('payments',0,1)
B=('payments',0,2)

class ExecutorTests(unittest.TestCase):
    def test_first_load(self):
        s=m.FakeBronzeSink(); r=s.execute('run_1',[A,B],1,3)
        self.assertEqual((r['inserted'],len(s.bronze)),(2,2))
        self.assertEqual(s.events,['stage','verify_staging','merge','verify_sink','commit'])
    def test_retry_same_run(self):
        s=m.FakeBronzeSink(); s.execute('run_1',[A]); r=s.execute('run_1',[A]); self.assertTrue(r['replay']); self.assertEqual(r['inserted'],0)
    def test_new_run_existing_identity(self):
        s=m.FakeBronzeSink(); s.execute('run_1',[A]); r=s.execute('run_2',[A,B]); self.assertEqual(r['inserted'],1)
    def test_reuse_changed_input_rejected(self):
        s=m.FakeBronzeSink(); s.execute('run_1',[A]);
        with self.assertRaises(m.ContractFailure): s.execute('run_1',[B])
    def test_duplicate_rejected(self):
        with self.assertRaises(m.ContractFailure): m.FakeBronzeSink().execute('run_1',[A,A])
    def test_bad_identity_rejected(self):
        with self.assertRaises(m.ContractFailure): m.FakeBronzeSink().execute('run_1',[('payments',True,2)])
    def test_count_mismatch_rejected(self):
        with self.assertRaises(m.ContractFailure): m.FakeBronzeSink().execute('run_1',[A],1,1)
    def test_premerge_failure_retry(self):
        s=m.FakeBronzeSink(fail_at='merge')
        with self.assertRaises(RuntimeError): s.execute('run_1',[A])
        self.assertEqual(len(s.bronze),0)
        self.assertEqual(s.execute('run_1',[A])['inserted'],1)
    def test_postmerge_failure_retry(self):
        s=m.FakeBronzeSink(fail_at='verify_sink')
        with self.assertRaises(RuntimeError): s.execute('run_1',[A])
        self.assertEqual(s.execute('run_1',[A])['inserted'],0)
        self.assertEqual(len(s.bronze),1)
    def test_commit_failure_retry(self):
        s=m.FakeBronzeSink(fail_at='commit')
        with self.assertRaises(RuntimeError): s.execute('run_1',[A])
        self.assertEqual(s.execute('run_1',[A])['inserted'],0)
    def test_staging_conflict_rejected(self):
        s=m.FakeBronzeSink(staging={'run_1':frozenset([B])})
        with self.assertRaises(m.ContractFailure): s.execute('run_1',[A])
    def test_invalid_run_rejected(self):
        with self.assertRaises(m.ContractFailure): m.FakeBronzeSink().execute('bad;run',[A])

if __name__=='__main__': unittest.main()
