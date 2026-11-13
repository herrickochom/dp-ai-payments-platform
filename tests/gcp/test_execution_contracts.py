import importlib.util
from pathlib import Path
import unittest

MODULE = Path(__file__).resolve().parents[2] / 'pipelines/gcp/raw_to_bronze/execution_contracts.py'
spec = importlib.util.spec_from_file_location('execution_contracts', MODULE)
m = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = m
spec.loader.exec_module(m)

class ContractTests(unittest.TestCase):
    def test_merge_targets_distinct(self):
        sql = m.merge_sql(staging_table='proj-dev.bronze.payments_raw_v1_staging', bronze_table='proj-dev.bronze.payments_raw_v1', run_id='run_001')
        self.assertIn('WHEN NOT MATCHED THEN INSERT', sql)
        self.assertIn('ROW_NUMBER()', sql)
        self.assertIn("'_ingest_run_id'", "'_ingest_run_id'")
        self.assertIn('`_ingest_run_id`', sql)
        self.assertIn('`_kafka_offset`', sql)
    def test_reject_same_table(self):
        with self.assertRaises(ValueError):
            m.merge_sql(staging_table='p.d.t', bronze_table='p.d.t', run_id='a')
    def test_reject_injection(self):
        for run in ("x';DROP TABLE a;--", '', 'a b'):
            with self.assertRaises(ValueError):
                m.merge_sql(staging_table='p.d.s', bronze_table='p.d.b', run_id=run)
        with self.assertRaises(ValueError):
            m.merge_sql(staging_table='p.d.s`x', bronze_table='p.d.b', run_id='a')
    def test_reconciliation_balanced(self):
        r=m.Reconciliation(12,10,2,3,7,5,2)
        self.assertEqual(r.as_dict()['staged_records'],7)
    def test_reconciliation_rejects_mismatch(self):
        with self.assertRaises(ValueError):
            m.Reconciliation(12,10,1,3,7,5,2).validate()
        with self.assertRaises(ValueError):
            m.Reconciliation(12,10,2,3,7,6,2).validate()
    def test_reconciliation_rejects_invalid_types(self):
        with self.assertRaises(ValueError):
            m.Reconciliation(True,0,0,0,0,0,0).validate()
        with self.assertRaises(ValueError):
            m.Reconciliation(0,0,0,0,0,0,-1).validate()

if __name__ == '__main__': unittest.main()
