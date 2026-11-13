import tempfile
import unittest
from pathlib import Path
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import DurableLedger, OfflineAdapter, ContractError, fingerprint_records

SQL='MERGE `project.bronze.target` T USING `project.bronze.stage` S ON TRUE WHEN NOT MATCHED THEN INSERT ROW'
def rows(n=2):
    return [dict(_kafka_topic='test',_kafka_partition=0,_kafka_offset=i) for i in range(n)]

class Fake:
    def __init__(self):
        self.log=[]; self.bronze=0; self.staged=0; self.fail=False; self.bad_stage=False
    def count_bronze(self):
        self.log.append('count_bronze'); return self.bronze
    def stage(self,run_id,records):
        self.log.append('stage'); self.staged=len(records)
    def count_staging(self,run_id):
        self.log.append('count_staging'); return self.staged-1 if self.bad_stage else self.staged
    def merge(self,sql):
        self.log.append('merge')
        if self.fail: raise RuntimeError('injected failure')
        self.bronze+=self.staged

class DurableAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.path=Path(self.temp.name)/'ledger.sqlite3'; self.ledger=DurableLedger(self.path)
    def tearDown(self):
        self.ledger.close(); self.temp.cleanup()
    def test_first_run(self):
        f=Fake(); self.assertEqual(OfflineAdapter(self.ledger,f).execute('run1',rows(),SQL),'COMPLETE'); self.assertEqual(f.bronze,2)
    def test_order(self):
        f=Fake(); OfflineAdapter(self.ledger,f).execute('run1',rows(),SQL)
        self.assertEqual(f.log,['count_bronze','stage','count_staging','merge','count_bronze'])
    def test_completed_retry(self):
        f=Fake(); a=OfflineAdapter(self.ledger,f); a.execute('run1',rows(),SQL)
        self.assertEqual(a.execute('run1',rows(),SQL),'ALREADY_COMPLETE'); self.assertEqual(f.bronze,2)
    def test_persists_across_connection(self):
        fp=fingerprint_records(rows()); self.ledger.prepare('run1',fp,2); self.ledger.close()
        self.ledger=DurableLedger(self.path); self.assertEqual(self.ledger.get('run1')['state'],'PREPARED')
    def test_changed_run_fails(self):
        self.ledger.prepare('run1',fingerprint_records(rows()),2)
        with self.assertRaises(ContractError): self.ledger.prepare('run1',fingerprint_records(rows(3)),3)
    def test_duplicate_identity_fails(self):
        with self.assertRaises(ContractError): fingerprint_records(rows(1)*2)
    def test_boolean_offset_fails(self):
        with self.assertRaises(ContractError): fingerprint_records([dict(_kafka_topic='t',_kafka_partition=0,_kafka_offset=True)])
    def test_staging_mismatch(self):
        f=Fake(); f.bad_stage=True
        with self.assertRaises(ContractError): OfflineAdapter(self.ledger,f).execute('run1',rows(),SQL)
        self.assertNotIn('merge',f.log)
    def test_failed_merge_blocks_retry(self):
        f=Fake(); f.fail=True; a=OfflineAdapter(self.ledger,f)
        with self.assertRaises(RuntimeError): a.execute('run1',rows(),SQL)
        self.assertEqual(self.ledger.get('run1')['state'],'FAILED')
        with self.assertRaises(ContractError): a.execute('run1',rows(),SQL)
    def test_ambiguous_merging_blocks_retry(self):
        self.ledger.prepare('run1',fingerprint_records(rows()),2)
        self.ledger.transition('run1',('PREPARED',),'MERGING')
        with self.assertRaises(ContractError): OfflineAdapter(self.ledger,Fake()).execute('run1',rows(),SQL)
    def test_non_merge_sql_rejected(self):
        with self.assertRaises(ContractError): OfflineAdapter(self.ledger,Fake()).execute('run1',rows(),'DELETE FROM x')
    def test_invalid_run_id(self):
        with self.assertRaises(ContractError): self.ledger.prepare('../bad',fingerprint_records(rows()),2)

if __name__=='__main__': unittest.main()
