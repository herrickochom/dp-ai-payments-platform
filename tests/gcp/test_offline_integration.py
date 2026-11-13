import tempfile
import unittest
from pathlib import Path
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import DurableLedger, ContractError
from pipelines.gcp.raw_to_bronze.offline_integration import execute_offline

STAGE='proj-dev.bronze.payments_raw_v1_staging'
BRONZE='proj-dev.bronze.payments_raw_v1'
def rows(n=2):
    return [dict(_kafka_topic='test', _kafka_partition=0, _kafka_offset=i) for i in range(n)]
class Fake:
    def __init__(self): self.events=[]; self.staged=0; self.bronze=0; self.fail=False
    def count_bronze(self): self.events.append('count_bronze'); return self.bronze
    def stage(self,run_id,records): self.events.append('stage'); self.staged=len(records)
    def count_staging(self,run_id): self.events.append('count_staging'); return self.staged
    def merge(self,sql):
        self.events.append('merge'); assert sql.startswith('MERGE `proj-dev.bronze.payments_raw_v1`')
        if self.fail: raise RuntimeError('injected merge failure')
        self.bronze+=self.staged
class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.ledger=DurableLedger(Path(self.tmp.name)/'ledger.sqlite3'); self.fake=Fake()
    def tearDown(self): self.ledger.close(); self.tmp.cleanup()
    def run_case(self, **kw):
        args=dict(ledger=self.ledger,fake_client=self.fake,run_id='run_14fa',staging_rows=rows(),
                  staging_table=STAGE,bronze_table=BRONZE,input_records=4,quarantined_records=1,duplicate_records=1)
        args.update(kw); return execute_offline(**args)
    def test_integrated_sql_and_reconciliation(self):
        result=self.run_case(); self.assertEqual(result['status'],'COMPLETE')
        self.assertEqual(result['reconciliation']['inserted_records'],2)
        self.assertIn("WHERE S.`_ingest_run_id` = 'run_14fa'",result['merge_sql'])
    def test_execution_order(self):
        self.run_case(); self.assertEqual(self.fake.events,['count_bronze','stage','count_staging','merge','count_bronze'])
    def test_retry_no_more_sink_calls(self):
        self.run_case(); self.assertEqual(self.run_case()['status'],'ALREADY_COMPLETE'); self.assertEqual(len(self.fake.events),5)
    def test_accounting_mismatch_fails_before_ledger(self):
        with self.assertRaises(ContractError): self.run_case(input_records=5)
        self.assertIsNone(self.ledger.get('run_14fa'))
    def test_duplicate_staging_identity_fails_before_ledger(self):
        with self.assertRaises(ContractError): self.run_case(staging_rows=rows(1)*2)
        self.assertIsNone(self.ledger.get('run_14fa'))
    def test_invalid_target_fails_before_ledger(self):
        with self.assertRaises(ValueError): self.run_case(bronze_table=STAGE)
        self.assertIsNone(self.ledger.get('run_14fa'))
    def test_ambiguous_merge_blocks_retry(self):
        self.fake.fail=True
        with self.assertRaises(RuntimeError): self.run_case()
        self.fake.fail=False
        with self.assertRaises(ContractError): self.run_case()
        self.assertEqual(self.ledger.get('run_14fa')['state'],'FAILED')
    def test_no_client_rejected(self):
        with self.assertRaises(ContractError): self.run_case(fake_client=None)
        self.assertIsNone(self.ledger.get('run_14fa'))
if __name__=='__main__': unittest.main()
