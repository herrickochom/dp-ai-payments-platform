import unittest
from pipelines.gcp.raw_to_bronze.cloud_service_adapters import (
    AdapterContractError, InjectedGCSReader, InjectedBigQueryAdapter, job_id, table_id
)
ST='sample-project.bronze.payments_raw_v1_staging'
BR='sample-project.bronze.payments_raw_v1'
class Job:
    errors=None
    def __init__(self, rows=None): self.rows=rows or []
    def result(self): return self.rows
class Client:
    def __init__(self): self.calls=[]
    def load_table_from_json(self,rows,table,**kw):
        self.calls.append(('load',table,kw['job_id'])); return Job()
    def query(self,sql,**kw):
        self.calls.append(('query',sql,kw['job_id']))
        return Job([{'n':3}] if sql.startswith('SELECT') else [])
class Blob:
    def download_as_bytes(self): return b'Obj\x01'
class Bucket:
    def blob(self,name,**kw): return Blob()
class Storage:
    def bucket(self,name): return Bucket()
class Tests(unittest.TestCase):
    def test_deterministic_jobs(self): self.assertEqual(job_id('run1','merge','sql'),job_id('run1','merge','sql'))
    def test_distinct_job_operations(self): self.assertNotEqual(job_id('run1','merge','sql'),job_id('run1','stage','sql'))
    def test_bad_run_rejected(self):
        with self.assertRaises(AdapterContractError): job_id('bad run','merge','sql')
    def test_bad_table_rejected(self):
        with self.assertRaises(AdapterContractError): table_id('project;DROP TABLE.bronze.table')
    def test_same_target_rejected(self):
        with self.assertRaises(AdapterContractError): InjectedBigQueryAdapter(Client(),ST,ST)
    def test_read_canonical(self):
        o=InjectedGCSReader(Storage(),'dp-pdm-ai-platform-dev').read('raw/v1/2026-11-03/event.avro',1)
        self.assertEqual(o.content,b'Obj\x01')
    def test_noncanonical_rejected(self):
        with self.assertRaises(AdapterContractError): InjectedGCSReader(Storage(),'dp-pdm-ai-platform-dev').read('raw/unapproved/a.avro',1)
    def test_generation_bool_rejected(self):
        with self.assertRaises(AdapterContractError): InjectedGCSReader(Storage(),'dp-pdm-ai-platform-dev').read('raw/v1/a.avro',True)
    def test_stage_and_merge_order(self):
        c=Client(); a=InjectedBigQueryAdapter(c,ST,BR)
        a.stage('run1','fingerprint',[{'id':1}],job_config=object())
        a.merge('run1','MERGE `x` USING `y` ON TRUE',job_config=object())
        self.assertEqual([x[0] for x in c.calls],['load','query'])
    def test_reconcile_count(self):
        self.assertEqual(InjectedBigQueryAdapter(Client(),ST,BR).count('run1',BR,job_config=object()),3)
    def test_unapproved_count_rejected(self):
        with self.assertRaises(AdapterContractError): InjectedBigQueryAdapter(Client(),ST,BR).count('run1','sample-project.other.x',job_config=object())
    def test_merge_non_sql_rejected(self):
        with self.assertRaises(AdapterContractError): InjectedBigQueryAdapter(Client(),ST,BR).merge('run1','DELETE FROM x',job_config=object())
