import hashlib,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
from pipelines.gcp.raw_to_bronze.cloud_sdk_boundary import stage_schema,quarantine_ddl,submit_with_journal
from pipelines.gcp.raw_to_bronze.cloud_pipeline_integration import CloudPlan
from pipelines.gcp.raw_to_bronze.remote_job_recovery import JobJournal
from pipelines.gcp.raw_to_bronze.execution_contracts import merge_sql
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import ContractError
class Fake:
 __stage14fk_offline_fake__=True
 def __init__(self):self.jobs={};self.calls=[]
 def load_table_from_json(self,rows,table,*,job_id,location,job_config):
  self.calls.append(('load',table,len(rows)));j=SimpleNamespace(job_id=job_id,state='DONE',error_result=None,result=lambda:None);self.jobs[job_id]=j;return j
 def query(self,sql,*,job_id,location,job_config):
  self.calls.append(('query',sql));j=SimpleNamespace(job_id=job_id,state='DONE',error_result=None,result=lambda:None);self.jobs[job_id]=j;return j
 def get_job(self,job_id,*,location):return self.jobs[job_id]
def make_plan():
 run='run_14fk';s=hashlib.sha256(run.encode()).hexdigest()[:20];st=f'sample-project.bronze.payments_raw_v1_staging_{s}';qt=f'sample-project.bronze.payments_raw_v1_quarantine_{s}';bt='sample-project.bronze.payments_raw_v1';r={'_ingest_run_id':run,'_kafka_topic':'topic','_kafka_partition':0,'_kafka_offset':7}
 return CloudPlan(run,'a'*64,st,qt,bt,'job_stage_fk','job_quarantine_fk','job_merge_fk',merge_sql(staging_table=st,bronze_table=bt,run_id=run),1,1,0,0,(r,),())
class Tests(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.j=JobJournal(Path(self.tmp.name)/'j.db');self.addCleanup(self.j.close);self.plan=make_plan();self.client=Fake();self.configs={x:object() for x in ('stage','quarantine','merge')}
 def run_fake(self,**kwargs):return submit_with_journal(plan=self.plan,journal=self.j,client=kwargs.get('client',self.client),configs=self.configs,location='europe-west2',timestamp=datetime.now(timezone.utc),offline_only=kwargs.get('offline_only',True))
 def test_zero_quarantine_creates_empty_table(self):
  self.run_fake();self.assertEqual([x[0] for x in self.client.calls],['load','query','query']);self.assertIn('CREATE TABLE IF NOT EXISTS',self.client.calls[1][1])
 def test_retry_does_not_resubmit(self):
  self.run_fake();self.run_fake();self.assertEqual(len(self.client.calls),3)
 def test_no_production_acceptance(self):self.assertFalse(self.run_fake()['production_accepted'])
 def test_real_client_blocked(self):
  with self.assertRaises(ContractError):self.run_fake(client=object())
 def test_live_switch_blocked(self):
  with self.assertRaises(ContractError):self.run_fake(offline_only=False)
 def test_schema_extension(self):self.assertEqual(stage_schema([{'name':'a','type':'STRING'}])[-1]['name'],'_ingest_run_id')
 def test_schema_duplicate_blocked(self):
  with self.assertRaises(ContractError):stage_schema([{'name':'a'},{'name':'a'}])
 def test_quarantine_ddl_scoped(self):self.assertIn(self.plan.quarantine_table,quarantine_ddl(self.plan))
