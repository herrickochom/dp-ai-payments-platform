import unittest
from types import SimpleNamespace
from pipelines.gcp.raw_to_bronze.remote_job_reconciliation import query_plan,execute_offline_reconciliation
from pipelines.gcp.raw_to_bronze.cloud_pipeline_integration import CloudPlan
from pipelines.gcp.raw_to_bronze.execution_contracts import merge_sql
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import ContractError

class FakeQuery:
 __stage14fj_offline_fake__=True
 def __init__(self,stage=1,paths=(),identities=(('topic',0,1),)):
  self.stage=stage;self.paths=paths;self.identities=identities;self.calls=[]
 def query(self,sql,*,job_config,location):
  self.calls.append((sql,location,job_config))
  if 'COUNT(*) AS n' in sql: rows=[{'n':self.stage}]
  elif 'SELECT source_path' in sql: rows=[{'source_path':x} for x in self.paths]
  else: rows=[dict(zip(('_kafka_topic','_kafka_partition','_kafka_offset'),x)) for x in self.identities]
  return SimpleNamespace(result=lambda:rows)

def plan():
 run='stage14fj_test'
 st='proj123.bronze.payments_raw_v1_staging_8a85e8e8e8e8e8e8e8e8'
 # validate_plan requires exact run suffix
 import hashlib
 suf=hashlib.sha256(run.encode()).hexdigest()[:20]
 st=f'proj123.bronze.payments_raw_v1_staging_{suf}'
 qt=f'proj123.bronze.payments_raw_v1_quarantine_{suf}'
 bt='proj123.bronze.payments_raw_v1'
 row={'_kafka_topic':'topic','_kafka_partition':0,'_kafka_offset':1,'_ingest_run_id':run}
 sql=merge_sql(staging_table=st,bronze_table=bt,run_id=run)
 return CloudPlan(run,'a'*64,st,qt,bt,'job_stage','job_quarantine','job_merge',sql,1,1,0,0,(row,),())

class Tests(unittest.TestCase):
 def configs(self):return {'stage':object(),'quarantine':object(),'bronze':object()}
 def test_actual_three_query_calls(self):
  client=FakeQuery();r=execute_offline_reconciliation(plan=plan(),client=client,configs=self.configs())
  self.assertEqual(len(client.calls),3);self.assertFalse(r['production_accepted'])
 def test_zero_quarantine_succeeds(self):
  r=execute_offline_reconciliation(plan=plan(),client=FakeQuery(paths=()),configs=self.configs())
  self.assertEqual(r['quarantine_verified'],0)
 def test_wrong_stage_count_rejected(self):
  with self.assertRaises(ContractError):execute_offline_reconciliation(plan=plan(),client=FakeQuery(stage=2),configs=self.configs())
 def test_missing_bronze_rejected(self):
  with self.assertRaises(ContractError):execute_offline_reconciliation(plan=plan(),client=FakeQuery(identities=()),configs=self.configs())
 def test_duplicate_bronze_rejected(self):
  with self.assertRaises(ContractError):execute_offline_reconciliation(plan=plan(),client=FakeQuery(identities=(('topic',0,1),('topic',0,1))),configs=self.configs())
 def test_real_client_refused(self):
  with self.assertRaises(ContractError):execute_offline_reconciliation(plan=plan(),client=object(),configs=self.configs())
 def test_wrong_location_refused(self):
  with self.assertRaises(ContractError):execute_offline_reconciliation(plan=plan(),client=FakeQuery(),configs=self.configs(),location='US')
 def test_config_missing_refused(self):
  with self.assertRaises(ContractError):execute_offline_reconciliation(plan=plan(),client=FakeQuery(),configs={'stage':object()})
 def test_no_global_bronze_count(self):
  sql=query_plan(plan());self.assertNotIn('COUNT(*)',sql['bronze']);self.assertIn('JOIN expected',sql['bronze'])
 def test_read_only_sql(self):
  for sql in query_plan(plan()).values():
   self.assertTrue(sql.startswith(('SELECT','WITH')))
   self.assertNotIn('MERGE INTO',sql)
