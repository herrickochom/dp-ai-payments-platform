import hashlib
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
from pipelines.gcp.raw_to_bronze.dependency_injected_executor import ExecutionPermit,execute_offline_fake,verify_offline_fake
from pipelines.gcp.raw_to_bronze.remote_job_recovery import JobJournal
from pipelines.gcp.raw_to_bronze.cloud_pipeline_integration import CloudPlan
from pipelines.gcp.raw_to_bronze.execution_contracts import merge_sql
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import ContractError

class FakeClient:
 __stage14fi_offline_fake__=True
 def __init__(self):self.jobs={};self.calls=[];self.fail=False
 def load_table_from_json(self,rows,table,*,job_id,location,job_config):
  self.calls.append(('load',table,job_id,location,len(rows)))
  if self.fail:raise TimeoutError('ambiguous submission')
  job=SimpleNamespace(job_id=job_id,state='DONE',error_result=None,result=lambda:None)
  self.jobs[job_id]=job
  return job
 def query(self,sql,*,job_id,location,job_config):
  self.calls.append(('merge',job_id,location))
  job=SimpleNamespace(job_id=job_id,state='DONE',error_result=None,result=lambda:None)
  self.jobs[job_id]=job
  return job
 def get_job(self,job_id,*,location):
  self.calls.append(('observe',job_id,location))
  return self.jobs[job_id]

class Tests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.j=JobJournal(Path(self.temp.name)/'journal.db')
  run='run_14fi';suffix=hashlib.sha256(run.encode()).hexdigest()[:20]
  st=f'sample-project.bronze.payments_raw_v1_staging_{suffix}'
  qt=f'sample-project.bronze.payments_raw_v1_quarantine_{suffix}'
  bronze='sample-project.bronze.payments_raw_v1'
  row={'_ingest_run_id':run,'_kafka_topic':'topic','_kafka_partition':0,'_kafka_offset':7}
  q={'event_id':'bad','source_path':'gs://x/bad','reason':'invalid','record_json':'{}','_pipeline_version':'stage14fg'}
  self.plan=CloudPlan(run,'a'*64,st,qt,bronze,'pdm_stage_1','pdm_stage_2','pdm_merge_3',
    merge_sql(staging_table=st,bronze_table=bronze,run_id=run),2,1,1,0,(row,),(q,))
  self.permit=ExecutionPermit(run,bronze,'europe-west2','OFFLINE_FAKE_CLIENT')
  self.client=FakeClient();self.cfg={'stage':object(),'quarantine':object(),'merge':object()}
  self.kw=dict(plan=self.plan,journal=self.j,client=self.client,permit=self.permit,
               configs=self.cfg,timestamp=datetime(2026,11,6,tzinfo=timezone.utc))
 def tearDown(self):self.j.close();self.temp.cleanup()
 def test_order_and_isolated_targets(self):
  result=execute_offline_fake(**self.kw)
  self.assertEqual(result['status'],'REMOTE_JOBS_DONE_RECONCILIATION_REQUIRED')
  calls=[x for x in self.client.calls if x[0] in ('load','merge')]
  self.assertEqual([x[0] for x in calls],['load','load','merge'])
  self.assertEqual(calls[0][1],self.plan.staging_table)
  self.assertEqual(calls[1][1],self.plan.quarantine_table)
 def test_retry_does_not_resubmit(self):
  execute_offline_fake(**self.kw)
  execute_offline_fake(**self.kw)
  self.assertEqual(len([x for x in self.client.calls if x[0] in ('load','merge')]),3)
 def test_durable_resume(self):
  execute_offline_fake(**self.kw);self.j.close()
  self.j=JobJournal(Path(self.temp.name)/'journal.db');self.kw['journal']=self.j
  execute_offline_fake(**self.kw)
  self.assertEqual(len([x for x in self.client.calls if x[0] in ('load','merge')]),3)
 def test_no_permit(self):
  with self.assertRaisesRegex(ContractError,'LIVE_BLOCKED'):execute_offline_fake(**dict(self.kw,permit=None))
 def test_real_client_without_marker_blocked(self):
  with self.assertRaisesRegex(ContractError,'LIVE_BLOCKED'):execute_offline_fake(**dict(self.kw,client=object()))
 def test_ambiguous_submission_blocks_retry(self):
  self.client.fail=True
  with self.assertRaisesRegex(ContractError,'ambiguous'):execute_offline_fake(**self.kw)
  self.assertEqual(self.j.get(self.plan.run_id,'stage')['state'],'SUBMITTED')
  with self.assertRaisesRegex(ContractError,'not conclusively DONE'):execute_offline_fake(**self.kw)
 def test_location_scope(self):
  from dataclasses import replace
  with self.assertRaises(ContractError):execute_offline_fake(**dict(self.kw,permit=replace(self.permit,location='us')))
 def test_reconciliation_required(self):
  execute_offline_fake(**self.kw)
  self.assertEqual(self.j.readiness(self.plan.run_id),'REMOTE_JOBS_DONE_RECONCILIATION_REQUIRED')
  obs=dict(plan=self.plan,journal=self.j,client=self.client,permit=self.permit,
           stage_count=1,quarantine_count=1,bronze_identities=[('topic',0,7)],quarantine_paths=['gs://x/bad'])
  self.assertEqual(verify_offline_fake(**obs)['bronze_identities_verified'],1)
  with self.assertRaises(ContractError):verify_offline_fake(**dict(obs,bronze_identities=[('topic',0,8)]))
 def test_no_reconciliation_before_jobs(self):
  with self.assertRaises(ContractError):verify_offline_fake(plan=self.plan,journal=self.j,client=self.client,
    permit=self.permit,stage_count=1,quarantine_count=1,bronze_identities=[],quarantine_paths=[])
 def test_no_quarantine_blocks(self):
  from dataclasses import replace
  p=replace(self.plan,input_records=1,quarantined_records=0,quarantine_rows=())
  with self.assertRaisesRegex(ContractError,'zero-quarantine'):execute_offline_fake(**dict(self.kw,plan=p))
