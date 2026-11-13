import sqlite3
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime,timezone
from pathlib import Path
from types import SimpleNamespace
from pipelines.gcp.raw_to_bronze.remote_job_recovery import JobJournal,quarantine_payload,reconcile_evidence,execution_gate
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import ContractError
from pipelines.gcp.raw_to_bronze.cloud_pipeline_integration import CloudPlan
from pipelines.gcp.raw_to_bronze.execution_contracts import merge_sql

class Client:
 def __init__(self,state='DONE',error=None): self.state=state;self.error=error;self.calls=[]
 def get_job(self,job_id,location):
  self.calls.append((job_id,location))
  return SimpleNamespace(state=self.state,error_result=self.error)

class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory()
  self.path=Path(self.tmp.name)/'jobs.sqlite'
  self.j=JobJournal(self.path)
  self.run='run_14fh'
  stage='sample-project.bronze.payments_raw_v1_staging_4cdb8ac5'
  quar='sample-project.bronze.payments_raw_v1_quarantine_4cdb8ac5'
  # Match the actual deterministic suffix used by 14F-G.
  import hashlib
  suffix=hashlib.sha256(self.run.encode()).hexdigest()[:20]
  stage='sample-project.bronze.payments_raw_v1_staging_'+suffix
  quar='sample-project.bronze.payments_raw_v1_quarantine_'+suffix
  bronze='sample-project.bronze.payments_raw_v1'
  row={'_ingest_run_id':self.run,'_kafka_topic':'t','_kafka_partition':0,'_kafka_offset':2}
  q={'event_id':'bad','source_path':'gs://source/bad','reason':'invalid','record_json':'{}','_pipeline_version':'stage14fg'}
  self.plan=CloudPlan(self.run,'a'*64,stage,quar,bronze,'pdm_stage_1','pdm_stage_2','pdm_merge_3',
                      merge_sql(staging_table=stage,bronze_table=bronze,run_id=self.run),2,1,1,0,(row,),(q,))
 def tearDown(self): self.j.close();self.tmp.cleanup()
 def test_registration_persists_and_pins_location(self):
  self.j.register(self.plan,location='europe-west2');self.j.close()
  self.j=JobJournal(self.path)
  self.assertEqual(self.j.get(self.run,'merge')['location'],'europe-west2')
 def test_plan_drift_rejected(self):
  self.j.register(self.plan,location='europe-west2')
  with self.assertRaises(ContractError):self.j.register(replace(self.plan,source_digest='b'*64),location='europe-west2')
 def test_location_drift_rejected(self):
  self.j.register(self.plan,location='europe-west2')
  with self.assertRaises(ContractError):self.j.register(self.plan,location='us')
 def test_pre_submit_observation_blocked(self):
  self.j.register(self.plan,location='europe-west2')
  with self.assertRaises(ContractError):self.j.observe(self.run,'merge',client=Client())
 def test_submitted_unknown_blocks(self):
  self.j.register(self.plan,location='europe-west2');self.j.mark_submitted(self.run,'merge')
  self.assertEqual(self.j.observe(self.run,'merge',client=Client('UNKNOWN')),'UNKNOWN')
  self.assertEqual(self.j.readiness(self.run),'BLOCKED_REMOTE_JOB_UNCERTAINTY')
  with self.assertRaises(ContractError):self.j.mark_submitted(self.run,'merge')
 def test_job_error_not_success(self):
  self.j.register(self.plan,location='europe-west2');self.j.mark_submitted(self.run,'merge')
  self.assertEqual(self.j.observe(self.run,'merge',client=Client('DONE',{'reason':'bad'})),'FAILED')
 def test_all_jobs_done_still_requires_reconciliation(self):
  self.j.register(self.plan,location='europe-west2')
  for op in ('stage','quarantine','merge'):
   self.j.mark_submitted(self.run,op)
   self.assertEqual(self.j.observe(self.run,op,client=Client()),'DONE')
  self.assertEqual(self.j.readiness(self.run),'REMOTE_JOBS_DONE_RECONCILIATION_REQUIRED')
 def test_quarantine_schema_timestamp(self):
  rows=quarantine_payload(self.plan,timestamp=datetime(2026,11,6,tzinfo=timezone.utc))
  self.assertEqual(len(rows),1)
  self.assertEqual(set(rows[0]),{'event_id','source_path','reason','record_json','_quarantined_at','_pipeline_version'})
  with self.assertRaises(ContractError):quarantine_payload(self.plan,timestamp=datetime(2026,11,6))
 def test_independent_evidence(self):
  kw={'plan':self.plan,'observed_stage_count':1,'observed_quarantine_count':1,
      'observed_bronze_identities':[('t',0,2)],'observed_quarantine_source_paths':['gs://source/bad']}
  self.assertEqual(reconcile_evidence(**kw)['bronze_identities_verified'],1)
  with self.assertRaises(ContractError):reconcile_evidence(**dict(kw,observed_bronze_identities=[('t',0,3)]))
  with self.assertRaises(ContractError):reconcile_evidence(**dict(kw,observed_quarantine_count=0))
 def test_live_blocked(self):
  with self.assertRaisesRegex(ContractError,'LIVE_BLOCKED'):execution_gate(approval='YES')
