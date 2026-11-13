import unittest
from pipelines.gcp.raw_to_bronze.job_recovery import plan,recovery_decision
from pipelines.gcp.raw_to_bronze.execution_contracts import merge_sql
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import ContractError
ST='sample-project.bronze.payments_raw_v1_staging'
BR='sample-project.bronze.payments_raw_v1'
ROWS=[{'_kafka_topic':'synthetic','_kafka_partition':0,'_kafka_offset':1}]
class Tests(unittest.TestCase):
 def make(self,run='r1',rows=None):
  return plan(run_id=run,rows=ROWS if rows is None else rows,staging_table=ST,bronze_table=BR,
      merge_sql=merge_sql(staging_table=ST,bronze_table=BR,run_id=run))
 def test_deterministic_jobs(self):
  self.assertEqual(self.make(),self.make())
 def test_distinct_operations(self):
  p=self.make(); self.assertNotEqual(p.stage_job_id,p.merge_job_id)
 def test_changed_input_changes_stage_job(self):
  p=self.make(); q=self.make(rows=[{'_kafka_topic':'synthetic','_kafka_partition':0,'_kafka_offset':2}]); self.assertNotEqual(p.stage_job_id,q.stage_job_id)
 def test_sql_scope_mismatch_rejected(self):
  with self.assertRaises(ContractError): plan(run_id='r1',rows=ROWS,staging_table=ST,bronze_table=BR,merge_sql=merge_sql(staging_table=ST,bronze_table=BR,run_id='r2'))
 def test_no_empty_rows(self):
  with self.assertRaises(ContractError): self.make(rows=[])
 def test_complete_evidence(self):
  self.assertEqual(recovery_decision(stage_state='DONE',merge_state='DONE',stage_rows=2,expected_rows=2,bronze_before=5,bronze_after=6,inserted_rows=1),'EVIDENCE_CONSISTENT_NOT_LIVE_ACCEPTED')
 def test_ambiguous_remote_job_rejected(self):
  for state in ('NOT_FOUND','PENDING','RUNNING','FAILED','UNKNOWN'):
   with self.subTest(state=state),self.assertRaises(ContractError): recovery_decision(stage_state='DONE',merge_state=state,stage_rows=1,expected_rows=1,bronze_before=0,bronze_after=1,inserted_rows=1)
 def test_incorrect_count_rejected(self):
  with self.assertRaises(ContractError): recovery_decision(stage_state='DONE',merge_state='DONE',stage_rows=2,expected_rows=1,bronze_before=0,bronze_after=1,inserted_rows=1)
 def test_delta_mismatch_rejected(self):
  with self.assertRaises(ContractError): recovery_decision(stage_state='DONE',merge_state='DONE',stage_rows=1,expected_rows=1,bronze_before=0,bronze_after=1,inserted_rows=0)
 def test_boolean_count_rejected(self):
  with self.assertRaises(ContractError): recovery_decision(stage_state='DONE',merge_state='DONE',stage_rows=True,expected_rows=1,bronze_before=0,bronze_after=1,inserted_rows=1)
