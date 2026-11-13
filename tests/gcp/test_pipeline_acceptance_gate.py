import unittest,tempfile
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime,timezone
from pipelines.gcp.raw_to_bronze.pipeline_acceptance_gate import accept_offline,production_acceptance
from pipelines.gcp.raw_to_bronze.cloud_sdk_boundary import submit_with_journal
from pipelines.gcp.raw_to_bronze.remote_job_recovery import JobJournal
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import ContractError
from tests.gcp.test_cloud_sdk_boundary import Fake,make_plan
class QueryFake:
 __stage14fj_offline_fake__=True
 def query(self,sql,*,job_config,location):
  if 'COUNT(*) AS n' in sql:rows=[{'n':1}]
  elif 'SELECT source_path' in sql:rows=[]
  else:rows=[{'_kafka_topic':'topic','_kafka_partition':0,'_kafka_offset':7}]
  return SimpleNamespace(result=lambda:rows)
class Tests(unittest.TestCase):
 def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.j=JobJournal(Path(self.tmp.name)/'journal.db');self.addCleanup(self.j.close);self.plan=make_plan()
 def test_no_premature_acceptance(self):
  with self.assertRaises(ContractError):accept_offline(plan=self.plan,journal=self.j,query_client=QueryFake(),configs={x:object() for x in ('stage','quarantine','bronze')})
 def test_completed_offline_reconciliation(self):
  submit_with_journal(plan=self.plan,journal=self.j,client=Fake(),configs={x:object() for x in ('stage','quarantine','merge')},location='europe-west2',timestamp=datetime.now(timezone.utc))
  r=accept_offline(plan=self.plan,journal=self.j,query_client=QueryFake(),configs={x:object() for x in ('stage','quarantine','bronze')})
  self.assertEqual(r['bronze_identities_verified'],1);self.assertFalse(r['production_accepted'])
 def test_live_acceptance_denied(self):
  with self.assertRaises(ContractError):production_acceptance()
