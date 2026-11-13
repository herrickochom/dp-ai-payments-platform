import hashlib
import io
import unittest
from fastavro import parse_schema,writer
from pipelines.gcp.raw_to_bronze.cloud_pipeline_integration import PinnedSource,build_plan,validate_plan,execution_gate
from pipelines.gcp.raw_to_bronze.cloud_service_adapters import InjectedGCSReader
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import ContractError
SCHEMA=parse_schema({'type':'record','name':'Event','fields':[
 {'name':'event_id','type':'string'},{'name':'timestamp','type':'string'},
 {'name':'source_system','type':'string'},{'name':'message_type','type':'string'},
 {'name':'payload','type':{'type':'map','values':'string'}},
 {'name':'_kafka_metadata','type':{'type':'map','values':'string'}}]})
class Blob:
 def __init__(self,content): self.content=content
 def download_as_bytes(self): return self.content
class Bucket:
 def __init__(self,content): self.content=content
 def blob(self,name,generation=None): return Blob(self.content)
class Client:
 def __init__(self,content): self.content=content
 def bucket(self,name): return Bucket(self.content)
class Tests(unittest.TestCase):
 def setUp(self):
  record={'event_id':'e1','timestamp':'2026-11-03T00:00:00Z','source_system':'synthetic',
   'message_type':'payment','payload':{'currency':'GBP'},
   '_kafka_metadata':{'topic':'synthetic.topic','partition':'0','offset':'1','timestamp':'2026-11-03T00:00:00Z'}}
  b=io.BytesIO();writer(b,SCHEMA,[record]);self.data=b.getvalue()
  self.reader=InjectedGCSReader(Client(self.data),'dp-pdm-ai-platform-dev')
  self.source=PinnedSource('raw/v1/date=2026-11-03/a.avro',7,len(self.data),hashlib.sha256(self.data).hexdigest())
 def plan(self,**kwargs):
  args={'run_id':'run_14fg','manifest':[self.source],'reader':self.reader,
        'bronze_table':'sample-project.bronze.payments_raw_v1'}
  args.update(kwargs);return build_plan(**args)
 def test_pinned_source_to_plan(self):
  p=self.plan();self.assertEqual(p.staged_records,1);self.assertEqual(p.input_records,1)
  self.assertEqual(p.staging_rows[0]['_ingest_run_id'],'run_14fg')
 def test_isolated_staging(self):
  a=self.plan();b=self.plan(run_id='another_run')
  self.assertNotEqual(a.staging_table,b.staging_table)
  self.assertNotEqual(a.quarantine_table,b.quarantine_table)
 def test_merge_is_run_scoped(self):
  p=self.plan();self.assertIn("`_ingest_run_id`",p.merge_statement)
  self.assertIn("'run_14fg'",p.merge_statement)
 def test_source_checksum_guard(self):
  bad=PinnedSource(self.source.name,7,len(self.data),'0'*64)
  with self.assertRaises(ContractError):self.plan(manifest=[bad])
 def test_duplicate_source_guard(self):
  with self.assertRaises(ContractError):self.plan(manifest=[self.source,self.source])
 def test_invalid_generation_guard(self):
  with self.assertRaises(ContractError):self.plan(manifest=[PinnedSource(self.source.name,True,len(self.data),self.source.sha256)])
 def test_noncanonical_target_guard(self):
  with self.assertRaises(ContractError):self.plan(bronze_table='sample-project.gold.payments_raw_v1')
 def test_mutated_plan_guard(self):
  from dataclasses import replace
  p=self.plan()
  with self.assertRaises(ContractError):validate_plan(replace(p,staged_records=2))
 def test_no_live_bypass(self):
  with self.assertRaisesRegex(ContractError,'LIVE_BLOCKED'):execution_gate(plan=self.plan(),approval='YES')
 def test_invalid_run(self):
  with self.assertRaises(ContractError):self.plan(run_id='bad run')
