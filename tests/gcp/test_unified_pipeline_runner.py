import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from fastavro import parse_schema, writer
from pipelines.gcp.raw_to_bronze.unified_pipeline_runner import execute_local,InMemoryBronzeSink
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import ContractError
SCHEMA=parse_schema({'type':'record','name':'Event','fields':[
 {'name':'event_id','type':'string'}, {'name':'timestamp','type':'string'},
 {'name':'source_system','type':'string'}, {'name':'message_type','type':'string'},
 {'name':'payload','type':{'type':'map','values':'string'}},
 {'name':'_kafka_metadata','type':{'type':'map','values':'string'}}]})
class Tests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(); self.root=Path(self.tmp.name)
  self.source=self.root/'a.avro'
  record={'event_id':'e1','timestamp':'2026-11-03T00:00:00Z','source_system':'synthetic',
    'message_type':'payment','payload':{'currency':'GBP'},
    '_kafka_metadata':{'topic':'synthetic.topic','partition':'0','offset':'1',
                       'timestamp':'2026-11-03T00:00:00Z'}}
  with self.source.open('wb') as f: writer(f,SCHEMA,[record])
  self.sink=InMemoryBronzeSink()
 def tearDown(self): self.tmp.cleanup()
 def manifest(self):
  return [{'path':str(self.source),'sha256':hashlib.sha256(self.source.read_bytes()).hexdigest()}]
 def execute(self,manifest=None,run_id='run_1',evidence='evidence.json'):
  return execute_local(manifest=self.manifest() if manifest is None else manifest,
   ledger_path=self.root/'ledger.sqlite',run_id=run_id,
   staging_table='sample-project.bronze.payments_raw_v1_staging',
   bronze_table='sample-project.bronze.payments_raw_v1',
   sink=self.sink,evidence_path=self.root/evidence)
 def test_full_local_workflow(self):
  result=self.execute()
  self.assertEqual(result['status'],'COMPLETE')
  self.assertEqual(result['reconciliation']['inserted_records'],1)
  self.assertEqual(result['cloud_writes'],0)
 def test_evidence_persisted(self):
  self.execute()
  self.assertEqual(json.loads((self.root/'evidence.json').read_text())['ledger_state'],'COMPLETE')
 def test_manifest_digest_guard(self):
  with self.assertRaises(ContractError): self.execute([{'path':str(self.source),'sha256':'0'*64}])
 def test_no_manifest_guard(self):
  with self.assertRaises(ContractError): self.execute([])
 def test_evidence_no_overwrite(self):
  self.execute()
  with self.assertRaises(ContractError): self.execute(evidence='evidence.json')
 def test_persisted_retry(self):
  self.execute()
  result=self.execute(evidence='retry.json')
  self.assertEqual(result['status'],'ALREADY_COMPLETE')
 def test_invalid_run_fails(self):
  with self.assertRaises(ContractError): self.execute(run_id='invalid run')
