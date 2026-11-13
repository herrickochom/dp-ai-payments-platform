import unittest
from pipelines.gcp.raw_to_bronze.remote_job_observation import (
    SourceVersion,fetch_pinned,observe_job,observe_count,accept_observed)
from pipelines.gcp.raw_to_bronze.cloud_service_adapters import InjectedGCSReader
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import ContractError

class Blob:
    def __init__(self, name, generation): self.name,self.generation=name,generation
    def download_as_bytes(self): return b'avro'
class Bucket:
    def blob(self,name,generation): return Blob(name,generation)
class GCS:
    def bucket(self,name): return Bucket()
class Job:
    job_id='pdm_merge_abc'
    state='DONE'
    error_result=None
    errors=None
class BQ:
    def __init__(self): self.calls=0
    def get_job(self,jid,location):
        self.calls+=1
        return Job()
class Tests(unittest.TestCase):
    def test_pinned_gcs_object(self):
        result=fetch_pinned(InjectedGCSReader(GCS(),'dp-pdm-ai-platform-dev'),
            [SourceVersion('raw/v1/a.avro',2,4)])
        self.assertEqual(result,[('raw/v1/a.avro',b'avro')])
    def test_source_size_mismatch_blocks(self):
        with self.assertRaises(ContractError):
            fetch_pinned(InjectedGCSReader(GCS(),'dp-pdm-ai-platform-dev'),
                [SourceVersion('raw/v1/a.avro',2,5)])
    def test_source_duplicate_blocks(self):
        with self.assertRaises(ContractError):
            fetch_pinned(InjectedGCSReader(GCS(),'dp-pdm-ai-platform-dev'),
                [SourceVersion('raw/v1/a.avro',2,4)]*2)
    def test_job_observation_only(self):
        client=BQ()
        self.assertEqual(observe_job(client,job_id='pdm_merge_abc',location='europe-west2'),'DONE')
        self.assertEqual(client.calls,1)
    def test_unknown_job_blocks(self):
        class Unknown(BQ):
            def get_job(self,jid,location): raise LookupError('not found')
        with self.assertRaises(ContractError):
            observe_job(Unknown(),job_id='pdm_merge_abc',location='europe-west2')
    def test_run_scoped_count_sql(self):
        self.assertIn("WHERE `_ingest_run_id` = 'r1'",
            observe_count(None,table='sample-project.bronze.stage',run_id='r1',job_config=object()))
    def test_count_rejects_unsafe_run(self):
        with self.assertRaises(ContractError):
            observe_count(None,table='sample-project.bronze.stage',run_id="r1';DROP",job_config=object())
    def test_acceptance_requires_remote_evidence(self):
        self.assertEqual(accept_observed(stage_state='DONE',merge_state='DONE',
            stage_rows=2,expected_rows=2,bronze_before=3,bronze_after=4,inserted_rows=1),
            'EVIDENCE_CONSISTENT_NOT_LIVE_ACCEPTED')
