import sys,unittest
from pathlib import Path
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'orchestration/gcp_composer/runtime'))
from dataflow_provider import ReadOnlyDataflowProvider,assess_visibility,DataflowVisibilityError
from bigquery_operations import BigQueryEvidenceVerifier
from recovery_contracts import JobIdentity,UnsafeToDeploy
E='a'*64; D='b'*64
I=JobIdentity(E,1,'example-project','europe-west2','pdm-test-abc',D)
class FakeDataflow:
    def __init__(self,jobs=None,error=False):self.jobs=jobs or [];self.error=error
    def list_jobs(self,request):
        if self.error:raise RuntimeError('provider down')
        return SimpleNamespace(jobs=self.jobs)
class FakeBQ:
    def get_table(self,name):return SimpleNamespace(num_rows=10)
class TestStage10(unittest.TestCase):
    def test_missing_job_not_permission_to_submit(self):
        p=ReadOnlyDataflowProvider(FakeDataflow(),project=I.project,region=I.region)
        self.assertEqual(assess_visibility(p,I)[0],'UNKNOWN')
    def test_provider_failure_unknown(self):
        p=ReadOnlyDataflowProvider(FakeDataflow(error=True),project=I.project,region=I.region)
        self.assertIsNone(p.list_matching(I))
    def test_scope_fenced(self):
        p=ReadOnlyDataflowProvider(FakeDataflow(),project=I.project,region=I.region)
        with self.assertRaises(DataflowVisibilityError):p.find_by_name('other',I.region,I.job_name)
    def test_multiple_jobs_quarantine(self):
        j=SimpleNamespace(name=I.job_name,id='job-1',labels={'pdm_spec_digest':D},current_state='JOB_STATE_DONE')
        p=ReadOnlyDataflowProvider(FakeDataflow([j,j]),project=I.project,region=I.region)
        self.assertEqual(assess_visibility(p,I)[0],'QUARANTINE')
    def test_bigquery_metadata_not_completion_proof(self):
        v=BigQueryEvidenceVerifier(FakeBQ(),project=I.project,dataset='pdm_payments_dev',table='payment_events')
        manifest=dict(execution_id=E,attempt_number=1,dataflow_job_id='job-1',spec_digest=D,manifest_verified=True)
        self.assertFalse(v.inspect_outputs(I,'job-1',manifest=manifest)['bigquery_verified'])
        self.assertIsNone(v.verify({},{}))
    def test_bigquery_missing_manifest_fails_closed(self):
        v=BigQueryEvidenceVerifier(FakeBQ(),project=I.project,dataset='pdm_payments_dev',table='payment_events')
        with self.assertRaises(UnsafeToDeploy):v.inspect_outputs(I,'job-1',manifest={})
    def test_no_submission_in_adapter(self):
        source=(ROOT/'orchestration/gcp_composer/runtime/dataflow_provider.py').read_text()
        self.assertNotIn('create_job(',source)
        self.assertNotIn('launch_template(',source)
if __name__=='__main__':unittest.main()
