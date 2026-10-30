"""Offline 9F-I tests. No cloud SDK or PostgreSQL required."""
import sys
from pathlib import Path
import unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'orchestration/gcp_composer/runtime'))
from stage9fghi_contracts import JobIdentity,classify_jobs,verified_evidence,UnsafeToDeploy,deployment_gate,orchestration_tick

E='a'*64
D='b'*64
class Contracts(unittest.TestCase):
    def setUp(self):
        self.i=JobIdentity(E,1,'example-project','europe-west2','pdm-test-abc',D)
        self.j={'id':'job-123','name':self.i.job_name,'project':self.i.project,'region':self.i.region,'spec_digest':D,'state':'JOB_STATE_DONE'}
        self.e={'execution_id':E,'attempt_number':1,'dataflow_job_id':'job-123','spec_digest':D,'bigquery_verified':True,'manifest_verified':True,'output_objects':[{'uri':'gs://bucket/result','sha256':'c'*64}]}
    def test_valid_identity(self): self.assertEqual(self.i.validate(),self.i)
    def test_invalid_attempt(self):
        with self.assertRaises(ValueError): JobIdentity(E,0,'p','r','pdm-test-abc',D).validate()
    def test_unknown_never_means_submit(self): self.assertEqual(classify_jobs(self.i,[])[0],'UNKNOWN')
    def test_provider_timeout_visibility_unknown(self): self.assertEqual(classify_jobs(self.i,None)[0],'UNKNOWN')
    def test_multiple_jobs_quarantine(self): self.assertEqual(classify_jobs(self.i,[self.j,self.j])[0],'QUARANTINE')
    def test_job_mismatch_quarantine(self): self.assertEqual(classify_jobs(self.i,[{**self.j,'region':'wrong'}])[0],'QUARANTINE')
    def test_one_matching_job(self): self.assertEqual(classify_jobs(self.i,[self.j])[0],'FOUND')
    def test_verified_evidence_digest(self): self.assertEqual(len(verified_evidence(self.i,self.j,self.e)['evidence_digest']),64)
    def test_wrong_execution_rejected(self):
        with self.assertRaises(UnsafeToDeploy): verified_evidence(self.i,self.j,{**self.e,'execution_id':'x'*64})
    def test_no_bigquery_evidence_rejected(self):
        with self.assertRaises(UnsafeToDeploy): verified_evidence(self.i,self.j,{**self.e,'bigquery_verified':False})
    def test_no_output_rejected(self):
        with self.assertRaises(UnsafeToDeploy): verified_evidence(self.i,self.j,{**self.e,'output_objects':[]})
    def test_not_done_rejected(self):
        with self.assertRaises(UnsafeToDeploy): verified_evidence(self.i,{**self.j,'state':'JOB_STATE_RUNNING'},self.e)
    def test_gate_fails_closed(self): self.assertFalse(deployment_gate({})['ready'])
    def test_gate_needs_explicit_approval(self):
        keys=deployment_gate({})['missing']
        self.assertIn('explicit_gcp_approval',keys)
    def test_no_remote_submission_code(self):
        source=(ROOT/'orchestration/gcp_composer/runtime/stage9fghi_contracts.py').read_text()
        self.assertNotIn('jobs.create(',source)
        self.assertNotIn('google.cloud',source)
    def test_existing_migrations_untouched(self):
        for n in ('007_gcp_payment_execution_ledger.sql','008_gcp_dataflow_submission_outbox.sql','009_gcp_stage9_recovery_leases.sql'):
            self.assertTrue((ROOT/'orchestration/gcp_composer/migrations'/n).exists())
if __name__=='__main__': unittest.main()
