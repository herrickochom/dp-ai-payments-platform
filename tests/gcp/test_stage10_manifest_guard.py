import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] /
                       'orchestration/gcp_composer/runtime'))
from recovery_contracts import JobIdentity, UnsafeToDeploy
from manifest_guard import validate_manifest

class ManifestGuardTests(unittest.TestCase):
    def setUp(self):
        self.identity=JobIdentity('a'*64,1,'project-x','europe-west2','pdm-test','b'*64)
        self.job={'id':'job-123','name':'pdm-test','project':'project-x',
                  'region':'europe-west2','spec_digest':'b'*64,'state':'JOB_STATE_DONE'}
        self.manifest={'execution_id':'a'*64,'attempt_number':1,
                       'dataflow_job_id':'job-123','spec_digest':'b'*64,
                       'bigquery_verified':True,
                       'output_objects':[{'uri':'gs://bucket/out/part-1','sha256':'c'*64}]}
    def test_valid_independently_attested(self):
        self.assertRegex(validate_manifest(self.identity,self.job,self.manifest,
                         attestation_verifier=lambda m: True)['evidence_digest'],r'^[0-9a-f]{64}$')
    def test_no_attestation(self):
        with self.assertRaises(UnsafeToDeploy):
            validate_manifest(self.identity,self.job,self.manifest,
                              attestation_verifier=lambda m: False)
    def test_wrong_execution(self):
        self.manifest['execution_id']='d'*64
        with self.assertRaises(UnsafeToDeploy):
            validate_manifest(self.identity,self.job,self.manifest,
                              attestation_verifier=lambda m: True)
    def test_no_bigquery_proof(self):
        self.manifest['bigquery_verified']=False
        with self.assertRaises(UnsafeToDeploy):
            validate_manifest(self.identity,self.job,self.manifest,
                              attestation_verifier=lambda m: True)
    def test_duplicate_output(self):
        self.manifest['output_objects']*=2
        with self.assertRaises(UnsafeToDeploy):
            validate_manifest(self.identity,self.job,self.manifest,
                              attestation_verifier=lambda m: True)
    def test_not_done(self):
        self.job['state']='JOB_STATE_RUNNING'
        with self.assertRaises(UnsafeToDeploy):
            validate_manifest(self.identity,self.job,self.manifest,
                              attestation_verifier=lambda m: True)
