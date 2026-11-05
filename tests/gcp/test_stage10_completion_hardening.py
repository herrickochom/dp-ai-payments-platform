import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'orchestration/gcp_composer/runtime'))
from submission_guard import SubmissionIdentity,assess_submission,submit,UnsafeSubmission
from bigquery_live_operations import BigQueryLineageVerifier
I=SubmissionIdentity('a'*64,1,'project-e48f194e-f44f-458d-b4f','europe-west2','pdm-test','b'*64)
J={'id':'123','name':I.job_name,'project':I.project,'region':I.region,'spec_digest':I.spec_digest}
class Tests(unittest.TestCase):
 def test_unknown_never_submits(self): self.assertEqual(assess_submission(I,'PREPARED',None),'RECONCILE_ONLY')
 def test_absence_never_submits(self): self.assertEqual(assess_submission(I,'PREPARED',[]),'RECONCILE_ONLY')
 def test_duplicates_quarantined(self): self.assertEqual(assess_submission(I,'PREPARED',[J,J]),'QUARANTINE')
 def test_mismatch_quarantined(self): self.assertEqual(assess_submission(I,'PREPARED',[{**J,'spec_digest':'wrong'}]),'QUARANTINE')
 def test_matching_observed(self): self.assertEqual(assess_submission(I,'PREPARED',[J]),'OBSERVE_EXISTING')
 def test_submission_disabled(self):
  with self.assertRaises(UnsafeSubmission): submit(I)
 def test_invalid_identity(self):
  with self.assertRaises(UnsafeSubmission): assess_submission(SubmissionIdentity('bad',1,I.project,I.region,I.job_name,I.spec_digest),'PREPARED',[])
 def test_lineage_requires_done(self):
  class Client:
   def query(self,*a,**kw): raise AssertionError('should not query')
  verifier=BigQueryLineageVerifier(Client(),project=I.project,dataset='pdm_payments_dev')
  self.assertIsNone(verifier.verify({'execution_id':I.execution_id,'attempt_number':1,'digest':I.spec_digest},{'id':'123','state':'JOB_STATE_RUNNING'}))
if __name__=='__main__': unittest.main()
