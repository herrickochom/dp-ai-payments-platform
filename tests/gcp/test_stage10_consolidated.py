import sys,unittest
from pathlib import Path
from datetime import datetime,timezone
from types import SimpleNamespace
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'orchestration/gcp_composer/runtime'))
from submission_guard import SubmissionIdentity
from safe_submission import LaunchConfig,SubmissionBlocked,build_launch_request,submit_one_attempt
from output_attestation import attest,AttestationBlocked
I=SubmissionIdentity('a'*64,1,'project-e48f194e-f44f-458d-b4f','europe-west2','pdm-test-123','b'*64)
C=LaunchConfig('gs://approved/template.json',{'input':'gs://approved/input'},'pdm@proj.iam.gserviceaccount.com','regions/europe-west2/subnetworks/private','gs://approved/tmp')
CLAIM={'state':'PREPARED','execution_id':I.execution_id,'attempt_number':1,'digest':I.spec_digest,'job_name':I.job_name,'project':I.project,'region':I.region,'token':'lease'}
COST=dict(estimated_launch_gbp=1.0,reported_spend_gbp=0.0,spend_observed_at=datetime.now(timezone.utc),approval=True)
class Store:
 def __init__(self):self.calls=[]
 def transition(self,*a,**k):self.calls.append((a,k))
class Provider:
 def __init__(self,fail=False):self.calls=[];self.fail=fail
 def launch_flex_template(self,**kwargs):
  self.calls.append(kwargs)
  if self.fail:raise TimeoutError('ambiguous')
  return SimpleNamespace(job=SimpleNamespace(id='job-123'))
class Tests(unittest.TestCase):
 def test_bounded_worker(self):
  r=build_launch_request(I,C);self.assertEqual(r['launch_parameter']['environment']['max_workers'],1)
 def test_labels_within_provider_limits(self):
  self.assertTrue(all(len(v)<=63 for v in build_launch_request(I,C)['launch_parameter']['environment']['additional_user_labels'].values()))
 def test_no_launch_without_execute(self):
  s,p=Store(),Provider();r=submit_one_attempt(store=s,provider=p,claim=CLAIM,identity=I,config=C,cost_inputs=COST)
  self.assertFalse(r['mutated']);self.assertEqual(p.calls,[]);self.assertEqual(s.calls,[])
 def test_missing_approval(self):
  s,p=Store(),Provider()
  with self.assertRaises(Exception):submit_one_attempt(store=s,provider=p,claim=CLAIM,identity=I,config=C,cost_inputs={**COST,'approval':False},execute=True)
  self.assertEqual(s.calls,[]);self.assertEqual(p.calls,[])
 def test_fenced_before_launch(self):
  s,p=Store(),Provider();r=submit_one_attempt(store=s,provider=p,claim=CLAIM,identity=I,config=C,cost_inputs=COST,execute=True)
  self.assertEqual(s.calls[0][0][2],'SUBMISSION_UNCERTAIN');self.assertEqual(len(p.calls),1)
  self.assertIsNone(p.calls[0]['retry']);self.assertNotEqual(r['decision'],'COMPLETED')
 def test_ambiguous_never_resubmitted(self):
  s,p=Store(),Provider(True)
  with self.assertRaises(SubmissionBlocked):submit_one_attempt(store=s,provider=p,claim=CLAIM,identity=I,config=C,cost_inputs=COST,execute=True)
  self.assertEqual(len(p.calls),1);self.assertEqual(len(s.calls),1)
 def test_wrong_claim(self):
  s,p=Store(),Provider()
  with self.assertRaises(SubmissionBlocked):submit_one_attempt(store=s,provider=p,claim={**CLAIM,'digest':'c'*64},identity=I,config=C,cost_inputs=COST,execute=True)
  self.assertEqual(p.calls,[])
 def test_matching_attestation(self):
  rows=[{'event_id':'evt','output_sha256':'f'*64}];ident=dict(execution_id='a'*64,attempt_number=1,job_id='job',spec_digest='b'*64)
  self.assertEqual(len(attest(expected=rows,observed=rows,manifest_verified=True,expected_identity=ident,observed_identity=ident)),64)
 def test_missing_manifest(self):
  with self.assertRaises(AttestationBlocked):attest(expected=[],observed=[],manifest_verified=False,expected_identity={},observed_identity={})
 def test_missing_output(self):
  ident=dict(execution_id='a'*64,attempt_number=1,job_id='job',spec_digest='b'*64)
  with self.assertRaises(AttestationBlocked):attest(expected=[{'event_id':'evt','output_sha256':'f'*64}],observed=[],manifest_verified=True,expected_identity=ident,observed_identity=ident)
if __name__=='__main__':unittest.main()
