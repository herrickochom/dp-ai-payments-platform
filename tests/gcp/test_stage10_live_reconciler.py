import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'orchestration/gcp_composer/runtime'))
from live_reconciler import reconcile_once
class Store:
 def __init__(self,state='SUBMISSION_UNCERTAIN'): self.state=state; self.events=[]
 def claim_due(self,worker): return dict(state=self.state,project='p',region='r',job_name='j',digest='d',worker=worker)
 def transition(self,claim,old,new,**kw): self.events.append((old,new,kw))
class Provider:
 def __init__(self,jobs): self.jobs=jobs
 def find_by_name(self,*args): return self.jobs
class Verifier:
 def __init__(self,digest): self.digest=digest
 def verify(self,*args): return self.digest
job=dict(name='j',project='p',region='r',spec_digest='d',id='id',state='JOB_STATE_DONE')
class Tests(unittest.TestCase):
 def test_prepared_does_not_submit(self):
  s=Store('PREPARED'); self.assertEqual(reconcile_once(s,Provider([job]),Verifier('x')),'ARMED_FOR_RECONCILIATION'); self.assertEqual(s.events[0][1],'SUBMISSION_UNCERTAIN')
 def test_missing_is_uncertain(self):
  s=Store(); self.assertEqual(reconcile_once(s,Provider([]),Verifier('x')),'WAIT_PROVIDER_VISIBILITY')
 def test_duplicate_quarantined(self):
  s=Store(); self.assertEqual(reconcile_once(s,Provider([job,job]),Verifier('x')),'QUARANTINED_DUPLICATE')
 def test_mismatch_quarantined(self):
  s=Store(); self.assertEqual(reconcile_once(s,Provider([{**job,'spec_digest':'bad'}]),Verifier('x')),'QUARANTINED_IDENTITY')
 def test_no_evidence_no_completion(self):
  s=Store(); self.assertEqual(reconcile_once(s,Provider([job]),Verifier(None)),'WAIT_EVIDENCE'); self.assertEqual(s.events[0][1],'VERIFYING')
 def test_verified_completion(self):
  s=Store(); self.assertEqual(reconcile_once(s,Provider([job]),Verifier('a'*64)),'EVIDENCE_RECORDED'); self.assertEqual(s.events[0][1],'COMPLETED')
if __name__=='__main__': unittest.main()
