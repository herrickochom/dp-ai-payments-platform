import unittest
import importlib.util
import pathlib
import sys

source = pathlib.Path(__file__).resolve().parents[2] / "platform/gcp/stage9/durable_dataflow.py"
spec = importlib.util.spec_from_file_location("stage9_durable_dataflow", source)
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
make_intent, Observation, Decision, reconcile, run_once = (
    getattr(module, name) for name in
    ("make_intent", "Observation", "Decision", "reconcile", "run_once")
)

class Evidence:
    def __init__(self, digest=None): self.digest = digest
    def verify(self, intent, job): return self.digest

class Ledger:
    def __init__(self): self.submissions=[]; self.completions=[]
    def record_submission(self, k,t,j): self.submissions.append((k,t,j)); return True
    def mark_complete(self,k,t,j,d): self.completions.append((k,t,j,d)); return True

class Provider:
    def __init__(self, jobs=None, error=None): self.jobs=jobs or []; self.error=error; self.calls=0
    def find_by_name(self, intent): return self.jobs
    def submit(self, intent):
        self.calls+=1
        if self.error: raise self.error
        return Observation('id-1',intent.job_name,'JOB_STATE_PENDING',intent.project,intent.region,intent.spec_digest)

class Stage9Tests(unittest.TestCase):
    def setUp(self):
        self.intent=make_intent('payment:001','p','europe-west2','sha256:abc')
        self.job=Observation('id-1',self.intent.job_name,'JOB_STATE_RUNNING','p','europe-west2','sha256:abc')
    def test_deterministic_identity(self): self.assertEqual(self.intent,make_intent('payment:001','p','europe-west2','sha256:abc'))
    def test_empty_can_submit(self): self.assertEqual(reconcile(self.intent,[],Evidence())[0],Decision.SUBMIT)
    def test_running_observed(self): self.assertEqual(reconcile(self.intent,[self.job],Evidence())[0],Decision.OBSERVE)
    def test_ambiguous_quarantined(self): self.assertEqual(reconcile(self.intent,[self.job,self.job],Evidence())[0],Decision.QUARANTINE)
    def test_mismatch_quarantined(self): self.assertEqual(reconcile(self.intent,[Observation('x',self.job.job_name,'JOB_STATE_DONE','p','europe-west2','wrong')],Evidence('e'))[0],Decision.QUARANTINE)
    def test_done_requires_evidence(self): self.assertEqual(reconcile(self.intent,[Observation('x',self.job.job_name,'JOB_STATE_DONE','p','europe-west2','sha256:abc')],Evidence())[0],Decision.WAIT)
    def test_done_with_evidence(self): self.assertEqual(reconcile(self.intent,[Observation('x',self.job.job_name,'JOB_STATE_DONE','p','europe-west2','sha256:abc')],Evidence('sha256:out'))[0],Decision.COMPLETE)
    def test_failure_recovery(self): self.assertEqual(reconcile(self.intent,[Observation('x',self.job.job_name,'JOB_STATE_FAILED','p','europe-west2','sha256:abc')],Evidence())[0],Decision.RECOVER)
    def test_expired_lease_never_submits(self):
        p=Provider(); self.assertEqual(run_once(self.intent,'token',p,Ledger(),Evidence(),lease_valid=False),Decision.WAIT); self.assertEqual(p.calls,0)
    def test_uncertain_never_resubmits(self):
        p=Provider(); self.assertEqual(run_once(self.intent,'token',p,Ledger(),Evidence(),lease_valid=True,submission_uncertain=True),Decision.WAIT); self.assertEqual(p.calls,0)
    def test_timeout_waits(self): self.assertEqual(run_once(self.intent,'token',Provider(error=TimeoutError()),Ledger(),Evidence(),lease_valid=True),Decision.WAIT)
    def test_submission_recorded(self):
        l=Ledger(); self.assertEqual(run_once(self.intent,'token',Provider(),l,Evidence(),lease_valid=True),Decision.OBSERVE); self.assertEqual(len(l.submissions),1)

if __name__=='__main__': unittest.main()
