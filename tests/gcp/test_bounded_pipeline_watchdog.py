import sys, unittest
from pathlib import Path
from datetime import datetime,timezone,timedelta
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'orchestration/gcp_composer/runtime'))
from bounded_watchdog import assess_job,inspect_jobs,cancel_expired,WatchdogBlocked
from launch_policy import evaluate_launch
from submission_guard import SubmissionIdentity
NOW=datetime(2026,10,29,12,tzinfo=timezone.utc)
P='project-e48f194e-f44f-458d-b4f'; R='europe-west2'
def job(**kw):
    return SimpleNamespace(id=kw.get('id','job-1'),name=kw.get('name','pdm-pilot-123'),
      labels=kw.get('labels',{'pdm_pilot':'stage10'}),
      current_state=kw.get('state','JOB_STATE_RUNNING'),
      create_time=kw.get('created',NOW-timedelta(minutes=31)))
class WatchdogTests(unittest.TestCase):
    def test_expired(self): self.assertEqual(assess_job(job(),project=P,region=R,now=NOW),'EXPIRED')
    def test_within_limit(self): self.assertEqual(assess_job(job(created=NOW-timedelta(minutes=4)),project=P,region=R,now=NOW),'WITHIN_LIMIT')
    def test_terminal(self): self.assertEqual(assess_job(job(state='JOB_STATE_DONE'),project=P,region=R,now=NOW),'TERMINAL')
    def test_unlabelled_blocked(self): self.assertEqual(assess_job(job(labels={}),project=P,region=R,now=NOW),'BLOCKED')
    def test_unknown_state_blocked(self): self.assertEqual(assess_job(job(state='NEW_STATE'),project=P,region=R,now=NOW),'BLOCKED')
    def test_scope_mismatch(self):
        with self.assertRaises(WatchdogBlocked): assess_job(job(),project='wrong',region=R,now=NOW)
    def test_missing_timestamp(self):
        with self.assertRaises(WatchdogBlocked): assess_job(job(created=None),project=P,region=R,now=NOW)
    def test_read_only_listing(self):
        class Client:
            def list_jobs(self,request): return SimpleNamespace(jobs=[job()])
        self.assertEqual(inspect_jobs(Client(),project=P,region=R,now=NOW)[0]['decision'],'EXPIRED')
    def test_listing_failure(self):
        class Client:
            def list_jobs(self,request): raise RuntimeError('unavailable')
        with self.assertRaises(WatchdogBlocked): inspect_jobs(Client(),project=P,region=R,now=NOW)
    def test_cancel_disabled(self):
        with self.assertRaises(WatchdogBlocked): cancel_expired(None)
    def test_policy_never_authorises_launch(self):
        i=SubmissionIdentity('a'*64,1,P,R,'pdm-pilot-123','b'*64)
        result=evaluate_launch(i,workers=1,runtime_minutes=20,estimated_launch_gbp=1,
            reported_spend_gbp=2,spend_observed_at=NOW,approval=True,now=NOW)
        self.assertFalse(result['submission_enabled'])
if __name__=='__main__': unittest.main()
