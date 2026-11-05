import sys, unittest
from pathlib import Path
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'orchestration/gcp_composer/runtime'))
from provider_enforcement import CancellationTarget, verify_target, cancel_exact_expired, WatchdogBlocked
NOW=datetime(2026,10,29,12,tzinfo=timezone.utc)
T=CancellationTarget('project-e48f194e-f44f-458d-b4f','europe-west2','job-123','pdm-pilot-123','a'*64,'b'*64)
def job(**kw):
    return SimpleNamespace(id=kw.get('id',T.job_id),name=kw.get('name',T.job_name),
      labels=kw.get('labels',{'pdm_pilot':'stage10','pdm_execution':T.execution_id,'pdm_spec':T.spec_digest}),
      current_state=kw.get('state','JOB_STATE_RUNNING'),create_time=kw.get('created',NOW-timedelta(minutes=31)))
class Client:
    def __init__(self, j=None): self.j=j or job(); self.gets=0; self.updates=0
    def get_job(self,request): self.gets+=1; return self.j
    def update_job(self,request): self.updates+=1; return self.j
class Tests(unittest.TestCase):
    def test_default_no_provider_call(self):
        c=Client(); self.assertFalse(cancel_exact_expired(c,T,now=NOW)['mutated']); self.assertEqual(c.gets,0)
    def test_approval_alone_no_provider_call(self):
        c=Client(); cancel_exact_expired(c,T,now=NOW,approval=True); self.assertEqual(c.gets,0)
    def test_execute_alone_no_provider_call(self):
        c=Client(); cancel_exact_expired(c,T,now=NOW,execute=True); self.assertEqual(c.gets,0)
    def test_valid_exact_cancel(self):
        c=Client(); self.assertTrue(cancel_exact_expired(c,T,now=NOW,execute=True,approval=True)['mutated']); self.assertEqual((c.gets,c.updates),(1,1))
    def test_wrong_execution_blocked(self):
        c=Client(job(labels={'pdm_pilot':'stage10','pdm_execution':'f'*64,'pdm_spec':'b'*64}))
        with self.assertRaises(WatchdogBlocked): cancel_exact_expired(c,T,now=NOW,execute=True,approval=True)
        self.assertEqual(c.updates,0)
    def test_wrong_job_id_blocked(self):
        c=Client(job(id='other'))
        with self.assertRaises(WatchdogBlocked): cancel_exact_expired(c,T,now=NOW,execute=True,approval=True)
        self.assertEqual(c.updates,0)
    def test_not_expired_blocked(self):
        c=Client(job(created=NOW-timedelta(minutes=1)))
        with self.assertRaises(WatchdogBlocked): cancel_exact_expired(c,T,now=NOW,execute=True,approval=True)
        self.assertEqual(c.updates,0)
    def test_terminal_blocked(self):
        c=Client(job(state='JOB_STATE_DONE'))
        with self.assertRaises(WatchdogBlocked): cancel_exact_expired(c,T,now=NOW,execute=True,approval=True)
        self.assertEqual(c.updates,0)
    def test_missing_provider_get_blocked(self):
        class Broken(Client):
            def get_job(self,request): raise RuntimeError('network')
        c=Broken()
        with self.assertRaises(WatchdogBlocked): cancel_exact_expired(c,T,now=NOW,execute=True,approval=True)
        self.assertEqual(c.updates,0)
    def test_unknown_state_blocked(self):
        c=Client(job(state='JOB_STATE_NEW'))
        with self.assertRaises(WatchdogBlocked): cancel_exact_expired(c,T,now=NOW,execute=True,approval=True)
        self.assertEqual(c.updates,0)
if __name__=='__main__': unittest.main()
