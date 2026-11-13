import unittest,sys
from pathlib import Path
from datetime import datetime,timezone,timedelta
from types import SimpleNamespace
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'orchestration/gcp_composer/runtime'))
from ledger_watchdog import LedgerJob,assess_ledger_job,WatchdogBlocked
from billing_evidence import validate_observation,CostGateBlocked
NOW=datetime(2026,10,29,12,tzinfo=timezone.utc)
L=LedgerJob('a'*64,1,'job-123','pdm-pilot-123',
            'project-e48f194e-f44f-458d-b4f','europe-west2','b'*64,'OBSERVING')
def job(**kwargs):
    return SimpleNamespace(id=kwargs.get('id','job-123'),name='pdm-pilot-123',
        labels=kwargs.get('labels',{'pdm_pilot':'stage10','pdm_execution':'a'*64,'pdm_spec':'b'*64}),
        current_state='JOB_STATE_RUNNING',create_time=NOW-timedelta(minutes=31))
class Client:
    def __init__(self,j):self.j=j;self.calls=0
    def get_job(self,request):self.calls+=1;return self.j
class Tests(unittest.TestCase):
    def test_exact_read_only(self):
        c=Client(job()); result=assess_ledger_job(c,L,now=NOW)
        self.assertEqual(result['decision'],'EXPIRED_VERIFIED_READ_ONLY')
        self.assertFalse(result['mutation_allowed'])
        self.assertEqual(c.calls,1)
    def test_mismatched_job(self):
        with self.assertRaises(WatchdogBlocked):
            assess_ledger_job(Client(job(id='other')),L,now=NOW)
    def test_unowned_job(self):
        with self.assertRaises(WatchdogBlocked):
            assess_ledger_job(Client(job(labels={})),L,now=NOW)
    def test_invalid_identity(self):
        bad=LedgerJob('invalid',1,L.job_id,L.job_name,L.project,L.region,L.spec_digest,L.state)
        with self.assertRaises(WatchdogBlocked):
            assess_ledger_job(Client(job()),bad,now=NOW)
    def test_valid_billing_observation_still_not_authorised(self):
        r=validate_observation(project=L.project,currency='GBP',spend='2.00',
          observed_at=NOW,source='RECONCILED_BILLING_EXPORT',now=NOW)
        self.assertFalse(r['launch_authorised'])
    def test_stale_billing_blocked(self):
        with self.assertRaises(CostGateBlocked):
            validate_observation(project=L.project,currency='GBP',spend='2',
              observed_at=NOW-timedelta(hours=3),source='RECONCILED_BILLING_EXPORT',now=NOW)
    def test_wrong_currency_blocked(self):
        with self.assertRaises(CostGateBlocked):
            validate_observation(project=L.project,currency='USD',spend='2',
              observed_at=NOW,source='RECONCILED_BILLING_EXPORT',now=NOW)
    def test_fake_source_blocked(self):
        with self.assertRaises(CostGateBlocked):
            validate_observation(project=L.project,currency='GBP',spend='2',
              observed_at=NOW,source='MANUAL',now=NOW)
    def test_negative_spend_blocked(self):
        with self.assertRaises(CostGateBlocked):
            validate_observation(project=L.project,currency='GBP',spend='-1',
              observed_at=NOW,source='RECONCILED_BILLING_EXPORT',now=NOW)
if __name__=='__main__':unittest.main()
