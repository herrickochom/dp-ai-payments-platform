"""Stage 9D offline crash/concurrency contracts and orchestration simulations."""
import importlib.util
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[2]
BASE=ROOT/'orchestration/gcp_composer/runtime'
import sys
sys.path.insert(0,str(BASE))
from stage9_transactional_recovery import RecoveryStore, Fenced
from stage9_offline_orchestrator import tick

class FakeStore:
    def __init__(self,state='PREPARED'):
        self.claim={'execution_id':'e'*64,'attempt_number':1,'state':state,'job_name':'pdm-x',
                    'project':'p','region':'r','digest':'d'*64,'worker':'w','token':'t'}
        self.calls=[]
    def claim_due(self,worker): return self.claim
    def transition(self,claim,expected,new_state,**kwargs):
        self.calls.append((expected,new_state,kwargs))
        self.claim={**claim,'state':new_state}

class Provider:
    def __init__(self,jobs): self.jobs=jobs
    def find_by_name(self,*args): return self.jobs

class Verifier:
    def __init__(self,digest): self.digest=digest
    def verify(self,*args): return self.digest

class Stage9CDE(unittest.TestCase):
    def test_crash_before_submission_arms_uncertainty(self):
        s=FakeStore(); self.assertEqual(tick(s,Provider([]),Verifier(None)),'ARMED_FOR_RECONCILIATION')
        self.assertEqual(s.calls[0][1],'SUBMISSION_UNCERTAIN')
    def test_crash_after_submission_no_blind_retry(self):
        s=FakeStore('SUBMISSION_UNCERTAIN')
        self.assertEqual(tick(s,Provider([]),Verifier(None)),'WAIT_PROVIDER_VISIBILITY')
    def test_unknown_provider_visibility_waits(self):
        s=FakeStore('SUBMISSION_UNCERTAIN')
        self.assertEqual(tick(s,Provider(None),Verifier(None)),'WAIT_PROVIDER_VISIBILITY')
    def test_duplicate_quarantines(self):
        s=FakeStore('OBSERVING')
        self.assertEqual(tick(s,Provider([{},{}]),Verifier(None)),'QUARANTINED_DUPLICATE')
    def test_mismatch_quarantines(self):
        s=FakeStore('OBSERVING')
        self.assertEqual(tick(s,Provider([{'name':'wrong','project':'p','region':'r','spec_digest':'d'*64}]),Verifier(None)),'QUARANTINED_IDENTITY')
    def test_completion_needs_evidence(self):
        s=FakeStore('OBSERVING'); j={'id':'j','name':'pdm-x','project':'p','region':'r','spec_digest':'d'*64,'state':'JOB_STATE_DONE'}
        self.assertEqual(tick(s,Provider([j]),Verifier(None)),'WAIT_EVIDENCE')
        self.assertEqual(s.calls[0][1],'VERIFYING')
    def test_completion_with_evidence(self):
        s=FakeStore('VERIFYING'); j={'id':'j','name':'pdm-x','project':'p','region':'r','spec_digest':'d'*64,'state':'JOB_STATE_DONE'}
        self.assertEqual(tick(s,Provider([j]),Verifier('a'*64)),'EVIDENCE_RECORDED')
        self.assertEqual(s.calls[0][2]['evidence_digest'],'a'*64)
    def test_terminal_failure_quarantines(self):
        s=FakeStore('OBSERVING'); j={'id':'j','name':'pdm-x','project':'p','region':'r','spec_digest':'d'*64,'state':'JOB_STATE_FAILED'}
        self.assertEqual(tick(s,Provider([j]),Verifier(None)),'QUARANTINED_TERMINAL')
    def test_sql_uses_server_clock_and_skip_locked(self):
        src=(BASE/'stage9_transactional_recovery.py').read_text()
        self.assertIn('FOR UPDATE OF i SKIP LOCKED',src)
        self.assertIn('lease_expires_at>=clock_timestamp()',src)
    def test_no_cloud_submission_in_orchestrator(self):
        src=(BASE/'stage9_offline_orchestrator.py').read_text()
        self.assertNotIn('provider.submit(',src)
    def test_invalid_completion_rejected(self):
        with self.assertRaises(ValueError):
            RecoveryStore(lambda:None).transition({'execution_id':'x'},'OBSERVING','COMPLETED')

if __name__=='__main__': unittest.main()
