import importlib.util
from pathlib import Path
import unittest

P = Path(__file__).resolve().parents[2] / 'orchestration/gcp_composer/runtime/stage9_recovery_policy.py'
spec = importlib.util.spec_from_file_location('stage9_recovery_policy', P)
mod = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)

class RecoveryPolicyTests(unittest.TestCase):
    def snap(self, **changes):
        defaults = dict(intent_state='SUBMISSION_UNCERTAIN', provider_state=None,
                        provider_identity_matches=None, evidence_verified=False,
                        submission_may_have_occurred=True, reconciliation_attempts=0)
        defaults.update(changes)
        return mod.RecoverySnapshot(**defaults)
    def test_unknown_never_submits(self):
        self.assertEqual(mod.decide_recovery(self.snap()), mod.RecoveryAction.RECONCILE)
    def test_budget_exhaustion_quarantines(self):
        self.assertEqual(mod.decide_recovery(self.snap(reconciliation_attempts=20)), mod.RecoveryAction.QUARANTINE)
    def test_mismatch_quarantines(self):
        self.assertEqual(mod.decide_recovery(self.snap(provider_identity_matches=False)), mod.RecoveryAction.QUARANTINE)
    def test_done_without_evidence_verifies(self):
        self.assertEqual(mod.decide_recovery(self.snap(provider_state='JOB_STATE_DONE', provider_identity_matches=True)), mod.RecoveryAction.VERIFY_OUTPUT)
    def test_done_with_evidence_completes(self):
        self.assertEqual(mod.decide_recovery(self.snap(provider_state='JOB_STATE_DONE', provider_identity_matches=True, evidence_verified=True)), mod.RecoveryAction.COMPLETE)
    def test_failed_requires_governed_retry(self):
        self.assertEqual(mod.decide_recovery(self.snap(provider_state='JOB_STATE_FAILED')), mod.RecoveryAction.QUARANTINE)
    def test_prepared_waits(self):
        self.assertEqual(mod.decide_recovery(self.snap(intent_state='PREPARED', submission_may_have_occurred=False)), mod.RecoveryAction.WAIT)
    def test_backoff_bounded(self):
        self.assertEqual(mod.next_delay_seconds(0), 10)
        self.assertEqual(mod.next_delay_seconds(100), 900)
    def test_bad_backoff_rejected(self):
        with self.assertRaises(ValueError): mod.next_delay_seconds(-1)

if __name__ == '__main__': unittest.main()
