import importlib.util
import pathlib
import unittest
P = pathlib.Path(__file__).resolve().parents[2] / 'platform/gcp/ingestion_recovery/recovery_policy.py'
spec = importlib.util.spec_from_file_location('kp001x_recovery_policy', P)
m = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = m
spec.loader.exec_module(m)
class RecoveryPolicyTests(unittest.TestCase):
    def test_verified_ack(self):
        d=m.decide(durable_verified=True,identity_valid=True,ack_future_confirmed=True)
        self.assertEqual(d.outcome,m.Outcome.VERIFIED_ACK); self.assertFalse(d.allow_ack)
    def test_unconfirmed_ack(self):
        d=m.decide(durable_verified=True,identity_valid=True)
        self.assertEqual(d.outcome,m.Outcome.ACK_UNCONFIRMED); self.assertTrue(d.allow_ack)
    def test_unverified_storage(self):
        self.assertFalse(m.decide(durable_verified=False,identity_valid=True).allow_ack)
    def test_processing_error(self):
        self.assertEqual(m.decide(durable_verified=True,identity_valid=True,processing_error=True).outcome,m.Outcome.RETRYABLE)
    def test_conflict(self):
        self.assertEqual(m.decide(durable_verified=True,identity_valid=True,content_conflict=True).outcome,m.Outcome.QUARANTINE_REQUIRED)
    def test_invalid_identity(self):
        self.assertFalse(m.decide(durable_verified=True,identity_valid=False).allow_ack)
if __name__=='__main__': unittest.main()
