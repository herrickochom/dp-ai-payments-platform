import importlib.util
import sys
import threading
import unittest
from pathlib import Path

PATH = Path(__file__).resolve().parents[2] / 'platform/gcp/pubsub_raw_ingestion/lease_guard.py'
spec = importlib.util.spec_from_file_location('kp001w_lease_guard', PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)

class Msg:
    def __init__(self, fail_ack=False):
        self.acks = 0
        self.fail_ack = fail_ack
    def ack(self):
        self.acks += 1
        if self.fail_ack:
            raise RuntimeError('ack unavailable')

class LeaseGuardTests(unittest.TestCase):
    def test_success(self):
        m = Msg()
        r = mod.process_with_renewal(m, lambda _: True, lambda _: None)
        self.assertTrue(r.acknowledged)
        self.assertEqual(m.acks, 1)

    def test_initial_lease_failure(self):
        m = Msg()
        r = mod.process_with_renewal(m, lambda _: self.fail('must not process'), lambda _: (_ for _ in ()).throw(RuntimeError()))
        self.assertEqual(r.reason, 'initial_lease_failed')
        self.assertEqual(m.acks, 0)

    def test_failed_durable_verification(self):
        m = Msg()
        r = mod.process_with_renewal(m, lambda _: False, lambda _: None)
        self.assertFalse(r.acknowledged)
        self.assertEqual(m.acks, 0)

    def test_processing_exception(self):
        m = Msg()
        r = mod.process_with_renewal(m, lambda _: (_ for _ in ()).throw(RuntimeError()), lambda _: None)
        self.assertEqual(r.reason, 'durable_processing_failed')
        self.assertEqual(m.acks, 0)

    def test_ack_failure(self):
        m = Msg(fail_ack=True)
        r = mod.process_with_renewal(m, lambda _: True, lambda _: None)
        self.assertEqual(r.reason, 'ack_failed_redelivery_possible')

    def test_continuous_renewal(self):
        m = Msg()
        second = threading.Event()
        count = [0]
        def extend(_):
            count[0] += 1
            if count[0] >= 2:
                second.set()
        def process(_):
            self.assertTrue(second.wait(1))
            return True
        r = mod.process_with_renewal(m, process, extend, renewal_interval_seconds=.005)
        self.assertTrue(r.acknowledged)
        self.assertGreaterEqual(count[0], 2)

    def test_renewal_failure_prevents_ack(self):
        m = Msg()
        failed = threading.Event()
        count = [0]
        def extend(_):
            count[0] += 1
            if count[0] >= 2:
                failed.set()
                raise RuntimeError('lease lost')
        def process(_):
            self.assertTrue(failed.wait(1))
            return True
        r = mod.process_with_renewal(m, process, extend, renewal_interval_seconds=.005)
        self.assertEqual(r.reason, 'lease_renewal_failed')
        self.assertEqual(m.acks, 0)

    def test_invalid_interval(self):
        with self.assertRaises(ValueError):
            mod.process_with_renewal(Msg(), lambda _: True, lambda _: None, renewal_interval_seconds=0)
