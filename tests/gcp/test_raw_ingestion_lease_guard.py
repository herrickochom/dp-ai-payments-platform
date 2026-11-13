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


class BoundedLeaseAcceptance(unittest.TestCase):

    def test_deadline_expires_during_verification(self):
        import time

        message = Msg()
        renewals = []

        def extend(_):
            renewals.append(time.monotonic())

        def verify(_):
            time.sleep(0.04)
            return True

        result = mod.process_with_renewal(
            message,
            verify,
            extend,
            renewal_interval_seconds=0.005,
            max_processing_seconds=0.02,
        )

        self.assertFalse(result.acknowledged)
        self.assertEqual(message.acks, 0)
        self.assertIn(
            result.reason,
            {
                "processing_deadline_exceeded",
                "lease_renewal_failed",
            },
        )
        self.assertGreaterEqual(len(renewals), 1)

    def test_deadline_expires_during_initial_extension(self):
        import time

        message = Msg()
        processed = []

        def extend(_):
            time.sleep(0.03)

        def verify(_):
            processed.append(True)
            return True

        result = mod.process_with_renewal(
            message,
            verify,
            extend,
            renewal_interval_seconds=0.005,
            max_processing_seconds=0.01,
        )

        self.assertFalse(result.acknowledged)
        self.assertEqual(
            result.reason, "processing_deadline_exceeded"
        )
        self.assertEqual(message.acks, 0)
        self.assertEqual(
            processed, [],
            "Durable processing must not start after deadline expiry"
        )

    def test_invalid_processing_durations(self):
        for duration in (0, -1, float("inf"), float("nan")):
            with self.subTest(duration=duration):
                with self.assertRaises(ValueError):
                    mod.process_with_renewal(
                        Msg(),
                        lambda _: True,
                        lambda _: None,
                        max_processing_seconds=duration,
                    )

    def test_renewal_thread_terminates_after_completion(self):
        message = Msg()

        result = mod.process_with_renewal(
            message,
            lambda _: True,
            lambda _: None,
            renewal_interval_seconds=0.005,
            max_processing_seconds=1.0,
        )

        self.assertTrue(result.acknowledged)
        self.assertEqual(message.acks, 1)
        self.assertFalse(
            any(
                thread.name == "kp001w-lease-renewal"
                and thread.is_alive()
                for thread in threading.enumerate()
            )
        )

class LeaseReasonAcceptance(unittest.TestCase):
    def test_processing_deadline_has_distinct_reason(self):
        import time

        message = Msg()

        def verify(_):
            time.sleep(0.04)
            return True

        result = mod.process_with_renewal(
            message,
            verify,
            lambda _: None,
            renewal_interval_seconds=0.005,
            max_processing_seconds=0.02,
        )

        self.assertFalse(result.acknowledged)
        self.assertEqual(
            result.reason,
            "processing_deadline_exceeded",
        )
        self.assertEqual(message.acks, 0)

    def test_renewal_exception_has_distinct_reason(self):
        import time

        message = Msg()
        calls = [0]

        def extend(_):
            calls[0] += 1
            if calls[0] >= 2:
                raise RuntimeError("simulated renewal failure")

        def verify(_):
            time.sleep(0.04)
            return True

        result = mod.process_with_renewal(
            message,
            verify,
            extend,
            renewal_interval_seconds=0.005,
            max_processing_seconds=1.0,
        )

        self.assertFalse(result.acknowledged)
        self.assertEqual(
            result.reason,
            "lease_renewal_failed",
        )
        self.assertEqual(message.acks, 0)

    def test_initial_extension_deadline_prevents_processing(self):
        import time

        message = Msg()
        processed = []

        def extend(_):
            time.sleep(0.03)

        def verify(_):
            processed.append(True)
            return True

        result = mod.process_with_renewal(
            message,
            verify,
            extend,
            renewal_interval_seconds=0.005,
            max_processing_seconds=0.01,
        )

        self.assertFalse(result.acknowledged)
        self.assertEqual(
            result.reason,
            "processing_deadline_exceeded",
        )
        self.assertEqual(message.acks, 0)
        self.assertEqual(processed, [])
