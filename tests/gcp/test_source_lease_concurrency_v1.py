"""Offline concurrency acceptance for the governed lease guard."""

import importlib.util
import sys
import threading
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "platform/gcp/pubsub_raw_ingestion/lease_guard.py"

spec = importlib.util.spec_from_file_location(
    "source_lease_guard_concurrency_v1", PATH
)
guard = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = guard
spec.loader.exec_module(guard)


class Message:
    def __init__(self):
        self.acks = 0

    def ack(self):
        self.acks += 1


class LeaseConcurrencyAcceptance(unittest.TestCase):

    def test_stalled_renewal_blocks_ack_and_subsequent_work(self):
        entered = threading.Event()
        release = threading.Event()
        calls = []
        first = Message()
        second = Message()

        def extend(_):
            calls.append("extend")
            if len(calls) == 2:
                entered.set()
                if not release.wait(2):
                    raise TimeoutError("test cleanup timeout")

        def verify(_):
            self.assertTrue(
                entered.wait(1),
                "Renewal worker did not enter callback",
            )
            return True

        try:
            start = time.monotonic()

            result = guard.process_with_renewal(
                first,
                verify,
                extend,
                renewal_interval_seconds=0.005,
                max_processing_seconds=1.0,
                shutdown_timeout_seconds=0.02,
            )

            elapsed = time.monotonic() - start

            self.assertFalse(result.acknowledged)
            self.assertEqual(
                result.reason,
                "lease_renewal_shutdown_unresolved",
            )
            self.assertEqual(first.acks, 0)
            self.assertLess(elapsed, 0.8)

            before = len(calls)

            blocked = guard.process_with_renewal(
                second,
                lambda _: self.fail(
                    "Second message must not be processed"
                ),
                extend,
                shutdown_timeout_seconds=0.02,
            )

            self.assertFalse(blocked.acknowledged)
            self.assertEqual(
                blocked.reason,
                "renewal_worker_unavailable",
            )
            self.assertEqual(second.acks, 0)
            self.assertEqual(len(calls), before)

        finally:
            release.set()

        # The reaper releases exclusion after the worker terminates.
        # Poll only for a bounded period; never leave the test hanging.
        deadline = time.monotonic() + 1.0
        recovered = None

        while time.monotonic() < deadline:
            recovered = guard.process_with_renewal(
                second,
                lambda _: True,
                lambda _: None,
                renewal_interval_seconds=0.005,
                shutdown_timeout_seconds=0.02,
            )

            if recovered.reason != "renewal_worker_unavailable":
                break

            time.sleep(0.005)

        self.assertIsNotNone(recovered)
        self.assertTrue(recovered.acknowledged, recovered.reason)
        self.assertEqual(second.acks, 1)

    def test_renewal_exception_prevents_ack(self):
        entered = threading.Event()
        message = Message()
        count = [0]

        def extend(_):
            count[0] += 1
            if count[0] == 2:
                entered.set()
                raise RuntimeError("renewal failed")

        def verify(_):
            self.assertTrue(entered.wait(1))
            return True

        result = guard.process_with_renewal(
            message,
            verify,
            extend,
            renewal_interval_seconds=0.005,
            shutdown_timeout_seconds=0.02,
        )

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "lease_renewal_failed")
        self.assertEqual(message.acks, 0)

    def test_initial_failure_releases_gate(self):
        first = Message()
        second = Message()

        result = guard.process_with_renewal(
            first,
            lambda _: self.fail("Must not process"),
            lambda _: (_ for _ in ()).throw(
                RuntimeError("initial extension failed")
            ),
        )

        self.assertEqual(result.reason, "initial_lease_failed")
        self.assertEqual(first.acks, 0)

        recovered = guard.process_with_renewal(
            second,
            lambda _: True,
            lambda _: None,
            shutdown_timeout_seconds=0.02,
        )

        self.assertTrue(recovered.acknowledged)
        self.assertEqual(second.acks, 1)


if __name__ == "__main__":
    unittest.main()
