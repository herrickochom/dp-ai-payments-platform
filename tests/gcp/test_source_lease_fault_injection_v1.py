"""Offline deterministic lease guard fault-injection acceptance."""
import importlib.util
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
RUNTIME_PATH = ROOT / (
    "platform/gcp/kafka_pubsub_bridge/"
    "source_operational_runtime_v1.py"
)

spec = importlib.util.spec_from_file_location(
    "fault_injection_operational_runtime", RUNTIME_PATH
)
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


class Message:
    def __init__(self):
        self.acks = 0

    def ack(self):
        self.acks += 1


class LeaseFaultInjectionAcceptance(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.guard = runtime.get_source_lease_guard()

    def test_concurrent_module_identity(self):
        with ThreadPoolExecutor(max_workers=16) as pool:
            modules = list(
                pool.map(
                    lambda _: runtime.get_source_lease_guard(),
                    range(64),
                )
            )

        self.assertTrue(
            all(module is modules[0] for module in modules)
        )
        self.assertIs(
            modules[0],
            sys.modules["dp_source_lease_guard"],
        )

    def test_renewal_thread_construction_failure(self):
        message = Message()
        stored = []

        with patch.object(
            self.guard,
            "Thread",
            side_effect=RuntimeError("thread construction failed"),
        ):
            with self.assertRaises(RuntimeError):
                self.guard.process_with_renewal(
                    message,
                    lambda _: stored.append(True) or True,
                    lambda _: None,
                )

        self.assertEqual(message.acks, 0)
        self.assertEqual(stored, [])

        # The failed startup must not permanently retain exclusion.
        recovery = Message()
        result = self.guard.process_with_renewal(
            recovery,
            lambda _: True,
            lambda _: None,
        )
        self.assertTrue(result.acknowledged)
        self.assertEqual(recovery.acks, 1)

    def test_renewal_thread_start_failure(self):
        message = Message()
        stored = []
        original_thread = threading.Thread

        class FailingThread(original_thread):
            def start(self):
                raise RuntimeError("thread start failed")

        with patch.object(self.guard, "Thread", FailingThread):
            with self.assertRaises(RuntimeError):
                self.guard.process_with_renewal(
                    message,
                    lambda _: stored.append(True) or True,
                    lambda _: None,
                )

        self.assertEqual(message.acks, 0)
        self.assertEqual(stored, [])

        recovery = Message()
        result = self.guard.process_with_renewal(
            recovery,
            lambda _: True,
            lambda _: None,
        )
        self.assertTrue(result.acknowledged)
        self.assertEqual(recovery.acks, 1)

    def test_reaper_failure_retains_exclusion(self):
        message = Message()
        entered = threading.Event()
        release = threading.Event()
        original_thread = threading.Thread

        class ReaperFailureThread(original_thread):
            def start(self):
                if self.name == "kp001w-renewal-reaper":
                    raise RuntimeError("reaper unavailable")
                return super().start()

        attempts = [0]

        def extend(_):
            attempts[0] += 1
            if attempts[0] == 2:
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("test renewal wait expired")

        def durable(_):
            self.assertTrue(entered.wait(2))
            return True

        # Isolate the deliberately poisoned gate in a fresh guard module.
        isolated_name = "fault_injection_isolated_lease_guard"
        guard_path = ROOT / (
            "platform/gcp/pubsub_raw_ingestion/lease_guard.py"
        )
        isolated_spec = importlib.util.spec_from_file_location(
            isolated_name, guard_path
        )
        isolated = importlib.util.module_from_spec(isolated_spec)
        sys.modules[isolated_name] = isolated

        try:
            isolated_spec.loader.exec_module(isolated)

            with patch.object(
                isolated, "Thread", ReaperFailureThread
            ):
                result = isolated.process_with_renewal(
                    message,
                    durable,
                    extend,
                    renewal_interval_seconds=0.005,
                    shutdown_timeout_seconds=0.02,
                )

            self.assertFalse(result.acknowledged)
            self.assertEqual(
                result.reason,
                "lease_renewal_shutdown_unresolved",
            )
            self.assertEqual(message.acks, 0)

            blocked = Message()
            blocked_result = isolated.process_with_renewal(
                blocked,
                lambda _: True,
                lambda _: None,
            )
            self.assertFalse(blocked_result.acknowledged)
            self.assertEqual(
                blocked_result.reason,
                "renewal_worker_unavailable",
            )
            self.assertEqual(blocked.acks, 0)

        finally:
            release.set()
            sys.modules.pop(isolated_name, None)

    def test_stalled_worker_reaper_releases_exclusion(self):
        isolated_name = "fault_injection_recovery_lease_guard"
        guard_path = ROOT / (
            "platform/gcp/pubsub_raw_ingestion/lease_guard.py"
        )
        isolated_spec = importlib.util.spec_from_file_location(
            isolated_name, guard_path
        )
        isolated = importlib.util.module_from_spec(isolated_spec)
        sys.modules[isolated_name] = isolated

        entered = threading.Event()
        release = threading.Event()
        attempts = [0]

        def extend(_):
            attempts[0] += 1
            if attempts[0] == 2:
                entered.set()
                if not release.wait(3):
                    raise RuntimeError("test renewal wait expired")

        try:
            isolated_spec.loader.exec_module(isolated)

            first = Message()
            result = isolated.process_with_renewal(
                first,
                lambda _: entered.wait(2),
                extend,
                renewal_interval_seconds=0.005,
                shutdown_timeout_seconds=0.02,
            )

            self.assertFalse(result.acknowledged)
            self.assertEqual(first.acks, 0)

            blocked = isolated.process_with_renewal(
                Message(),
                lambda _: True,
                lambda _: None,
            )
            self.assertEqual(
                blocked.reason,
                "renewal_worker_unavailable",
            )

            release.set()

            # Wait for the reaper to release exclusion, without sleeps.
            acquired = isolated._renewal_gate.acquire(timeout=2)
            self.assertTrue(acquired)
            if acquired:
                isolated._renewal_gate.release()

            recovered = Message()
            result = isolated.process_with_renewal(
                recovered,
                lambda _: True,
                lambda _: None,
            )
            self.assertTrue(result.acknowledged)
            self.assertEqual(recovered.acks, 1)

        finally:
            release.set()
            sys.modules.pop(isolated_name, None)


if __name__ == "__main__":
    unittest.main()
