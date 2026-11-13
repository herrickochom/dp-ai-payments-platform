import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

BASE = (
    Path(__file__).resolve().parents[2]
    / "platform/gcp/kafka_pubsub_bridge"
)

def load(name, filename):
    spec = importlib.util.spec_from_file_location(
        name,
        BASE / filename,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

adapter = load(
    "stage12_adapter",
    "source_runtime_adapter_v1.py",
)

delivery = load(
    "stage12_delivery",
    "source_delivery_v1.py",
)

runtime = load(
    "stage12_runtime",
    "source_operational_runtime_v1.py",
)

class Stage12SecurityTests(unittest.TestCase):

    def envelope(self):
        return adapter.make_envelope(
            "payment-events.retry",
            0,
            7,
            1770000000000,
            b"{}",
        )

    def test_versioned_source(self):
        envelope = self.envelope()

        self.assertEqual(
            envelope["message_type"],
            "payments.source-event.v1",
        )

        self.assertTrue(
            runtime.is_source_envelope(
                json.dumps(envelope).encode()
            )
        )

    def test_missing_type_rejected(self):
        envelope = self.envelope()
        del envelope["message_type"]

        with self.assertRaises(
            adapter.contract.RouteRejected
        ):
            adapter.contract.validate_envelope(envelope)

    def test_wrong_type_rejected(self):
        envelope = self.envelope()
        envelope["message_type"] = "unknown.v1"

        with self.assertRaises(
            adapter.contract.RouteRejected
        ):
            adapter.contract.validate_envelope(envelope)

    def test_default_deny_no_publish_or_commit(self):
        self.assertEqual(
            delivery.APPROVED_SOURCE_TOPICS,
            frozenset(),
        )

        calls = []

        with self.assertRaises(delivery.DeliveryError):
            delivery.publish_then_commit(
                self.envelope(),
                lambda envelope: calls.append("publish") or "id",
                lambda: calls.append("commit"),
            )

        self.assertEqual(calls, [])

    def test_invalid_source_no_write_ack_or_fallback(self):
        envelope = self.envelope()
        envelope["payload_sha256"] = "0" * 64

        calls = []

        class FakeEntry:
            def store_one(self, *args):
                calls.append("write")

        class FakeAcker:
            def ack(self, *args):
                calls.append("ack")

        with patch.object(runtime, "ENTRY", FakeEntry()):
            with self.assertRaises(
                runtime._stage12_contract.RouteRejected
            ):
                runtime.dispatch(
                    json.dumps(envelope).encode(),
                    "offline-ack",
                    "offline-bucket",
                    FakeAcker(),
                    lambda *args: calls.append("legacy"),
                    None,
                    None,
                )

        self.assertEqual(calls, [])

    def test_unversioned_source_no_fallback(self):
        envelope = self.envelope()
        del envelope["message_type"]

        calls = []

        with self.assertRaises(runtime.OperationalError):
            runtime.dispatch(
                json.dumps(envelope).encode(),
                "offline-ack",
                None,
                None,
                lambda *args: calls.append("legacy"),
                None,
                None,
            )

        self.assertEqual(calls, [])

    def test_unknown_type_no_fallback(self):
        calls = []

        with self.assertRaises(runtime.OperationalError):
            runtime.dispatch(
                b'{"message_type":"unknown.v1"}',
                "offline-ack",
                None,
                None,
                lambda *args: calls.append("legacy"),
                None,
                None,
            )

        self.assertEqual(calls, [])

    def test_legacy_classification_preserved(self):
        self.assertFalse(
            runtime.is_source_envelope(
                b'{"legacy":"event"}'
            )
        )

if __name__ == "__main__":
    unittest.main()
