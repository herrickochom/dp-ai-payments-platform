"""Offline acceptance of governed pulled-message routing."""

import base64
import hashlib
import importlib.util
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / (
    "platform/gcp/kafka_pubsub_bridge/"
    "source_operational_runtime_v1.py"
)

spec = importlib.util.spec_from_file_location(
    "pulled_subscriber_acceptance_runtime", PATH
)
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


class FakeSubscriber:
    def __init__(self):
        self.events = []

    def acknowledge(self, *, request, timeout, retry):
        self.events.append(("ack", request["ack_ids"][0]))
        assert timeout == 5.0
        assert retry is None

    def modify_ack_deadline(self, *, request, timeout, retry):
        self.events.append(("lease", request["ack_ids"][0]))
        assert request["ack_deadline_seconds"] == 30
        assert timeout == 5.0
        assert retry is None


class FakeLegacyAcker:
    def __init__(self):
        self.ids = []

    def ack(self, ack_id):
        self.ids.append(ack_id)


def source_data():
    payload = b"{}"
    return json.dumps({
        "message_type": "payments.source-event.v1",
        "kafka_topic": "payment-events.retry",
        "kafka_partition": 0,
        "kafka_offset": 17,
        "kafka_timestamp_ms": 1770000000000,
        "payload_base64": base64.b64encode(payload).decode(),
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "schema_registry_id": None,
        "value_is_null": False,
    }).encode()


class PulledSubscriberAcceptance(unittest.TestCase):

    def setUp(self):
        self.subscriber = FakeSubscriber()
        self.legacy_acker = FakeLegacyAcker()
        self.legacy_calls = []

    def invoke(self, data, processor, *, budget=60.0):
        received = SimpleNamespace(
            ack_id="ack-17",
            message=SimpleNamespace(data=data),
        )

        def legacy_process(data, ack_id, store, acker, decoder):
            self.legacy_calls.append(data)
            acker.ack(ack_id)
            return ("legacy-key", "CREATED")

        with patch.object(
            runtime,
            "process_source_with_lease",
            side_effect=processor,
        ):
            return runtime.route_pulled_message(
                received=received,
                subscriber=self.subscriber,
                subscription="projects/test/subscriptions/test",
                bucket=object(),
                legacy_acker=self.legacy_acker,
                legacy_process=legacy_process,
                legacy_store=object(),
                legacy_decoder=object(),
                max_processing_seconds=budget,
            )

    def test_source_success_single_ack(self):
        budgets = []

        def processor(message, bucket, extend, **kwargs):
            budgets.append(kwargs["max_processing_seconds"])
            extend(message)
            self.subscriber.events.append(("verified", True))
            message.ack()
            return SimpleNamespace(
                acknowledged=True,
                reason="acknowledged_after_verification",
            )

        result = self.invoke(source_data(), processor)

        self.assertEqual(result.route, "SOURCE")
        self.assertTrue(result.acknowledged)
        self.assertEqual(budgets, [60.0])
        self.assertEqual(
            self.subscriber.events,
            [
                ("lease", "ack-17"),
                ("verified", True),
                ("ack", "ack-17"),
            ],
        )
        self.assertEqual(self.legacy_acker.ids, [])
        self.assertEqual(self.legacy_calls, [])

    def test_source_lease_failure_no_ack(self):
        def processor(message, bucket, extend, **kwargs):
            extend(message)
            return SimpleNamespace(
                acknowledged=False,
                reason="initial_lease_failed",
            )

        result = self.invoke(source_data(), processor)

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "initial_lease_failed")
        self.assertEqual(
            self.subscriber.events,
            [("lease", "ack-17")],
        )
        self.assertEqual(self.legacy_acker.ids, [])

    def test_source_storage_failure_no_ack(self):
        def processor(message, bucket, extend, **kwargs):
            extend(message)
            return SimpleNamespace(
                acknowledged=False,
                reason="durable_processing_failed",
            )

        result = self.invoke(source_data(), processor)

        self.assertFalse(result.acknowledged)
        self.assertFalse(
            any(event[0] == "ack"
                for event in self.subscriber.events)
        )

    def test_unresolved_renewal_no_ack(self):
        def processor(message, bucket, extend, **kwargs):
            extend(message)
            return SimpleNamespace(
                acknowledged=False,
                reason="lease_renewal_shutdown_unresolved",
            )

        result = self.invoke(source_data(), processor)

        self.assertFalse(result.acknowledged)
        self.assertEqual(
            result.reason,
            "lease_renewal_shutdown_unresolved",
        )
        self.assertFalse(
            any(event[0] == "ack"
                for event in self.subscriber.events)
        )

    def test_legacy_uses_only_legacy_acker(self):
        def forbidden(*args, **kwargs):
            self.fail("Source processor called for legacy message")

        result = self.invoke(b"legacy-message", forbidden)

        self.assertEqual(result.route, "LEGACY")
        self.assertEqual(self.legacy_calls, [b"legacy-message"])
        self.assertEqual(self.legacy_acker.ids, ["ack-17"])
        self.assertEqual(self.subscriber.events, [])

    def test_invalid_source_no_fallback_or_ack(self):
        invalid = json.dumps({
            "message_type": "invalid",
            "kafka_topic": "payment-events.retry",
        }).encode()

        def forbidden(*args, **kwargs):
            self.fail("Invalid source reached processor")

        with self.assertRaises(Exception):
            self.invoke(invalid, forbidden)

        self.assertEqual(self.legacy_calls, [])
        self.assertEqual(self.legacy_acker.ids, [])
        self.assertEqual(self.subscriber.events, [])

    def test_nonfinite_processing_budget_rejected(self):
        def forbidden(*args, **kwargs):
            self.fail("Processor must not run")

        for value in (None, 0, -1, float("inf"), float("nan"), True):
            with self.subTest(value=value):
                with self.assertRaises(runtime.OperationalError):
                    self.invoke(source_data(), forbidden, budget=value)

        self.assertEqual(self.subscriber.events, [])


if __name__ == "__main__":
    unittest.main()
