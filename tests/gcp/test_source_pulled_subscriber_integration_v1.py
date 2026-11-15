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


def legacy_data():
    schema_id = 1
    wire = bytes([0]) + schema_id.to_bytes(4, "big") + b"{}"

    return json.dumps({
        "kafka_topic": "pdmis.repayments",
        "kafka_partition": 0,
        "kafka_offset": 17,
        "kafka_timestamp_ms": 1770000000000,
        "schema_registry_id": schema_id,
        "payload_base64": base64.b64encode(wire).decode(),
        "payload_sha256": hashlib.sha256(wire).hexdigest(),
    }).encode()


class PulledSubscriberAcceptance(unittest.TestCase):

    def setUp(self):
        self.subscriber = FakeSubscriber()
        self.legacy_acker = FakeLegacyAcker()
        self.legacy_calls = []

    def invoke(
        self, data, processor, *, budget=60.0,
        legacy_processor=None,
    ):
        received = SimpleNamespace(
            ack_id="ack-17",
            message=SimpleNamespace(data=data),
        )

        def default_legacy_process(data, ack_id, store, acker, decoder):
            self.legacy_calls.append(data)
            acker.ack(ack_id)
            return ("legacy-key", "CREATED")

        selected = (
            default_legacy_process
            if legacy_processor is None
            else legacy_processor
        )

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
                legacy_process=selected,
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

    def test_legacy_acknowledged_only_after_guard(self):
        def forbidden(*args, **kwargs):
            self.fail("Source processor called for legacy message")

        payload = legacy_data()
        result = self.invoke(payload, forbidden)

        self.assertEqual(result.route, "LEGACY")
        self.assertTrue(result.acknowledged)
        self.assertEqual(
            result.reason,
            "acknowledged_after_verification",
        )
        self.assertEqual(self.legacy_calls, [payload])
        self.assertEqual(self.legacy_acker.ids, [])
        self.assertEqual(
            self.subscriber.events,
            [("lease", "ack-17"), ("ack", "ack-17")],
        )

    def test_legacy_initial_lease_failure_prevents_processing(self):
        def forbidden(*args, **kwargs):
            self.fail("Source processor called for legacy message")

        with patch.object(
            self.subscriber,
            "modify_ack_deadline",
            side_effect=RuntimeError("lease unavailable"),
        ):
            result = self.invoke(legacy_data(), forbidden)

        self.assertEqual(result.route, "LEGACY")
        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "initial_lease_failed")
        self.assertEqual(self.legacy_calls, [])
        self.assertEqual(self.legacy_acker.ids, [])
        self.assertEqual(self.subscriber.events, [])

    def test_legacy_ack_rpc_failure_reports_unacknowledged(self):
        def forbidden(*args, **kwargs):
            self.fail("Source processor called for legacy message")

        with patch.object(
            self.subscriber,
            "acknowledge",
            side_effect=RuntimeError("ack unavailable"),
        ):
            result = self.invoke(legacy_data(), forbidden)

        self.assertEqual(result.route, "LEGACY")
        self.assertFalse(result.acknowledged)
        self.assertEqual(
            result.reason,
            "ack_failed_redelivery_possible",
        )
        self.assertEqual(self.legacy_calls, [legacy_data()])
        self.assertEqual(self.legacy_acker.ids, [])
        self.assertEqual(
            self.subscriber.events,
            [("lease", "ack-17")],
        )

    def test_legacy_missing_ack_intent_prevents_ack(self):
        def forbidden(*args, **kwargs):
            self.fail("Source processor called for legacy message")

        def processor(data, ack_id, store, acker, decoder):
            self.legacy_calls.append(data)
            return ("legacy-key", "CREATED")

        result = self.invoke(
            legacy_data(), forbidden, legacy_processor=processor
        )

        self.assertEqual(result.route, "LEGACY")
        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "not_durably_verified")
        self.assertEqual(self.legacy_acker.ids, [])
        self.assertEqual(
            self.subscriber.events,
            [("lease", "ack-17")],
        )

    def test_legacy_duplicate_ack_intent_prevents_ack(self):
        def forbidden(*args, **kwargs):
            self.fail("Source processor called for legacy message")

        def processor(data, ack_id, store, acker, decoder):
            acker.ack(ack_id)
            acker.ack(ack_id)
            return ("legacy-key", "CREATED")

        result = self.invoke(
            legacy_data(), forbidden, legacy_processor=processor
        )

        self.assertEqual(result.route, "LEGACY")
        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "durable_processing_failed")
        self.assertEqual(self.legacy_acker.ids, [])
        self.assertEqual(
            self.subscriber.events,
            [("lease", "ack-17")],
        )

    def test_legacy_processing_exception_prevents_ack(self):
        def forbidden(*args, **kwargs):
            self.fail("Source processor called for legacy message")

        def processor(data, ack_id, store, acker, decoder):
            raise RuntimeError("simulated storage failure")

        result = self.invoke(
            legacy_data(), forbidden, legacy_processor=processor
        )

        self.assertEqual(result.route, "LEGACY")
        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "durable_processing_failed")
        self.assertEqual(self.legacy_acker.ids, [])
        self.assertEqual(
            self.subscriber.events,
            [("lease", "ack-17")],
        )

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
