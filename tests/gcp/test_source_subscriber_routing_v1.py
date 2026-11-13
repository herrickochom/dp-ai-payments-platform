import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]


def load(name, relative):
    path = ROOT / relative
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Register the dataclass-bearing module before execution.
import sys

routing_path = (
    ROOT / "platform/gcp/kafka_pubsub_bridge/"
    "source_subscriber_routing_v1.py"
)
routing_spec = importlib.util.spec_from_file_location(
    "source_subscriber_routing_v1", routing_path
)
routing = importlib.util.module_from_spec(routing_spec)
sys.modules[routing_spec.name] = routing
routing_spec.loader.exec_module(routing)

runtime = load(
    "source_operational_runtime_v1",
    "platform/gcp/kafka_pubsub_bridge/"
    "source_operational_runtime_v1.py",
)


class Acker:
    def __init__(self):
        self.ids = []

    def ack(self, ack_id):
        self.ids.append(ack_id)


class RoutingAcceptance(unittest.TestCase):
    def route(self, data, processor, legacy=None):
        acker = Acker()
        events = []

        if legacy is None:
            def legacy(*args):
                events.append("legacy")
                return ("key", "CREATED")

        result = routing.route_received(
            data=data,
            ack_id="ack-17",
            bucket=object(),
            acker=acker,
            legacy_process=legacy,
            legacy_store=None,
            legacy_decoder=None,
            dispatch=runtime.dispatch,
            process_source_with_lease=processor,
            extend_lease=lambda _: events.append("extend"),
        )
        return result, acker, events

    @staticmethod
    def source_data():
        import base64
        import hashlib
        import json

        payload = b"{}"
        return json.dumps({
            "message_type": "payments.source-event.v1",
            "kafka_topic": "payment-events.retry",
            "kafka_partition": 0,
            "kafka_offset": 17,
            "kafka_timestamp_ms": 1770000000000,
            "payload_base64": base64.b64encode(
                payload
            ).decode("ascii"),
            "payload_sha256": hashlib.sha256(
                payload
            ).hexdigest(),
            "schema_registry_id": None,
            "value_is_null": False,
        }).encode("utf-8")

    def test_source_success(self):
        def processor(message, bucket, extend, **kwargs):
            extend(message)
            message.ack()
            return SimpleNamespace(
                acknowledged=True,
                reason="acknowledged_after_verification",
            )

        result, acker, events = self.route(
            self.source_data(), processor
        )

        self.assertEqual(result.route, "SOURCE")
        self.assertTrue(result.acknowledged)
        self.assertEqual(acker.ids, ["ack-17"])
        self.assertEqual(events, ["extend"])

    def test_source_lease_failure(self):
        def processor(message, bucket, extend, **kwargs):
            return SimpleNamespace(
                acknowledged=False,
                reason="initial_lease_failed",
            )

        result, acker, events = self.route(
            self.source_data(), processor
        )

        self.assertEqual(result.route, "SOURCE")
        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "initial_lease_failed")
        self.assertEqual(acker.ids, [])

    def test_source_storage_failure(self):
        def processor(message, bucket, extend, **kwargs):
            return SimpleNamespace(
                acknowledged=False,
                reason="durable_processing_failed",
            )

        result, acker, events = self.route(
            self.source_data(), processor
        )

        self.assertFalse(result.acknowledged)
        self.assertEqual(
            result.reason, "durable_processing_failed"
        )
        self.assertEqual(acker.ids, [])

    def test_invalid_source_no_fallback(self):
        import json

        legacy_calls = []

        with self.assertRaises(Exception):
            self.route(
                json.dumps({
                    "message_type": "invalid",
                    "kafka_topic": "payment-events.retry",
                }).encode(),
                lambda *args, **kwargs: self.fail(
                    "source processor must not run"
                ),
                legacy=lambda *args: legacy_calls.append(args),
            )

        self.assertEqual(legacy_calls, [])

    def test_legacy_unchanged(self):
        result, acker, events = self.route(
            b"legacy",
            lambda *args, **kwargs: self.fail(
                "source processor must not run"
            ),
        )

        self.assertEqual(result.route, "LEGACY")
        self.assertEqual(
            result.reason, "legacy_dispatch_completed"
        )
        self.assertEqual(events, ["legacy"])


if __name__ == "__main__":
    unittest.main()
