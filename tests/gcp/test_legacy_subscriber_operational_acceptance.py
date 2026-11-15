"""Offline acceptance: legacy Pub/Sub -> Avro Raw -> guarded ack."""

import base64
import hashlib
import importlib.util
import json
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]


def load(relative, name):
    path = ROOT / relative
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runtime = load(
    "platform/gcp/kafka_pubsub_bridge/source_operational_runtime_v1.py",
    "legacy_operational_acceptance_runtime",
)
raw = load(
    "platform/gcp/pubsub_raw_ingestion/raw_v1.py",
    "legacy_operational_acceptance_raw",
)


def envelope(offset=17):
    schema_id = 1
    wire = bytes([0]) + schema_id.to_bytes(4, "big") + b"{}"
    return json.dumps({
        "kafka_topic": "pdmis.repayments",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "kafka_timestamp_ms": 1770000000000,
        "schema_registry_id": schema_id,
        "payload_base64": base64.b64encode(wire).decode(),
        "payload_sha256": hashlib.sha256(wire).hexdigest(),
    }).encode()


def decoded_event(wire, topic):
    assert wire[:1] == bytes([0])
    assert topic == "pdmis.repayments"
    return {
        "event_id": "payment-event-17",
        "source_system": "pdmis",
        "message_type": "repayments",
    }


class MemoryStore:
    def __init__(self):
        self.objects = {}
        self.created = 0
        self.events = []

    def create_if_absent(self, key, data):
        self.events.append(("create_attempt", key))
        if key in self.objects:
            return False
        self.objects[key] = data
        self.created += 1
        return True

    def read(self, key):
        self.events.append(("read", key))
        return self.objects[key]


class FakeSubscriber:
    def __init__(self, store):
        self.store = store
        self.events = []
        self.fail_ack = False
        self.fail_initial_lease = False
        self.fail_renewal = False
        self.lease_calls = 0
        self.renewal_failed = threading.Event()

    def acknowledge(self, *, request, timeout, retry):
        self.events.append(("ack_attempt", request["ack_ids"][0]))
        assert timeout == 5.0
        assert retry is None
        if self.fail_ack:
            raise RuntimeError("ack unavailable")

    def modify_ack_deadline(self, *, request, timeout, retry):
        self.lease_calls += 1
        self.events.append(("lease", request["ack_ids"][0]))
        assert request["ack_deadline_seconds"] == 30
        assert timeout == 5.0
        assert retry is None

        if self.fail_initial_lease and self.lease_calls == 1:
            raise RuntimeError("initial lease unavailable")

        if self.fail_renewal and self.lease_calls >= 2:
            self.renewal_failed.set()
            raise RuntimeError("renewal unavailable")


class ForbiddenLegacyAcker:
    def ack(self, ack_id):
        raise AssertionError("legacy acknowledger must not be called")


class LegacyOperationalAcceptance(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.subscriber = FakeSubscriber(self.store)

    def invoke(
        self,
        *,
        offset=17,
        processor=None,
        interval=0.005,
        budget=1.0,
    ):
        received = SimpleNamespace(
            ack_id="ack-17",
            message=SimpleNamespace(data=envelope(offset)),
        )

        return runtime.route_pulled_message(
            received=received,
            subscriber=self.subscriber,
            subscription="projects/test/subscriptions/test",
            bucket=object(),
            legacy_acker=ForbiddenLegacyAcker(),
            legacy_process=raw.process_raw_v1 if processor is None else processor,
            legacy_store=self.store,
            legacy_decoder=decoded_event,
            renewal_interval_seconds=interval,
            max_processing_seconds=budget,
        )

    def test_create_readback_then_ack(self):
        result = self.invoke()

        self.assertEqual(result.route, "LEGACY")
        self.assertTrue(result.acknowledged)
        self.assertEqual(
            result.reason, "acknowledged_after_verification"
        )
        self.assertEqual(self.store.created, 1)

        self.assertEqual(
            len(self.store.objects), 1
        )
        key = next(iter(self.store.objects))
        self.assertTrue(key.startswith("raw/v1/streaming/"))
        self.assertTrue(key.endswith("/record.avro"))

        self.assertEqual(
            self.subscriber.events[0],
            ("lease", "ack-17"),
        )
        self.assertEqual(
            self.subscriber.events[-1],
            ("ack_attempt", "ack-17"),
        )
        self.assertTrue(
            any(event[0] == "read" for event in self.store.events)
        )

    def test_duplicate_delivery_is_idempotent(self):
        first = self.invoke()
        second = self.invoke()

        self.assertTrue(first.acknowledged)
        self.assertTrue(second.acknowledged)
        self.assertEqual(self.store.created, 1)
        self.assertEqual(len(self.store.objects), 1)
        self.assertEqual(
            sum(
                event[0] == "ack_attempt"
                for event in self.subscriber.events
            ),
            2,
        )

    def test_existing_coordinate_collision_prevents_ack(self):
        first = self.invoke()
        self.assertTrue(first.acknowledged)

        key = next(iter(self.store.objects))
        self.store.objects[key] = b"invalid-existing-object"
        previous_acks = sum(
            event[0] == "ack_attempt"
            for event in self.subscriber.events
        )

        result = self.invoke()

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "durable_processing_failed")
        self.assertEqual(
            sum(
                event[0] == "ack_attempt"
                for event in self.subscriber.events
            ),
            previous_acks,
        )

    def test_initial_lease_failure_prevents_storage_and_ack(self):
        self.subscriber.fail_initial_lease = True
        result = self.invoke()

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "initial_lease_failed")
        self.assertEqual(self.store.objects, {})
        self.assertFalse(
            any(
                event[0] == "ack_attempt"
                for event in self.subscriber.events
            )
        )

    def test_ack_failure_preserves_durable_record(self):
        self.subscriber.fail_ack = True
        result = self.invoke()

        self.assertFalse(result.acknowledged)
        self.assertEqual(
            result.reason, "ack_failed_redelivery_possible"
        )
        self.assertEqual(self.store.created, 1)
        self.assertEqual(len(self.store.objects), 1)

    def test_renewal_failure_prevents_ack(self):
        self.subscriber.fail_renewal = True

        def slow_processor(data, ack_id, store, acker, decoder):
            result = raw.process_raw_v1(
                data, ack_id, store, acker, decoder
            )
            self.assertTrue(
                self.subscriber.renewal_failed.wait(1.0)
            )
            return result

        result = self.invoke(processor=slow_processor)

        self.assertFalse(result.acknowledged)
        self.assertEqual(result.reason, "lease_renewal_failed")
        self.assertEqual(self.store.created, 1)
        self.assertFalse(
            any(
                event[0] == "ack_attempt"
                for event in self.subscriber.events
            )
        )

    def test_processing_deadline_prevents_ack(self):
        def slow_processor(data, ack_id, store, acker, decoder):
            result = raw.process_raw_v1(
                data, ack_id, store, acker, decoder
            )
            time.sleep(0.04)
            return result

        result = self.invoke(
            processor=slow_processor,
            budget=0.02,
        )

        self.assertFalse(result.acknowledged)
        self.assertEqual(
            result.reason, "processing_deadline_exceeded"
        )
        self.assertEqual(self.store.created, 1)
        self.assertFalse(
            any(
                event[0] == "ack_attempt"
                for event in self.subscriber.events
            )
        )


if __name__ == "__main__":
    unittest.main()
