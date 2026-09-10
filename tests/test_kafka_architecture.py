import importlib.util
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


producer = load("payment_producer", "services/payment-producer/kafka_producer.py")
consumer = load("payment_consumer", "services/kafka-consumer-events/kafka_consumer_events.py")
topic_admin = load("topic_admin", "platform/kafka/topic_admin.py")


class KafkaArchitectureTests(unittest.TestCase):
    def test_stable_business_key_is_separate_from_unique_event_id(self):
        first = {"event_id": "event-1", "message_id": "message-1",
                 "parsed_event_data": '{"loan_id":"loan-42"}'}
        second = {**first, "event_id": "event-2"}
        self.assertEqual("loan-42", producer.select_business_key("pdmis.loans", first))
        self.assertEqual(producer.select_business_key("pdmis.loans", first),
                         producer.select_business_key("pdmis.loans", second))
        self.assertNotEqual(first["event_id"], second["event_id"])

    def test_message_id_and_source_key_are_documented_fallbacks(self):
        self.assertEqual("iso-message", producer.select_business_key(
            "icmn.vpm.pain001", {"event_id": "e", "message_id": "iso-message"}))
        self.assertEqual("file.xml", producer.select_business_key(
            "unknown", {"event_id": "e", "source_key": "file.xml"}))

    def test_deterministic_raw_path(self):
        stamp = datetime(2026, 9, 10, tzinfo=timezone.utc)
        expected = ("raw/v2/category=pdmis/source_group=pdmis/source_system=pdmis/"
                    "year=2026/month=09/day=10/topic=pdmis.loans/partition=2/offset=81/record.avro")
        self.assertEqual(expected, consumer.deterministic_s3_key("pdmis.loans", 2, 81, stamp))

    def test_existing_raw_object_makes_replay_a_noop(self):
        client = Mock()
        with patch.object(consumer, "get_minio_client", return_value=client):
            result = consumer.store_event_to_s3(
                {"event_id": "e"}, "pdmis.loans", 1, 2,
                datetime(2026, 9, 10, tzinfo=timezone.utc))
        self.assertTrue(result.endswith("offset=2/record.avro"))
        client.put_object.assert_not_called()

    def test_storage_precedes_synchronous_commit(self):
        calls = []
        kafka_consumer = Mock()
        kafka_consumer.commit.side_effect = lambda *_a, **_k: calls.append("commit")
        msg = Mock()
        msg.topic.return_value, msg.partition.return_value, msg.offset.return_value = "pdmis.loans", 0, 3
        with patch.object(consumer, "store_event_to_s3", side_effect=lambda *_a: calls.append("store") or "key"):
            consumer.store_then_commit(kafka_consumer, msg, {}, datetime.now(timezone.utc))
        self.assertEqual(["store", "commit"], calls)
        kafka_consumer.commit.assert_called_once_with(msg, asynchronous=False)

    def test_retry_is_exponential_and_bounded(self):
        with patch.object(consumer.Settings, "RETRY_INITIAL_DELAY_SECONDS", 2), \
             patch.object(consumer.Settings, "RETRY_BACKOFF_MULTIPLIER", 2), \
             patch.object(consumer.Settings, "RETRY_MAX_DELAY_SECONDS", 5):
            self.assertEqual([2, 4, 5, 5], [consumer.retry_delay(i) for i in range(1, 5)])

    def test_transient_failure_retries_then_dlqs(self):
        with patch.object(consumer.Settings, "MAX_PROCESSING_RETRIES", 3):
            self.assertFalse(consumer.should_send_to_dlq(OSError("temporary"), 0))
            self.assertFalse(consumer.should_send_to_dlq(OSError("temporary"), 2))
            self.assertTrue(consumer.should_send_to_dlq(OSError("temporary"), 3))
            self.assertTrue(consumer.should_send_to_dlq(
                consumer.PermanentProcessingError("poison"), 0))

    def test_poison_record_commits_only_after_dlq_ack(self):
        kafka_consumer, failure_producer, msg = Mock(), Mock(), Mock()
        with patch.object(consumer, "publish_failure", return_value=False):
            self.assertFalse(consumer.publish_dlq_then_commit(
                failure_producer, kafka_consumer, msg, {}))
            kafka_consumer.commit.assert_not_called()
        with patch.object(consumer, "publish_failure", return_value=True):
            self.assertTrue(consumer.publish_dlq_then_commit(
                failure_producer, kafka_consumer, msg, {}))
            kafka_consumer.commit.assert_called_once_with(msg, asynchronous=False)

    def test_delivery_tracker_counts_callback_results(self):
        tracker = producer.DeliveryTracker()
        message = Mock()
        message.topic.return_value, message.partition.return_value = "pdmis.loans", 4
        tracker.callback("event", "loan")(None, message)
        tracker.callback("event", "loan")(RuntimeError("failed"), message)
        self.assertEqual((1, 1), (tracker.succeeded, tracker.failed))

    def test_topic_manifest_is_authoritative_and_complete(self):
        topics = topic_admin.load_manifest(str(ROOT / "platform/kafka/topics.yaml"))
        names = {item["name"] for item in topics}
        self.assertEqual(26, len(topics))
        self.assertIn("payment-events.retry", names)
        self.assertIn("payment-events.dlq", names)
        self.assertNotIn("reconciliation.links", names)

    def test_schema_changes_require_backward_transitive_compatibility(self):
        compose = (ROOT / "docker-compose.yaml").read_text()
        self.assertIn("SCHEMA_COMPATIBILITY: BACKWARD_TRANSITIVE", compose)


if __name__ == "__main__":
    unittest.main()
