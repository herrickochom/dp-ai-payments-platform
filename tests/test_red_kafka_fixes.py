"""Focused tests for the two RED Kafka correctness fixes.

RED 1 - poison-record loss when DLQ publication fails:
    A source offset advances only after Raw storage OR DLQ publication succeeds
    durably.  When DLQ publication fails the consumer fails-stop (raises
    ``DlqPublishFailedError``), the offset stays uncommitted, and later offsets
    in the same partition cannot be processed or committed past it.

RED 2 - idempotent / duplicate-safe DLQ handling:
    DLQ envelopes carry a deterministic ``failure_id = <topic>:<partition>:<offset>``
    derived from the original Kafka coordinates, the DLQ record is keyed by it, the
    business key is preserved separately, and ``replay_dlq.py`` deduplicates replay
    when the same ``failure_id`` has multiple DLQ envelopes.
"""

import importlib.util
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, ROOT / path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


consumer = load("payment_consumer_red", "services/kafka-consumer-events/kafka_consumer_events.py")
replay = load("replay_dlq_red", "services/kafka-consumer-events/replay_dlq.py")


def make_msg(topic="pdmis.loans", partition=0, offset=101,
             key=b"loan-42", value=b"\x00\x00garbage-not-avro"):
    msg = Mock()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.key.return_value = key
    msg.value.return_value = value
    msg.timestamp.return_value = (1, 1750000000000, None)
    return msg


def callback_invoking_producer():
    producer = Mock()
    producer.flush.return_value = 0

    def fake_produce(topic, key=None, value=None, headers=None, callback=None):
        callback(None, Mock(topic=lambda: topic, partition=lambda: 0, offset=lambda: 0))

    producer.produce.side_effect = fake_produce
    return producer
class Red1FailStopTests(unittest.TestCase):
    def test_successful_raw_write_still_commits_normally(self):
        msg = make_msg()
        kafka_consumer = Mock()
        with patch.object(consumer, "store_event_to_s3", return_value="raw/key.avro") as store:
            outcome = consumer.process_message(
                kafka_consumer, Mock(), Mock(return_value={"event_id": "e-1"}), msg)
        self.assertEqual("ok", outcome)
        store.assert_called_once()
        kafka_consumer.commit.assert_called_once_with(msg, asynchronous=False)

    def test_successful_dlq_publish_commits_poison_source_record(self):
        msg = make_msg()
        kafka_consumer = Mock()
        with patch.object(consumer, "store_then_commit",
                          side_effect=consumer.PermanentProcessingError("poison")), \
             patch.object(consumer, "publish_failure", return_value=True) as publish:
            outcome = consumer.process_message(
                kafka_consumer, Mock(), Mock(return_value={"event_id": "e-1"}), msg)
        self.assertEqual("dlq", outcome)
        publish.assert_called_once()
        # The DLQ record is keyed by the deterministic failure identity.
        self.assertEqual("pdmis.loans:0:101", publish.call_args.kwargs["key"])
        kafka_consumer.commit.assert_called_once_with(msg, asynchronous=False)

    def test_failed_dlq_publish_does_not_commit_poison_source_record(self):
        msg = make_msg()
        kafka_consumer = Mock()
        with patch.object(consumer, "store_then_commit",
                          side_effect=consumer.PermanentProcessingError("poison")), \
             patch.object(consumer, "publish_failure", return_value=False):
            with self.assertRaises(consumer.DlqPublishFailedError):
                consumer.process_message(
                    kafka_consumer, Mock(), Mock(return_value={"event_id": "e-1"}), msg)
        kafka_consumer.commit.assert_not_called()

    def test_dlq_producer_raise_is_also_fail_stop(self):
        # publish_dlq_then_commit raising (e.g. UNKNOWN_TOPIC_OR_PART with
        # auto-create disabled) must fail-stop with the same invariant.
        msg = make_msg()
        kafka_consumer = Mock()
        with patch.object(consumer, "store_then_commit",
                          side_effect=consumer.PermanentProcessingError("poison")), \
             patch.object(consumer, "publish_dlq_then_commit",
                          side_effect=RuntimeError("no such topic")):
            with self.assertRaises(consumer.DlqPublishFailedError):
                consumer.process_message(
                    kafka_consumer, Mock(), Mock(return_value={"event_id": "e-1"}), msg)
        kafka_consumer.commit.assert_not_called()

    def test_failed_dlq_publish_cannot_let_later_offset_commit_past_it(self):
        # offset 100 = valid (committed); offset 101 = poison with failing DLQ;
        # offset 102 = valid must NEVER be processed or committed past 101.
        msg100 = make_msg(offset=100)
        msg101 = make_msg(offset=101)
        msg102 = make_msg(offset=102)
        kafka_consumer = Mock()
        kafka_consumer.poll.side_effect = [msg100, msg101, msg102, None]
        consumed = []

        def drain():
            while True:
                msg = kafka_consumer.poll(1.0)
                if msg is None:
                    return
                consumed.append(msg.offset())
                consumer.process_message(
                    kafka_consumer, Mock(), Mock(return_value={"event_id": "e-1"}), msg)

        # offset 101 fails permanent storage; its DLQ publication fails too.
        def fake_store(consumer_arg, msg_arg, event, timestamp):
            if msg_arg.offset() == 101:
                raise consumer.PermanentProcessingError("poison")
            consumer_arg.commit(msg_arg, asynchronous=False)
            return "raw/key.avro"

        with patch.object(consumer, "store_then_commit", side_effect=fake_store), \
             patch.object(consumer, "publish_failure", return_value=False):
            with self.assertRaises(consumer.DlqPublishFailedError):
                drain()

        # The consumer stopped at 101: it never polled/processed 102 and never
        # committed 101, so Kafka can only resume from <=101 on restart.
        self.assertEqual([100, 101], consumed)
        committed_offsets = [call.args[0].offset() for call in kafka_consumer.commit.call_args_list]
        self.assertEqual([100], committed_offsets)

    def test_uncommitted_poison_offset_remains_replayable_after_restart(self):
        # First run: poison 101, DLQ publish fails -> fail-stop, nothing committed.
        first_run = Mock()
        with patch.object(consumer, "store_then_commit",
                          side_effect=consumer.PermanentProcessingError("poison")), \
             patch.object(consumer, "publish_failure", return_value=False):
            with self.assertRaises(consumer.DlqPublishFailedError):
                consumer.process_message(
                    first_run, Mock(), Mock(return_value={"event_id": "e-1"}), make_msg(offset=101))
        first_run.commit.assert_not_called()

        # Restart: Kafka replays the same coordinate (101) and, because identity
        # is deterministic, the replayed record maps to the same failure_id.
        restarted = Mock()
        replayed = make_msg(offset=101)
        envelope = consumer.failure_envelope(replayed, None, ValueError("poison"), 0, "t1")
        self.assertEqual("pdmis.loans:0:101", envelope["failure_id"])
        with patch.object(consumer, "store_then_commit",
                          side_effect=consumer.PermanentProcessingError("poison")), \
             patch.object(consumer, "publish_failure", return_value=True):
            outcome = consumer.process_message(
                restarted, Mock(), Mock(return_value={"event_id": "e-1"}), replayed)
        self.assertEqual("dlq", outcome)
        restarted.commit.assert_called_once_with(replayed, asynchronous=False)


class Red2FailureIdentityTests(unittest.TestCase):
    def test_failure_id_is_deterministic_for_same_coordinates(self):
        expected = "pdmis.loans:0:101"
        self.assertEqual(expected, consumer.deterministic_failure_id("pdmis.loans", 0, 101))
        self.assertEqual(consumer.deterministic_failure_id("pdmis.loans", 0, 101),
                         consumer.deterministic_failure_id("pdmis.loans", 0, 101))

    def test_replayed_poison_record_generates_same_deterministic_identity(self):
        first = consumer.failure_envelope(make_msg(), None, ValueError("poison"), 0, "t0")
        replayed = consumer.failure_envelope(make_msg(), None, ValueError("poison"), 0, "t1")
        self.assertEqual(first["failure_id"], replayed["failure_id"])
        self.assertEqual("pdmis.loans:0:101", first["failure_id"])

    def test_different_source_offsets_generate_different_failure_ids(self):
        self.assertNotEqual(
            consumer.deterministic_failure_id("t", 0, 101),
            consumer.deterministic_failure_id("t", 0, 102))
        self.assertNotEqual(
            consumer.failure_envelope(make_msg(offset=101), None, ValueError("x"), 0, "t")["failure_id"],
            consumer.failure_envelope(make_msg(offset=102), None, ValueError("x"), 0, "t")["failure_id"])

    def test_business_key_remains_preserved_separately(self):
        envelope = consumer.failure_envelope(make_msg(key=b"loan-42"), None, ValueError("x"), 0, "t")
        self.assertEqual("pdmis.loans:0:101", envelope["failure_id"])
        self.assertEqual("loan-42", envelope["business_key"])
        self.assertNotEqual(envelope["failure_id"], envelope["business_key"])
        self.assertIn("business_key", envelope)

    def test_dlq_record_keyed_by_failure_id_with_header(self):
        producer = callback_invoking_producer()
        kafka_consumer = Mock()
        msg = make_msg(key=b"loan-42")
        ok = consumer.publish_dlq_then_commit(
            producer, kafka_consumer, msg,
            {"failure_id": "pdmis.loans:0:101", "business_key": "loan-42"})
        self.assertTrue(ok)
        published = producer.produce.call_args
        self.assertEqual(consumer.Settings.KAFKA_DLQ_TOPIC, published.args[0])
        self.assertEqual("pdmis.loans:0:101", published.kwargs["key"])
        self.assertEqual("pdmis.loans:0:101", published.kwargs["headers"]["x-failure-id"])
        kafka_consumer.commit.assert_called_once_with(msg, asynchronous=False)

    def test_crash_after_dlq_before_commit_yields_identifiable_duplicate(self):
        # 101 -> processing fails -> DLQ publish succeeds -> crash BEFORE commit.
        producer = callback_invoking_producer()
        msg = make_msg(offset=101)
        envelope = consumer.failure_envelope(msg, None, ValueError("poison"), 0, "t0")
        consumer.publish_failure(producer, consumer.Settings.KAFKA_DLQ_TOPIC, envelope,
                                 key=envelope["failure_id"],
                                 headers={"x-failure-id": envelope["failure_id"]})
        # Kafka replays 101; the consumer DLQ's it again -> second envelope with
        # the SAME failure_id, deterministically identifiable as a duplicate.
        replayed_envelope = consumer.failure_envelope(
            make_msg(offset=101), None, ValueError("poison"), 0, "t1")
        self.assertEqual(envelope["failure_id"], replayed_envelope["failure_id"])
        records = [(2, 100, envelope), (2, 101, replayed_envelope)]
        duplicates = replay.duplicate_groups(records)
        self.assertIn("pdmis.loans:0:101", duplicates)
        self.assertEqual(2, len(duplicates["pdmis.loans:0:101"]))
class Red2DuplicateSafeReplayTests(unittest.TestCase):
    def test_duplicate_dlq_envelopes_grouped_by_failure_id(self):
        env_a = {"failure_id": "t:0:101", "original_topic": "t", "original_partition": 0,
                 "original_offset": 101}
        env_b = {"failure_id": "t:0:101", "original_topic": "t", "original_partition": 0,
                 "original_offset": 101}
        env_c = {"failure_id": "t:0:102", "original_topic": "t", "original_partition": 0,
                 "original_offset": 102}
        groups = replay.group_by_failure_id([(0, 1, env_a), (1, 2, env_b), (0, 3, env_c)])
        self.assertEqual(2, len(groups))
        self.assertEqual(2, len(groups["t:0:101"]))
        self.assertEqual(1, len(groups["t:0:102"]))
        duplicates = replay.duplicate_groups([(0, 1, env_a), (1, 2, env_b), (0, 3, env_c)])
        self.assertEqual(["t:0:101"], list(duplicates))

    def test_legacy_envelope_without_failure_id_reconstructed_during_replay(self):
        legacy = {"original_topic": "t", "original_partition": 0, "original_offset": 101}
        new = {"failure_id": "t:0:101", "original_topic": "t", "original_partition": 0,
               "original_offset": 101}
        self.assertEqual(replay.failure_id_from_envelope(legacy),
                         replay.failure_id_from_envelope(new))

    def test_same_failure_id_across_legacy_and_new_envelopes_is_duplicate(self):
        legacy = {"business_key": "loan-42", "original_topic": "pdmis.loans",
                  "original_partition": 0, "original_offset": 101}
        new = {"failure_id": "pdmis.loans:0:101", "business_key": "loan-42",
               "original_topic": "pdmis.loans", "original_partition": 0,
               "original_offset": 101}
        duplicates = replay.duplicate_groups([(0, 1, legacy), (4, 9, new)])
        self.assertIn("pdmis.loans:0:101", duplicates)
        self.assertEqual(2, len(duplicates["pdmis.loans:0:101"]))

    def test_replay_execute_requires_explicit_duplicate_ack(self):
        parser = replay.parser()
        base = ["--partition", "0", "--offset", "1", "--reason", "schema repaired"]
        self.assertFalse(parser.parse_args(base).allow_duplicate_replay)
        self.assertTrue(
            parser.parse_args(base + ["--allow-duplicate-replay"]).allow_duplicate_replay)

    def test_existing_deterministic_minio_replay_identity_unchanged(self):
        stamp = datetime(2026, 9, 10, tzinfo=timezone.utc)
        with patch.dict(os.environ, {"RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2"}):
            self.assertEqual(
                "raw/v2/category=pdmis/source_group=pdmis/source_system=pdmis/year=2026/month=09/"
                "day=10/topic=pdmis.loans/partition=2/offset=81/record.avro",
                consumer.deterministic_s3_key("pdmis.loans", 2, 81, stamp))


if __name__ == "__main__":
    unittest.main()