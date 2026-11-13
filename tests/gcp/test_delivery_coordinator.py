import ast
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / "services/payment-producer"
sys.path.insert(0, str(BASE))

from delivery_journal import DeliveryJournal, RecoveryBlocked
from delivery_coordinator import (
    DeliveryCoordinator, DeliveryFailure
)


class Message:
    def __init__(self, topic, key=b"k"):
        self._topic = topic
        self._key = key

    def topic(self):
        return self._topic

    def partition(self):
        return 1

    def offset(self):
        return 42

    def key(self):
        return self._key


class FakeProducer:
    def __init__(self, mode="success"):
        self.mode = mode
        self.callbacks = []
        self.produce_count = 0

    def produce(self, topic, key, value, on_delivery):
        self.produce_count += 1
        if self.mode == "produce_error":
            raise RuntimeError("simulated produce failure")
        self.callbacks.append((topic, key, on_delivery))

    def poll(self, timeout):
        if self.mode == "poll_error":
            raise RuntimeError("simulated poll failure")
        if self.mode == "callback_error" and self.callbacks:
            _, _, cb = self.callbacks.pop(0)
            cb(RuntimeError("simulated delivery error"), None)

    def flush(self, timeout):
        if self.mode == "incomplete":
            return len(self.callbacks)
        if self.mode == "flush_error":
            raise RuntimeError("simulated flush failure")
        while self.callbacks:
            topic, key, cb = self.callbacks.pop(0)
            cb(None, Message(topic, key))
        return 0


def publish(coordinator, event_id, content=b"record"):
    coordinator.publish(
        "agent.transactions", event_id,
        content, b"k", b"wire"
    )


def test_successful_acknowledgement(tmp_path):
    with DeliveryJournal(tmp_path / "j.sqlite") as journal:
        c = DeliveryCoordinator(journal, FakeProducer())
        publish(c, "event-1")
        assert len(c.finish()) == 1
        assert journal.state("event-1") == "CONFIRMED"


def test_callback_failure_blocks_recovery(tmp_path):
    path = tmp_path / "j.sqlite"
    with DeliveryJournal(path) as journal:
        c = DeliveryCoordinator(
            journal, FakeProducer("callback_error")
        )
        with pytest.raises(DeliveryFailure):
            publish(c, "event-2")
        assert journal.state("event-2") == "UNCERTAIN"

    with DeliveryJournal(path) as journal:
        with pytest.raises(RecoveryBlocked):
            journal.assert_recovery_safe()


def test_incomplete_flush_blocks_success(tmp_path):
    with DeliveryJournal(tmp_path / "j.sqlite") as journal:
        c = DeliveryCoordinator(
            journal, FakeProducer("incomplete")
        )
        publish(c, "event-3")
        with pytest.raises(DeliveryFailure):
            c.finish()
        assert journal.state("event-3") == "PENDING"


def test_produce_exception_remains_pending(tmp_path):
    with DeliveryJournal(tmp_path / "j.sqlite") as journal:
        c = DeliveryCoordinator(
            journal, FakeProducer("produce_error")
        )
        with pytest.raises(RuntimeError):
            publish(c, "event-4")
        assert journal.state("event-4") == "PENDING"


def test_poll_exception_remains_pending(tmp_path):
    with DeliveryJournal(tmp_path / "j.sqlite") as journal:
        c = DeliveryCoordinator(
            journal, FakeProducer("poll_error")
        )
        with pytest.raises(RuntimeError):
            publish(c, "event-5")
        assert journal.state("event-5") == "PENDING"


def test_duplicate_event_cannot_publish_twice(tmp_path):
    with DeliveryJournal(tmp_path / "j.sqlite") as journal:
        producer = FakeProducer()
        c = DeliveryCoordinator(journal, producer)
        publish(c, "event-6")
        c.finish()
        with pytest.raises(RecoveryBlocked):
            publish(c, "event-6")
        assert producer.produce_count == 1


def test_changed_content_is_rejected(tmp_path):
    with DeliveryJournal(tmp_path / "j.sqlite") as journal:
        producer = FakeProducer()
        c = DeliveryCoordinator(journal, producer)
        publish(c, "event-7", b"original")
        c.finish()
        with pytest.raises(RecoveryBlocked):
            publish(c, "event-7", b"changed")
        assert producer.produce_count == 1


def test_restart_blocks_pending_event(tmp_path):
    path = tmp_path / "j.sqlite"
    with DeliveryJournal(path) as journal:
        c = DeliveryCoordinator(
            journal, FakeProducer("incomplete")
        )
        publish(c, "event-8")

    with DeliveryJournal(path) as journal:
        with pytest.raises(RecoveryBlocked):
            DeliveryCoordinator(journal, FakeProducer())


def test_flush_exception_does_not_confirm(tmp_path):
    with DeliveryJournal(tmp_path / "j.sqlite") as journal:
        c = DeliveryCoordinator(
            journal, FakeProducer("flush_error")
        )
        publish(c, "event-10")
        with pytest.raises(RuntimeError):
            c.finish()
        assert journal.state("event-10") == "PENDING"


def test_multiple_inflight_deliveries(tmp_path):
    with DeliveryJournal(tmp_path / "j.sqlite") as journal:
        c = DeliveryCoordinator(journal, FakeProducer())
        publish(c, "event-a")
        publish(c, "event-b")
        publish(c, "event-c")
        assert len(c.finish()) == 3
        assert journal.counts() == {"CONFIRMED": 3}


def test_live_entry_is_fail_closed():
    source = (
        BASE / "source_publisher.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    run_live = next(
        n for n in tree.body
        if isinstance(n, ast.FunctionDef)
        and n.name == "run_live"
    )
    assert isinstance(run_live.body[0], ast.Raise)
