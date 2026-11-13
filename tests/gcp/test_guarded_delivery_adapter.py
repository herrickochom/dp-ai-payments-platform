import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(
    0, str(ROOT / "services/payment-producer")
)

from delivery_journal import (
    DeliveryJournal,
    RecoveryBlocked,
)
from event_admission_policy import AdmissionBoundary
from guarded_delivery_adapter import (
    GuardedDeliveryAdapter,
    GuardedDeliveryError,
)


class FakeMessage:
    def __init__(self, topic, key):
        self._topic = topic
        self._key = key

    def topic(self):
        return self._topic

    def partition(self):
        return 2

    def offset(self):
        return 91

    def key(self):
        return self._key


class FakeProducer:
    def __init__(self, mode="success"):
        self.mode = mode
        self.pending = []
        self.produce_count = 0

    def produce(self, topic, key, value, on_delivery):
        self.produce_count += 1

        if self.mode == "produce_error":
            raise RuntimeError("simulated producer failure")

        self.pending.append(
            (topic, key, on_delivery)
        )

    def poll(self, timeout):
        if self.mode == "poll_error":
            raise RuntimeError("simulated poll failure")

    def flush(self, timeout):
        if self.mode == "incomplete":
            return len(self.pending)

        while self.pending:
            topic, key, callback = self.pending.pop(0)

            if self.mode == "delivery_error":
                callback(
                    RuntimeError("simulated delivery failure"),
                    None,
                )
            else:
                callback(
                    None,
                    FakeMessage(topic, key),
                )

        return 0


def boundary(**changes):
    settings = {
        "historical_ids": frozenset({
            "legacy-001",
            "new:historical-collision",
        }),
        "approved_new_namespace": "new:",
        "boundary_verified": True,
        "live_activation_approved": False,
    }
    settings.update(changes)
    return AdmissionBoundary(**settings)


def send(adapter, event_id="new:event-001"):
    adapter.publish(
        "agent.transactions",
        event_id,
        b"canonical-content",
        b"business-key",
        b"encoded-avro-payload",
    )


def test_successful_durable_delivery(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        producer = FakeProducer()
        adapter = GuardedDeliveryAdapter(
            j, producer, boundary(),
            offline_test_mode=True,
        )

        send(adapter)
        result = adapter.finish()

        assert len(result) == 1
        assert j.state("new:event-001") == "CONFIRMED"
        assert producer.produce_count == 1


def test_historical_event_quarantined(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        producer = FakeProducer()
        adapter = GuardedDeliveryAdapter(
            j, producer, boundary(),
            offline_test_mode=True,
        )

        with pytest.raises(GuardedDeliveryError):
            send(adapter, "legacy-001")

        assert producer.produce_count == 0


def test_historical_namespace_collision_blocked(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        producer = FakeProducer()
        adapter = GuardedDeliveryAdapter(
            j, producer, boundary(),
            offline_test_mode=True,
        )

        with pytest.raises(GuardedDeliveryError):
            send(adapter, "new:historical-collision")

        assert producer.produce_count == 0


def test_unverified_boundary_blocked(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        with pytest.raises(
            GuardedDeliveryError,
            match="unverified",
        ):
            GuardedDeliveryAdapter(
                j,
                FakeProducer(),
                boundary(boundary_verified=False),
                offline_test_mode=True,
            )


def test_live_mode_blocked(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        with pytest.raises(
            GuardedDeliveryError,
            match="live integration disabled",
        ):
            GuardedDeliveryAdapter(
                j, FakeProducer(), boundary()
            )


def test_approval_flag_not_accepted_offline(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        with pytest.raises(
            GuardedDeliveryError,
            match="live approval",
        ):
            GuardedDeliveryAdapter(
                j,
                FakeProducer(),
                boundary(live_activation_approved=True),
                offline_test_mode=True,
            )


def test_duplicate_identity_blocked(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        producer = FakeProducer()
        adapter = GuardedDeliveryAdapter(
            j, producer, boundary(),
            offline_test_mode=True,
        )

        send(adapter)
        adapter.finish()

        with pytest.raises(RecoveryBlocked):
            send(adapter)

        assert producer.produce_count == 1


def test_incomplete_flush_remains_pending(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        adapter = GuardedDeliveryAdapter(
            j,
            FakeProducer("incomplete"),
            boundary(),
            offline_test_mode=True,
        )

        send(adapter)

        with pytest.raises(Exception):
            adapter.finish()

        assert j.state("new:event-001") == "PENDING"


def test_producer_exception_remains_pending(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        adapter = GuardedDeliveryAdapter(
            j,
            FakeProducer("produce_error"),
            boundary(),
            offline_test_mode=True,
        )

        with pytest.raises(RuntimeError):
            send(adapter)

        assert j.state("new:event-001") == "PENDING"


def test_delivery_error_not_confirmed(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        adapter = GuardedDeliveryAdapter(
            j,
            FakeProducer("delivery_error"),
            boundary(),
            offline_test_mode=True,
        )

        send(adapter)

        with pytest.raises(Exception):
            adapter.finish()

        assert j.state("new:event-001") != "CONFIRMED"


def test_invalid_wire_rejected_before_producer(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as j:
        producer = FakeProducer()
        adapter = GuardedDeliveryAdapter(
            j, producer, boundary(),
            offline_test_mode=True,
        )

        with pytest.raises(GuardedDeliveryError):
            adapter.publish(
                "agent.transactions",
                "new:event-001",
                b"content",
                b"key",
                b"",
            )

        assert producer.produce_count == 0


def test_restart_blocks_unresolved_delivery(tmp_path):
    path = tmp_path / "journal.sqlite"

    with DeliveryJournal(path) as j:
        adapter = GuardedDeliveryAdapter(
            j,
            FakeProducer("incomplete"),
            boundary(),
            offline_test_mode=True,
        )
        send(adapter)

    with DeliveryJournal(path) as j:
        with pytest.raises(RecoveryBlocked):
            GuardedDeliveryAdapter(
                j,
                FakeProducer(),
                boundary(),
                offline_test_mode=True,
            )
