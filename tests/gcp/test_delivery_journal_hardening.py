import importlib.util
import multiprocessing
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "services/payment-producer/delivery_journal.py"

spec = importlib.util.spec_from_file_location(
    "delivery_journal_hardened", MODULE
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

DeliveryJournal = mod.DeliveryJournal
RecoveryBlocked = mod.RecoveryBlocked


def test_fingerprint_is_deterministic():
    assert (
        DeliveryJournal.fingerprint(b"payment")
        == DeliveryJournal.fingerprint(b"payment")
    )


def test_different_content_has_different_fingerprint():
    assert (
        DeliveryJournal.fingerprint(b"payment-a")
        != DeliveryJournal.fingerprint(b"payment-b")
    )


def test_content_conflict_blocks_reuse(tmp_path):
    path = tmp_path / "journal.sqlite"

    with DeliveryJournal(path) as journal:
        journal.begin(
            "event-1",
            "agent.transactions",
            journal.fingerprint(b"original"),
        )

        with pytest.raises(RecoveryBlocked):
            journal.begin(
                "event-1",
                "agent.transactions",
                journal.fingerprint(b"changed"),
            )


def test_confirmed_event_survives_restart(tmp_path):
    path = tmp_path / "journal.sqlite"

    with DeliveryJournal(path) as journal:
        journal.begin(
            "event-2",
            "agent.transactions",
            journal.fingerprint(b"payment"),
        )
        journal.confirm(
            "event-2", "agent.transactions", 0, 100
        )

    with DeliveryJournal(path) as journal:
        assert journal.state("event-2") == "CONFIRMED"
        journal.assert_recovery_safe()


def test_pending_blocks_recovery(tmp_path):
    path = tmp_path / "journal.sqlite"

    with DeliveryJournal(path) as journal:
        journal.begin("event-3", "agent.transactions")

    with DeliveryJournal(path) as journal:
        with pytest.raises(RecoveryBlocked):
            journal.assert_recovery_safe()


def test_uncertain_blocks_recovery(tmp_path):
    with DeliveryJournal(
        tmp_path / "journal.sqlite"
    ) as journal:
        journal.begin("event-4", "agent.transactions")
        journal.mark_uncertain("event-4")

        with pytest.raises(RecoveryBlocked):
            journal.assert_recovery_safe()


def test_exclusive_writer_lock(tmp_path):
    path = tmp_path / "journal.sqlite"

    with DeliveryJournal(path):
        with pytest.raises(RecoveryBlocked):
            DeliveryJournal(path)


def test_lock_released_after_close(tmp_path):
    path = tmp_path / "journal.sqlite"

    with DeliveryJournal(path):
        pass

    with DeliveryJournal(path) as journal:
        assert journal.counts() == {}


def test_invalid_fingerprint_rejected(tmp_path):
    with DeliveryJournal(
        tmp_path / "journal.sqlite"
    ) as journal:
        with pytest.raises(ValueError):
            journal.begin(
                "event-5",
                "agent.transactions",
                "not-a-sha256",
            )


def test_wrong_topic_rejected(tmp_path):
    with DeliveryJournal(
        tmp_path / "journal.sqlite"
    ) as journal:
        journal.begin("event-6", "agent.transactions")

        with pytest.raises(RecoveryBlocked):
            journal.confirm(
                "event-6", "pdmis.loans", 0, 10
            )


def test_confirmed_delivery_cannot_be_rewritten(tmp_path):
    with DeliveryJournal(
        tmp_path / "journal.sqlite"
    ) as journal:
        journal.begin("event-7", "agent.transactions")
        journal.confirm(
            "event-7", "agent.transactions", 0, 10
        )

        with pytest.raises(RecoveryBlocked):
            journal.confirm(
                "event-7", "agent.transactions", 0, 11
            )


def test_invalid_offset_rejected(tmp_path):
    with DeliveryJournal(
        tmp_path / "journal.sqlite"
    ) as journal:
        journal.begin("event-8", "agent.transactions")

        with pytest.raises(ValueError):
            journal.confirm(
                "event-8", "agent.transactions", 0, -1
            )


def test_full_synchronous_mode(tmp_path):
    with DeliveryJournal(
        tmp_path / "journal.sqlite"
    ) as journal:
        value = journal.db.execute(
            "PRAGMA synchronous"
        ).fetchone()[0]

        assert value == 2


def test_existing_schema_is_migrated(tmp_path):
    path = tmp_path / "journal.sqlite"

    connection = sqlite3.connect(path)
    connection.execute("""
        CREATE TABLE deliveries (
            event_id TEXT PRIMARY KEY,
            topic TEXT NOT NULL,
            state TEXT NOT NULL,
            partition_id INTEGER,
            offset_id INTEGER
        )
    """)
    connection.execute(
        """
        INSERT INTO deliveries
        VALUES ('old-event', 'agent.transactions',
                'CONFIRMED', 0, 15)
        """
    )
    connection.commit()
    connection.close()

    with DeliveryJournal(path) as journal:
        assert journal.state("old-event") == "CONFIRMED"

        columns = {
            row[1]
            for row in journal.db.execute(
                "PRAGMA table_info(deliveries)"
            )
        }

        assert "content_sha256" in columns


def test_legacy_record_cannot_be_silently_reused(tmp_path):
    with DeliveryJournal(
        tmp_path / "journal.sqlite"
    ) as journal:
        journal.begin("event-9", "agent.transactions")

        with pytest.raises(RecoveryBlocked):
            journal.begin(
                "event-9",
                "agent.transactions",
                journal.fingerprint(b"new-content"),
            )
