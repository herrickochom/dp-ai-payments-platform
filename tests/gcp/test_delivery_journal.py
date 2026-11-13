import importlib.util
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "services/payment-producer/delivery_journal.py"

spec = importlib.util.spec_from_file_location(
    "delivery_journal", MODULE
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

DeliveryJournal = mod.DeliveryJournal
RecoveryBlocked = mod.RecoveryBlocked


def test_confirmed_delivery_survives_restart(tmp_path):
    path = tmp_path / "journal.sqlite"

    with DeliveryJournal(path) as journal:
        journal.begin("event-1", "agent.transactions")
        journal.confirm("event-1", "agent.transactions", 2, 100)

    with DeliveryJournal(path) as journal:
        assert journal.state("event-1") == "CONFIRMED"
        journal.assert_recovery_safe()


def test_pending_delivery_blocks_restart(tmp_path):
    path = tmp_path / "journal.sqlite"

    with DeliveryJournal(path) as journal:
        journal.begin("event-2", "pdmis.loans")

    with DeliveryJournal(path) as journal:
        with pytest.raises(RecoveryBlocked):
            journal.assert_recovery_safe()


def test_uncertain_delivery_blocks_replay(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as journal:
        journal.begin("event-3", "pdmis.repayments")
        journal.mark_uncertain("event-3")

        assert journal.state("event-3") == "UNCERTAIN"

        with pytest.raises(RecoveryBlocked):
            journal.assert_recovery_safe()


def test_duplicate_event_is_rejected(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as journal:
        journal.begin("event-4", "agent.transactions")

        with pytest.raises(RecoveryBlocked):
            journal.begin("event-4", "agent.transactions")


def test_confirmed_delivery_cannot_be_rewritten(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as journal:
        journal.begin("event-5", "agent.transactions")
        journal.confirm("event-5", "agent.transactions", 0, 10)

        with pytest.raises(RecoveryBlocked):
            journal.confirm("event-5", "agent.transactions", 0, 11)


def test_wrong_topic_cannot_be_confirmed(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as journal:
        journal.begin("event-6", "agent.transactions")

        with pytest.raises(RecoveryBlocked):
            journal.confirm("event-6", "pdmis.loans", 0, 10)


def test_invalid_offset_rejected(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as journal:
        journal.begin("event-7", "agent.transactions")

        with pytest.raises(ValueError):
            journal.confirm("event-7", "agent.transactions", 0, -1)


def test_sqlite_uses_full_synchronous_mode(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as journal:
        result = journal.db.execute(
            "PRAGMA synchronous"
        ).fetchone()[0]
        assert result == 2


def test_schema_rejects_invalid_state(tmp_path):
    with DeliveryJournal(tmp_path / "journal.sqlite") as journal:
        with pytest.raises(sqlite3.IntegrityError):
            journal.db.execute(
                """INSERT INTO deliveries(event_id, topic, state)
                   VALUES ('bad', 'agent.transactions', 'UNKNOWN')"""
            )
