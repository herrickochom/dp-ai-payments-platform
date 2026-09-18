import pytest

from services.shared.cdc.debezium_normalizer import (
    CDCEnvelopeError,
    normalize_debezium_event,
)


INGESTED = "2026-09-18T12:00:00Z"


def event(
    op,
    *,
    before=None,
    after=None,
    lsn=123456,
    transaction=None,
):
    return {
        "payload": {
            "before": before,
            "after": after,
            "source": {
                "schema": "public",
                "table": "beneficiary",
                "lsn": lsn,
                "ts_ms": 1789732800000,
                "snapshot": "false",
            },
            "op": op,
            "ts_ms": 1789732801000,
            "transaction": transaction,
        }
    }


def normalize(value, key=None):
    return normalize_debezium_event(
        source_system="future_pdmis_db",
        key=key or {"beneficiary_id": "B-001"},
        value=value,
        ingestion_timestamp=INGESTED,
    )


def test_insert():
    row = normalize(
        event(
            "c",
            after={"beneficiary_id": "B-001"},
        )
    )

    assert row["operation"] == "c"
    assert row["operation_name"] == "INSERT"
    assert row["source_position"] == "123456"
    assert row["after"]["beneficiary_id"] == "B-001"


def test_update_preserves_before_after():
    row = normalize(
        event(
            "u",
            before={"status": "OLD"},
            after={"status": "NEW"},
        )
    )

    assert row["operation_name"] == "UPDATE"
    assert row["before"]["status"] == "OLD"
    assert row["after"]["status"] == "NEW"


def test_delete_preserves_before():
    row = normalize(
        event(
            "d",
            before={"beneficiary_id": "B-001"},
        )
    )

    assert row["operation_name"] == "DELETE"
    assert row["before"] is not None
    assert row["after"] is None


def test_snapshot_read():
    row = normalize(
        event(
            "r",
            after={"beneficiary_id": "B-001"},
        )
    )

    assert row["operation_name"] == "SNAPSHOT_READ"


def test_transaction_preserved():
    row = normalize(
        event(
            "u",
            before={"status": "A"},
            after={"status": "B"},
            transaction={"id": "tx-77"},
        )
    )

    assert row["source_transaction"] == "tx-77"


def test_tombstone_recognised():
    row = normalize(None)

    assert row["record_type"] == "CDC_TOMBSTONE"
    assert row["tombstone"] is True
    assert row["record_key"] == {
        "beneficiary_id": "B-001"
    }


def test_missing_key_fails():
    with pytest.raises(
        CDCEnvelopeError,
        match="record key",
    ):
        normalize_debezium_event(
            source_system="future_pdmis_db",
            key=None,
            value=event(
                "c",
                after={"x": 1},
            ),
            ingestion_timestamp=INGESTED,
        )


def test_missing_position_fails():
    with pytest.raises(
        CDCEnvelopeError,
        match="source position",
    ):
        normalize(
            event(
                "c",
                after={"x": 1},
                lsn=None,
            )
        )


def test_unknown_operation_fails():
    with pytest.raises(
        CDCEnvelopeError,
        match="Unsupported",
    ):
        normalize(
            event(
                "x",
                after={"x": 1},
            )
        )


def test_delete_without_before_fails():
    with pytest.raises(
        CDCEnvelopeError,
        match="DELETE requires before",
    ):
        normalize(
            event(
                "d",
                before=None,
            )
        )
