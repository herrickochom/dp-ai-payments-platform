from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class CDCEnvelopeError(ValueError):
    """Raised when a Debezium record cannot satisfy the CDC contract."""


OPERATIONS = {
    "c": "INSERT",
    "u": "UPDATE",
    "d": "DELETE",
    "r": "SNAPSHOT_READ",
}


def _iso_from_epoch_ms(
    value: int | None,
) -> str:
    if value is None:
        raise CDCEnvelopeError(
            "Debezium event timestamp is required"
        )

    return datetime.fromtimestamp(
        value / 1000,
        tz=timezone.utc,
    ).isoformat().replace("+00:00", "Z")


def _source_position(
    source: dict[str, Any],
) -> str:
    """
    Preserve an opaque database position.

    PostgreSQL Debezium normally exposes an LSN. Other connector
    families may expose a different position in future.
    """
    lsn = source.get("lsn")

    if lsn is not None:
        return str(lsn)

    sequence = source.get("sequence")

    if sequence is not None:
        return str(sequence)

    raise CDCEnvelopeError(
        "CDC source position is required"
    )


def _record_key(
    key: Any,
) -> Any:
    if key is None:
        raise CDCEnvelopeError(
            "CDC record key is required"
        )

    return key


def normalize_debezium_event(
    *,
    source_system: str,
    key: Any,
    value: dict[str, Any] | None,
    ingestion_timestamp: str,
    schema_version: str = "1",
) -> dict[str, Any]:
    """
    Convert a Debezium change payload into the platform CDC Raw
    envelope.

    Kafka tombstones are represented separately because their value
    is null and therefore contains no Debezium source envelope.
    """
    if value is None:
        return {
            "record_type": "CDC_TOMBSTONE",
            "source_system": source_system,
            "record_key": _record_key(key),
            "ingestion_timestamp": ingestion_timestamp,
            "schema_version": schema_version,
            "tombstone": True,
        }

    payload = value.get("payload", value)

    if not isinstance(payload, dict):
        raise CDCEnvelopeError(
            "Debezium payload must be an object"
        )

    op = payload.get("op")

    if op not in OPERATIONS:
        raise CDCEnvelopeError(
            f"Unsupported CDC operation: {op!r}"
        )

    source = payload.get("source")

    if not isinstance(source, dict):
        raise CDCEnvelopeError(
            "Debezium source metadata is required"
        )

    schema = (
        source.get("schema")
        or source.get("schema_name")
    )

    table = source.get("table")

    if not schema:
        raise CDCEnvelopeError(
            "CDC source schema is required"
        )

    if not table:
        raise CDCEnvelopeError(
            "CDC source table is required"
        )

    event_ms = (
        payload.get("ts_ms")
        if payload.get("ts_ms") is not None
        else source.get("ts_ms")
    )

    transaction = payload.get("transaction")

    transaction_id = None

    if isinstance(transaction, dict):
        transaction_id = (
            transaction.get("id")
            or transaction.get("transaction_id")
        )

    envelope = {
        "record_type": "CDC_CHANGE_EVENT",
        "source_system": source_system,
        "source_schema": str(schema),
        "source_table": str(table),
        "operation": op,
        "operation_name": OPERATIONS[op],
        "source_position": _source_position(source),
        "source_transaction": transaction_id,
        "event_timestamp": _iso_from_epoch_ms(
            event_ms
        ),
        "ingestion_timestamp": ingestion_timestamp,
        "schema_version": schema_version,
        "record_key": _record_key(key),
        "before": payload.get("before"),
        "after": payload.get("after"),
        "snapshot": source.get("snapshot"),
        "tombstone": False,
    }

    if op == "c" and envelope["after"] is None:
        raise CDCEnvelopeError(
            "INSERT requires after state"
        )

    if op == "u":
        if envelope["after"] is None:
            raise CDCEnvelopeError(
                "UPDATE requires after state"
            )

    if op == "d":
        if envelope["before"] is None:
            raise CDCEnvelopeError(
                "DELETE requires before state"
            )

    if op == "r" and envelope["after"] is None:
        raise CDCEnvelopeError(
            "SNAPSHOT_READ requires after state"
        )

    return envelope
