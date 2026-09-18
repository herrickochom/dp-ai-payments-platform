from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


ALLOWED_FAILURE_FIELDS = frozenset(
    {
        "failure_id",
        "source_system",
        "topic",
        "partition",
        "offset",
        "failure_category",
        "error_class",
        "event_timestamp",
    }
)

PROHIBITED_FAILURE_FIELDS = frozenset(
    {
        "record_key",
        "record_key_json",
        "business_key",
        "before",
        "before_json",
        "after",
        "after_json",
        "payload",
        "original_payload",
        "original_key",
        "source_record",
    }
)


def deterministic_cdc_failure_id(
    *,
    source_system: str,
    topic: str,
    partition: int,
    offset: int,
) -> str:
    """
    Return a deterministic opaque CDC failure identifier.

    No source key, row value, before image or after image participates
    in the identifier.
    """
    material = (
        f"cdc-failure-v1|{source_system}|"
        f"{topic}|{partition}|{offset}"
    ).encode("utf-8")

    return hashlib.sha256(material).hexdigest()


def safe_error_class(error: BaseException) -> str:
    """
    Return only a bounded operational error category.

    Exception text and traceback content are deliberately excluded.
    """
    if isinstance(error, (ValueError, TypeError)):
        return "CDC_VALIDATION_ERROR"

    if isinstance(error, OSError):
        return "CDC_STORAGE_ERROR"

    return "CDC_PROCESSING_ERROR"


def cdc_failure_metadata(
    *,
    source_system: str,
    topic: str,
    partition: int,
    offset: int,
    failure_category: str,
    error: BaseException,
    event_timestamp: str | None = None,
) -> dict[str, Any]:
    """
    Build metadata safe for ordinary operational logging.

    This function intentionally has no argument for record key,
    before image, after image or source payload.
    """
    timestamp = event_timestamp

    if timestamp is None:
        timestamp = (
            datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )

    record = {
        "failure_id": deterministic_cdc_failure_id(
            source_system=source_system,
            topic=topic,
            partition=partition,
            offset=offset,
        ),
        "source_system": source_system,
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "failure_category": failure_category,
        "error_class": safe_error_class(error),
        "event_timestamp": timestamp,
    }

    if set(record) != ALLOWED_FAILURE_FIELDS:
        raise ValueError(
            "CDC failure metadata field contract violated"
        )

    if set(record).intersection(
        PROHIBITED_FAILURE_FIELDS
    ):
        raise ValueError(
            "Restricted CDC content entered failure metadata"
        )

    return record


def validate_safe_failure_metadata(
    record: dict[str, Any],
) -> None:
    """
    Fail closed if operational failure metadata contains anything
    outside the approved field contract.
    """
    fields = set(record)

    prohibited = fields.intersection(
        PROHIBITED_FAILURE_FIELDS
    )

    if prohibited:
        raise ValueError(
            "Restricted CDC failure fields prohibited"
        )

    unexpected = fields.difference(
        ALLOWED_FAILURE_FIELDS
    )

    if unexpected:
        raise ValueError(
            "Unexpected CDC failure metadata fields"
        )

    missing = ALLOWED_FAILURE_FIELDS.difference(
        fields
    )

    if missing:
        raise ValueError(
            "Required CDC failure metadata fields missing"
        )
