"""Privacy-safe payment source quarantine contracts.

No persistence or network operations are performed by this module.
"""

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


class SourceQuarantineError(ValueError):
    """Invalid source quarantine identity or metadata."""


ALLOWED_FAILURE_CATEGORIES = frozenset({
    "INVALID_JSON",
    "INVALID_RECORD",
    "MISSING_EVENT_ID",
    "INVALID_SNAPSHOT",
    "CONFLICTING_SNAPSHOT",
    "UNKNOWN_TOPIC",
})


@dataclass(frozen=True)
class SourceIdentity:
    source_id: str
    content_sha256: str


def source_identity(
    source_path: str,
    data_root: str,
    content: bytes,
) -> SourceIdentity:
    """Identify one source version without persisting its path or contents."""
    if not isinstance(content, bytes):
        raise SourceQuarantineError("source content must be bytes")

    root = Path(data_root).resolve()
    path = Path(source_path).resolve()

    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise SourceQuarantineError(
            "source path outside configured data root"
        ) from exc

    if relative in ("", "."):
        raise SourceQuarantineError("invalid source path")

    digest = hashlib.sha256(content).hexdigest()
    coordinate = json.dumps(
        [relative, digest],
        separators=(",", ":"),
    ).encode("utf-8")

    return SourceIdentity(
        source_id=hashlib.sha256(coordinate).hexdigest(),
        content_sha256=digest,
    )


def failure_metadata(
    identity: SourceIdentity,
    category: str,
) -> dict:
    """Return an allowlisted, privacy-safe quarantine record."""
    if category not in ALLOWED_FAILURE_CATEGORIES:
        raise SourceQuarantineError("unsupported failure category")

    for value in (identity.source_id, identity.content_sha256):
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
        ):
            raise SourceQuarantineError("invalid source identity")

    return {
        "contract_version": 1,
        "source_id": identity.source_id,
        "content_sha256": identity.content_sha256,
        "failure_category": category,
    }


def validate_failure_metadata(metadata: dict) -> None:
    """Reject additional fields, including raw records and exception text."""
    if not isinstance(metadata, dict):
        raise SourceQuarantineError("metadata must be an object")

    expected = {
        "contract_version",
        "source_id",
        "content_sha256",
        "failure_category",
    }

    if set(metadata) != expected:
        raise SourceQuarantineError("unexpected quarantine metadata fields")

    if type(metadata["contract_version"]) is not int or metadata["contract_version"] != 1:
        raise SourceQuarantineError("unsupported contract version")

    failure_metadata(
        SourceIdentity(
            source_id=metadata["source_id"],
            content_sha256=metadata["content_sha256"],
        ),
        metadata["failure_category"],
    )



class SourceQuarantinePersistenceError(RuntimeError):
    """Durable quarantine persistence could not be verified."""


class S3SourceQuarantineStore:
    """Restricted, idempotent S3-compatible quarantine adapter."""

    PREFIX = "restricted/payment-source-quarantine/v1"

    def __init__(self, client, bucket: str):
        if not isinstance(bucket, str) or not bucket.strip():
            raise ValueError("quarantine bucket is required")
        self.client = client
        self.bucket = bucket

    def persist(self, metadata: dict) -> str:
        validate_failure_metadata(metadata)

        key = (
            f"{self.PREFIX}/"
            f"source_id={metadata['source_id']}/metadata.json"
        )

        payload = json.dumps(
            metadata,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=payload,
                ContentType="application/json",
                IfNoneMatch="*",
            )
        except Exception as exc:
            if not self._is_precondition_failed(exc):
                raise SourceQuarantinePersistenceError(
                    "conditional quarantine creation failed"
                ) from exc

        try:
            response = self.client.get_object(
                Bucket=self.bucket,
                Key=key,
            )
            stored = self._read_body(response)
        except Exception as exc:
            raise SourceQuarantinePersistenceError(
                "quarantine verification failed"
            ) from exc

        if stored != payload:
            raise SourceQuarantinePersistenceError(
                "quarantine identity collision or read-back mismatch"
            )

        return key

    @staticmethod
    def _is_precondition_failed(exc):
        response = getattr(exc, "response", None)
        if not isinstance(response, dict):
            return False

        code = response.get("Error", {}).get("Code")
        return code in {"412", "PreconditionFailed"}

    @staticmethod
    def _read_body(response):
        try:
            return response["Body"].read()
        except Exception as exc:
            raise SourceQuarantinePersistenceError(
                "quarantine object read failed"
            ) from exc

    @staticmethod
    def _is_missing(exc):
        response = getattr(exc, "response", None)
        if not isinstance(response, dict):
            return False

        code = response.get("Error", {}).get("Code")
        return code in {"404", "NoSuchKey", "NotFound"}


class QuarantineStore(Protocol):
    """Durable adapter contract; implemented in a later increment."""

    def persist(self, metadata: dict) -> str:
        """Persist idempotently and return a durable reference."""
        ...
