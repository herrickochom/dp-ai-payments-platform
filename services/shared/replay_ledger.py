"""Gate 3.6B metadata-only durable replay ledger.

POC implementation:
- SQLite persistence supplied explicitly by the caller.
- No Kafka or infrastructure connectivity.
- No replay execution capability.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional


class ReplayStatus(str, Enum):
    REQUESTED = "REQUESTED"
    APPROVED = "APPROVED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SUPPRESSED_DUPLICATE = "SUPPRESSED_DUPLICATE"
    BLOCKED_POLICY = "BLOCKED_POLICY"


ALLOWED_TRANSITIONS = {
    ReplayStatus.REQUESTED: {
        ReplayStatus.APPROVED,
        ReplayStatus.BLOCKED_POLICY,
        ReplayStatus.SUPPRESSED_DUPLICATE,
    },
    ReplayStatus.APPROVED: {
        ReplayStatus.IN_PROGRESS,
        ReplayStatus.BLOCKED_POLICY,
    },
    ReplayStatus.IN_PROGRESS: {
        ReplayStatus.SUCCEEDED,
        ReplayStatus.FAILED,
    },
    ReplayStatus.FAILED: set(),
    ReplayStatus.SUCCEEDED: set(),
    ReplayStatus.SUPPRESSED_DUPLICATE: set(),
    ReplayStatus.BLOCKED_POLICY: set(),
}


PROHIBITED_FIELDS = {
    "payload",
    "raw_payload",
    "encoded_payload",
    "message_value",
    "kafka_key",
    "beneficiary_id",
    "beneficiary_token",
    "nin",
    "account_number",
    "wallet_number",
    "exception",
    "exception_text",
    "traceback",
}


@dataclass(frozen=True)
class ReplayRequest:
    replay_batch_id: str
    replay_request_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    target_topic: str
    replay_reason_code: str
    requested_by_service_or_role: str
    approved: bool
    policy_resolved: bool
    source_event_reference: Optional[str] = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_replay_identity(
    source_topic: str,
    source_partition: int,
    source_offset: int,
    target_topic: str,
) -> str:
    canonical = json.dumps(
        {
            "source_topic": source_topic,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "target_topic": target_topic,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_request(request: ReplayRequest) -> None:
    if not request.replay_request_id:
        raise ValueError("missing replay_request_id")
    if not request.replay_batch_id:
        raise ValueError("missing replay_batch_id")
    if not request.source_topic:
        raise ValueError("missing source_topic")
    if request.source_partition is None:
        raise ValueError("missing source_partition")
    if request.source_offset is None:
        raise ValueError("missing source_offset")
    if not request.target_topic:
        raise ValueError("missing target_topic")
    if not request.policy_resolved:
        raise PermissionError("policy unresolved")
    if not request.approved:
        raise PermissionError("approval absent")
    if not request.replay_reason_code:
        raise ValueError("missing replay_reason_code")
    if not request.requested_by_service_or_role:
        raise ValueError("missing requested_by_service_or_role")


def reject_prohibited_metadata(metadata: dict) -> None:
    prohibited = {
        str(key).lower()
        for key in metadata
        if str(key).lower() in PROHIBITED_FIELDS
    }
    if prohibited:
        raise ValueError(
            "prohibited replay metadata fields supplied: "
            + ",".join(sorted(prohibited))
        )


class ReplayLedger:
    """SQLite-backed metadata-only replay-control ledger."""

    def __init__(self, database_path: str | Path):
        if not database_path:
            raise ValueError("explicit database path required")

        self.database_path = Path(database_path)

        try:
            self.connection = sqlite3.connect(
                str(self.database_path),
                timeout=5.0,
                isolation_level=None,
            )
            self.connection.row_factory = sqlite3.Row
            self._initialise()
        except sqlite3.Error as exc:
            raise RuntimeError("durable replay ledger unavailable") from exc

    def _initialise(self) -> None:
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS replay_attempts (
                attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
                replay_identity TEXT NOT NULL,
                replay_request_id TEXT NOT NULL,
                replay_batch_id TEXT NOT NULL,
                attempted_at TEXT NOT NULL,
                attempt_status TEXT NOT NULL,
                result_code TEXT
            )
            """
        )

        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS replay_ledger (
                replay_identity TEXT PRIMARY KEY,
                replay_batch_id TEXT NOT NULL,
                replay_request_id TEXT NOT NULL UNIQUE,
                source_topic TEXT NOT NULL,
                source_partition INTEGER NOT NULL,
                source_offset INTEGER NOT NULL,
                source_event_reference TEXT,
                target_topic TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                replay_status TEXT NOT NULL,
                replay_reason_code TEXT NOT NULL,
                requested_by_service_or_role TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                previous_replay_reference TEXT,
                result_code TEXT
            )
            """
        )

    def close(self) -> None:
        self.connection.close()

    def reserve(
        self,
        request: ReplayRequest,
        extra_metadata: Optional[dict] = None,
    ) -> str:
        validate_request(request)
        reject_prohibited_metadata(extra_metadata or {})

        identity = canonical_replay_identity(
            request.source_topic,
            request.source_partition,
            request.source_offset,
            request.target_topic,
        )

        try:
            self.connection.execute("BEGIN IMMEDIATE")

            existing = self.connection.execute(
                """
                SELECT replay_status
                FROM replay_ledger
                WHERE replay_identity = ?
                """,
                (identity,),
            ).fetchone()

            if existing is not None:
                self.connection.execute(
                    """
                    INSERT INTO replay_attempts (
                        replay_identity,
                        replay_request_id,
                        replay_batch_id,
                        attempted_at,
                        attempt_status,
                        result_code
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        identity,
                        request.replay_request_id,
                        request.replay_batch_id,
                        utc_now(),
                        ReplayStatus.SUPPRESSED_DUPLICATE.value,
                        "PRIOR_RESERVATION_EXISTS",
                    ),
                )
                self.connection.execute("COMMIT")
                raise RuntimeError("duplicate replay suppressed")

            self.connection.execute(
                """
                INSERT INTO replay_ledger (
                    replay_identity,
                    replay_batch_id,
                    replay_request_id,
                    source_topic,
                    source_partition,
                    source_offset,
                    source_event_reference,
                    target_topic,
                    requested_at,
                    replay_status,
                    replay_reason_code,
                    requested_by_service_or_role,
                    attempt_number
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    identity,
                    request.replay_batch_id,
                    request.replay_request_id,
                    request.source_topic,
                    request.source_partition,
                    request.source_offset,
                    request.source_event_reference,
                    request.target_topic,
                    utc_now(),
                    ReplayStatus.REQUESTED.value,
                    request.replay_reason_code,
                    request.requested_by_service_or_role,
                    1,
                ),
            )

            self.connection.execute(
                """
                INSERT INTO replay_attempts (
                    replay_identity,
                    replay_request_id,
                    replay_batch_id,
                    attempted_at,
                    attempt_status,
                    result_code
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    identity,
                    request.replay_request_id,
                    request.replay_batch_id,
                    utc_now(),
                    ReplayStatus.REQUESTED.value,
                    "RESERVED",
                ),
            )

            self.connection.execute("COMMIT")
            return identity

        except sqlite3.IntegrityError as exc:
            try:
                self.connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise RuntimeError("duplicate replay suppressed") from exc

        except Exception:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def transition(
        self,
        replay_identity: str,
        new_status: ReplayStatus,
        result_code: Optional[str] = None,
    ) -> None:
        self.connection.execute("BEGIN IMMEDIATE")

        try:
            row = self.connection.execute(
                """
                SELECT replay_status
                FROM replay_ledger
                WHERE replay_identity = ?
                """,
                (replay_identity,),
            ).fetchone()

            if row is None:
                raise KeyError("replay identity not found")

            current = ReplayStatus(row["replay_status"])

            if new_status not in ALLOWED_TRANSITIONS[current]:
                raise ValueError(
                    f"invalid replay transition: {current.value}->{new_status.value}"
                )

            started_at = None
            completed_at = None

            if new_status == ReplayStatus.IN_PROGRESS:
                started_at = utc_now()

            if new_status in {
                ReplayStatus.SUCCEEDED,
                ReplayStatus.FAILED,
                ReplayStatus.SUPPRESSED_DUPLICATE,
                ReplayStatus.BLOCKED_POLICY,
            }:
                completed_at = utc_now()

            self.connection.execute(
                """
                UPDATE replay_ledger
                SET replay_status = ?,
                    started_at = COALESCE(?, started_at),
                    completed_at = COALESCE(?, completed_at),
                    result_code = ?
                WHERE replay_identity = ?
                """,
                (
                    new_status.value,
                    started_at,
                    completed_at,
                    result_code,
                    replay_identity,
                ),
            )

            self.connection.execute("COMMIT")

        except Exception:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def attempts(self, replay_identity: str) -> list[dict]:
        rows = self.connection.execute(
            """
            SELECT *
            FROM replay_attempts
            WHERE replay_identity = ?
            ORDER BY attempt_id
            """,
            (replay_identity,),
        ).fetchall()
        return [dict(row) for row in rows]

    def get(self, replay_identity: str) -> dict:
        row = self.connection.execute(
            """
            SELECT *
            FROM replay_ledger
            WHERE replay_identity = ?
            """,
            (replay_identity,),
        ).fetchone()

        if row is None:
            raise KeyError("replay identity not found")

        return dict(row)
