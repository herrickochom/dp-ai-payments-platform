"""Durable SQLite delivery journal with exclusive-writer protection."""

import fcntl
import hashlib
import os
import sqlite3
from pathlib import Path


class RecoveryBlocked(RuntimeError):
    pass


class DeliveryJournal:
    def __init__(self, path):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self._lock_fd = None
        self.db = None

        lock_path = str(self.path) + ".lock"

        try:
            self._lock_fd = os.open(
                lock_path,
                os.O_CREAT | os.O_RDWR,
                0o600,
            )

            try:
                fcntl.flock(
                    self._lock_fd,
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
            except BlockingIOError as exc:
                raise RecoveryBlocked(
                    "Another delivery-journal writer holds the lock"
                ) from exc

            self.db = sqlite3.connect(
                str(self.path),
                timeout=30,
                isolation_level=None,
            )

            self.db.execute("PRAGMA busy_timeout=30000")
            self.db.execute("PRAGMA synchronous=FULL")
            self.db.execute("PRAGMA journal_mode=WAL")

            self.db.execute("""
                CREATE TABLE IF NOT EXISTS deliveries (
                    event_id TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (
                        state IN (
                            'PENDING',
                            'CONFIRMED',
                            'UNCERTAIN'
                        )
                    ),
                    partition_id INTEGER,
                    offset_id INTEGER,
                    CHECK (
                        state != 'CONFIRMED'
                        OR (
                            partition_id IS NOT NULL
                            AND offset_id IS NOT NULL
                        )
                    )
                )
            """)

            columns = {
                row[1]
                for row in self.db.execute(
                    "PRAGMA table_info(deliveries)"
                )
            }

            if "content_sha256" not in columns:
                self.db.execute(
                    "ALTER TABLE deliveries "
                    "ADD COLUMN content_sha256 TEXT"
                )

        except BaseException:
            self.close()
            raise

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None

        if self._lock_fd is not None:
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
            self._lock_fd = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @staticmethod
    def fingerprint(content):
        if not isinstance(content, bytes):
            raise TypeError("content must be bytes")

        return hashlib.sha256(content).hexdigest()

    def state(self, event_id):
        row = self.db.execute(
            "SELECT state FROM deliveries WHERE event_id=?",
            (event_id,),
        ).fetchone()

        return row[0] if row else None

    def begin(self, event_id, topic, content_sha256=None):
        if not event_id or not topic:
            raise ValueError("event_id and topic required")

        if content_sha256 is not None:
            if (
                not isinstance(content_sha256, str)
                or len(content_sha256) != 64
                or any(
                    c not in "0123456789abcdef"
                    for c in content_sha256
                )
            ):
                raise ValueError("Invalid SHA-256 fingerprint")

        self.db.execute("BEGIN IMMEDIATE")

        try:
            existing = self.db.execute(
                """
                SELECT topic, state, content_sha256
                FROM deliveries
                WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()

            if existing:
                old_topic, old_state, old_hash = existing

                if old_topic != topic:
                    raise RecoveryBlocked(
                        f"Event ID topic conflict: {event_id}"
                    )

                if (
                    content_sha256 is not None
                    and old_hash is not None
                    and content_sha256 != old_hash
                ):
                    raise RecoveryBlocked(
                        f"Source-content conflict: {event_id}"
                    )

                if old_hash is None or content_sha256 is None:
                    raise RecoveryBlocked(
                        f"Event already recorded; fingerprint "
                        f"verification incomplete: {event_id}; "
                        f"state={old_state}"
                    )

                raise RecoveryBlocked(
                    f"Event already recorded: {event_id}; "
                    f"state={old_state}"
                )

            self.db.execute(
                """
                INSERT INTO deliveries(
                    event_id,
                    topic,
                    state,
                    content_sha256
                )
                VALUES (?, ?, 'PENDING', ?)
                """,
                (event_id, topic, content_sha256),
            )

            self.db.execute("COMMIT")

        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def confirm(self, event_id, topic, partition, offset):
        if (
            not isinstance(partition, int)
            or not isinstance(offset, int)
            or partition < 0
            or offset < 0
        ):
            raise ValueError("Invalid Kafka partition or offset")

        self.db.execute("BEGIN IMMEDIATE")

        try:
            row = self.db.execute(
                """
                SELECT topic, state
                FROM deliveries
                WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()

            if row != (topic, "PENDING"):
                raise RecoveryBlocked(
                    f"Cannot confirm delivery: {event_id}; "
                    f"record={row}"
                )

            self.db.execute(
                """
                UPDATE deliveries
                SET state='CONFIRMED',
                    partition_id=?,
                    offset_id=?
                WHERE event_id=?
                """,
                (partition, offset, event_id),
            )

            self.db.execute("COMMIT")

        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def mark_uncertain(self, event_id):
        self.db.execute("BEGIN IMMEDIATE")

        try:
            row = self.db.execute(
                """
                SELECT state
                FROM deliveries
                WHERE event_id=?
                """,
                (event_id,),
            ).fetchone()

            if row != ("PENDING",):
                raise RecoveryBlocked(
                    f"Cannot mark uncertain: {event_id}; "
                    f"record={row}"
                )

            self.db.execute(
                """
                UPDATE deliveries
                SET state='UNCERTAIN'
                WHERE event_id=?
                """,
                (event_id,),
            )

            self.db.execute("COMMIT")

        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def assert_recovery_safe(self):
        rows = self.db.execute(
            """
            SELECT event_id, state
            FROM deliveries
            WHERE state != 'CONFIRMED'
            ORDER BY event_id
            """
        ).fetchall()

        if rows:
            raise RecoveryBlocked(
                f"Recovery blocked: {len(rows)} "
                f"unresolved deliveries; first={rows[0]}"
            )

    def counts(self):
        return dict(
            self.db.execute(
                """
                SELECT state, COUNT(*)
                FROM deliveries
                GROUP BY state
                """
            ).fetchall()
        )
