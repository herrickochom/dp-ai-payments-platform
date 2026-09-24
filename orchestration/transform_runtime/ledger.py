"""Transactional transform ledger. PostgreSQL is required in deployed environments."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from uuid import uuid4

from services.shared.security.secret_provider import SecretUnavailable, require_secret


TERMINAL = frozenset({"SUCCEEDED", "FAILED", "CANCELLED", "ORPHANED"})
TRANSITIONS = {
    "ADMITTED": {"QUEUED", "CANCELLED"},
    "QUEUED": {"RUNNING", "CANCELLED", "ORPHANED"},
    "RUNNING": {"TESTING", "FAILED", "CANCELLED", "ORPHANED"},
    "TESTING": {"SUCCEEDED", "FAILED", "CANCELLED", "ORPHANED"},
}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


class LedgerError(RuntimeError):
    pass


class TransformLedger:
    """SQLite adapter is for isolated tests; deployment uses the matching PG migration."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.connection = sqlite3.connect(str(path), check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.lock = RLock()
        self.connection.executescript(Path(__file__).with_name("schema.sqlite.sql").read_text())

    @contextmanager
    def transaction(self):
        with self.lock, self.connection:
            yield self.connection

    def ping(self) -> None:
        self.connection.execute("SELECT 1").fetchone()

    def create_run(self, caller: str, key: str, mode: str, contract_digest: str, plan_digest: str):
        with self.transaction() as db:
            row = db.execute(
                "SELECT * FROM transform_runs WHERE caller_identity=? AND idempotency_key=?",
                (caller, key),
            ).fetchone()
            if row:
                immutable = (row["execution_mode"], row["contract_digest"], row["plan_digest"])
                if immutable != (mode, contract_digest, plan_digest):
                    raise LedgerError("idempotency key reused with different semantics")
                return dict(row)
            active = db.execute("SELECT 1 FROM transform_runs WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELLED')").fetchone()
            if active:
                raise LedgerError("one active transform run is already admitted")
            run_id = f"tr_{uuid4().hex}"
            accepted = now()
            db.execute(
                "INSERT INTO transform_runs VALUES (?,?,?,?,?,?,?,?,?)",
                (run_id, caller, key, mode, contract_digest, plan_digest, "ADMITTED", accepted, accepted),
            )
            self._event(db, run_id, None, "RUN_ADMITTED", {"caller": caller})
            return dict(db.execute("SELECT * FROM transform_runs WHERE transform_run_id=?", (run_id,)).fetchone())

    def submit_batch(self, run_id: str, caller: str, key: str, batch, fingerprint: str):
        with self.transaction() as db:
            run = db.execute("SELECT * FROM transform_runs WHERE transform_run_id=? AND caller_identity=?", (run_id, caller)).fetchone()
            if not run:
                raise LedgerError("transform run not found")
            prior = db.execute("SELECT * FROM batch_executions WHERE caller_identity=? AND idempotency_key=?", (caller, key)).fetchone()
            if prior:
                if prior["transform_run_id"] != run_id or prior["batch_id"] != batch.batch_id or prior["model_fingerprint"] != fingerprint:
                    raise LedgerError("idempotency key reused with different semantics")
                return dict(prior)
            active = db.execute("SELECT 1 FROM batch_executions WHERE transform_run_id=? AND batch_id=? AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','ORPHANED')", (run_id, batch.batch_id)).fetchone()
            if active:
                return dict(db.execute("SELECT * FROM batch_executions WHERE transform_run_id=? AND batch_id=?", (run_id, batch.batch_id)).fetchone())
            for prerequisite in batch.prerequisite_batches:
                succeeded = db.execute("SELECT 1 FROM batch_executions WHERE transform_run_id=? AND batch_id=? AND status='SUCCEEDED' AND test_status='PASSED'", (run_id, prerequisite)).fetchone()
                if not succeeded:
                    raise LedgerError("durable batch prerequisite is incomplete")
            execution_id = f"be_{uuid4().hex}"
            accepted = now()
            db.execute(
                "INSERT INTO batch_executions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (execution_id, run_id, caller, key, batch.batch_id, batch.authority, fingerprint, "ADMITTED", accepted, None, None, 1, None, None, accepted),
            )
            db.execute("INSERT INTO batch_attempts (batch_execution_id,attempt,status,heartbeat_at) VALUES (?,?,?,?)", (execution_id, 1, "ADMITTED", accepted))
            self._event(db, run_id, execution_id, "BATCH_ADMITTED", {"batch_id": batch.batch_id, "authority": batch.authority})
            return dict(db.execute("SELECT * FROM batch_executions WHERE batch_execution_id=?", (execution_id,)).fetchone())

    def get_batch(self, execution_id: str, caller: str):
        row = self.connection.execute("SELECT * FROM batch_executions WHERE batch_execution_id=? AND caller_identity=?", (execution_id, caller)).fetchone()
        if not row:
            raise LedgerError("batch execution not found")
        return dict(row)

    def transition(self, execution_id: str, state: str, *, test_status=None, failure_class=None):
        with self.transaction() as db:
            row = db.execute("SELECT * FROM batch_executions WHERE batch_execution_id=?", (execution_id,)).fetchone()
            if not row or state not in TRANSITIONS.get(row["status"], set()):
                raise LedgerError("invalid batch state transition")
            timestamp = now()
            started = timestamp if state == "RUNNING" else row["started_at"]
            finished = timestamp if state in TERMINAL else None
            db.execute("UPDATE batch_executions SET status=?,started_at=?,finished_at=?,test_status=?,failure_class=?,heartbeat_at=? WHERE batch_execution_id=?", (state, started, finished, test_status, failure_class, timestamp, execution_id))
            self._event(db, row["transform_run_id"], execution_id, state, {"failure_class": failure_class})
            return self.get_batch(execution_id, row["caller_identity"])

    def _event(self, db, run_id, execution_id, event_type, metadata):
        db.execute("INSERT INTO execution_events (event_id,transform_run_id,batch_execution_id,event_type,event_at,metadata_json) VALUES (?,?,?,?,?,?)", (uuid4().hex, run_id, execution_id, event_type, now(), json.dumps(metadata, sort_keys=True)))


def model_fingerprint(models) -> str:
    return hashlib.sha256("\n".join(sorted(models)).encode()).hexdigest()


def application_database_url() -> str:
    """Resolve the ledger application URL; an absent source yields an empty value."""
    try:
        return require_secret("TRANSFORM_LEDGER_APP_DATABASE_URL").strip()
    except SecretUnavailable:
        return ""


def create_ledger_from_environment():
    """
    Create the authoritative transform ledger.

    PostgreSQL is mandatory whenever TRANSFORM_LEDGER_APP_DATABASE_URL is
    configured. SQLite exists only for isolated unit tests.
    """
    import os

    database_url = application_database_url()

    if database_url:
        return PostgresTransformLedger(database_url)

    if os.getenv("PYTEST_CURRENT_TEST"):
        return TransformLedger(
            os.getenv("TRANSFORM_LEDGER_SQLITE_PATH", ":memory:")
        )

    raise LedgerError(
        "TRANSFORM_LEDGER_APP_DATABASE_URL is required outside isolated tests"
    )


class PostgresTransformLedger:
    """
    PostgreSQL-backed durable transform execution ledger.

    Uses the same externally visible contract as TransformLedger.
    """

    def __init__(self, database_url: str) -> None:
        if not database_url.startswith(
            ("postgresql://", "postgresql+psycopg://")
        ):
            raise LedgerError("PostgreSQL transform ledger URL is required")

        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise LedgerError("psycopg is required for PostgreSQL ledger") from exc

        # psycopg itself expects postgresql:// rather than SQLAlchemy's
        # postgresql+psycopg:// spelling.
        from orchestration.transform_runtime.postgres_connection import validate_database_url
        self.database_url = validate_database_url(database_url)
        self._psycopg = psycopg
        self._dict_row = dict_row

    @contextmanager
    def transaction(self):
        with self._psycopg.connect(
            self.database_url,
            row_factory=self._dict_row,
        ) as connection:
            with connection.transaction():
                yield connection

    def ping(self) -> None:
        with self.transaction() as db:
            db.execute("SELECT 1").fetchone()

    def create_run(
        self,
        caller: str,
        key: str,
        mode: str,
        contract_digest: str,
        plan_digest: str,
    ):
        with self.transaction() as db:
            row = db.execute(
                """
                SELECT *
                FROM transform_runs
                WHERE caller_identity=%s
                  AND idempotency_key=%s
                """,
                (caller, key),
            ).fetchone()

            if row:
                immutable = (
                    row["execution_mode"],
                    row["contract_digest"],
                    row["plan_digest"],
                )
                if immutable != (mode, contract_digest, plan_digest):
                    raise LedgerError(
                        "idempotency key reused with different semantics"
                    )
                return dict(row)

            active = db.execute(
                """
                SELECT 1
                FROM transform_runs
                WHERE status NOT IN
                    ('SUCCEEDED','FAILED','CANCELLED','ORPHANED')
                LIMIT 1
                """
            ).fetchone()

            if active:
                raise LedgerError(
                    "one active transform run is already admitted"
                )

            run_id = f"tr_{uuid4().hex}"
            accepted = now()

            row = db.execute(
                """
                INSERT INTO transform_runs (
                    transform_run_id,
                    caller_identity,
                    idempotency_key,
                    execution_mode,
                    contract_digest,
                    plan_digest,
                    status,
                    accepted_at,
                    heartbeat_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING *
                """,
                (
                    run_id,
                    caller,
                    key,
                    mode,
                    contract_digest,
                    plan_digest,
                    "ADMITTED",
                    accepted,
                    accepted,
                ),
            ).fetchone()

            self._event(
                db,
                run_id,
                None,
                "RUN_ADMITTED",
                {"caller": caller},
            )

            return dict(row)

    def submit_batch(
        self,
        run_id: str,
        caller: str,
        key: str,
        batch,
        fingerprint: str,
    ):
        with self.transaction() as db:
            run = db.execute(
                """
                SELECT *
                FROM transform_runs
                WHERE transform_run_id=%s
                  AND caller_identity=%s
                """,
                (run_id, caller),
            ).fetchone()

            if not run:
                raise LedgerError("transform run not found")

            prior = db.execute(
                """
                SELECT *
                FROM batch_executions
                WHERE caller_identity=%s
                  AND idempotency_key=%s
                """,
                (caller, key),
            ).fetchone()

            if prior:
                if (
                    prior["transform_run_id"] != run_id
                    or prior["batch_id"] != batch.batch_id
                    or prior["model_fingerprint"] != fingerprint
                ):
                    raise LedgerError(
                        "idempotency key reused with different semantics"
                    )
                return dict(prior)

            active = db.execute(
                """
                SELECT *
                FROM batch_executions
                WHERE transform_run_id=%s
                  AND batch_id=%s
                  AND status NOT IN
                    ('SUCCEEDED','FAILED','CANCELLED','ORPHANED')
                LIMIT 1
                """,
                (run_id, batch.batch_id),
            ).fetchone()

            if active:
                return dict(active)

            for prerequisite in batch.prerequisite_batches:
                succeeded = db.execute(
                    """
                    SELECT 1
                    FROM batch_executions
                    WHERE transform_run_id=%s
                      AND batch_id=%s
                      AND status='SUCCEEDED'
                      AND test_status='PASSED'
                    LIMIT 1
                    """,
                    (run_id, prerequisite),
                ).fetchone()

                if not succeeded:
                    raise LedgerError(
                        "durable batch prerequisite is incomplete"
                    )

            execution_id = f"be_{uuid4().hex}"
            accepted = now()

            row = db.execute(
                """
                INSERT INTO batch_executions (
                    batch_execution_id,
                    transform_run_id,
                    caller_identity,
                    idempotency_key,
                    batch_id,
                    authority,
                    model_fingerprint,
                    status,
                    accepted_at,
                    started_at,
                    finished_at,
                    attempt,
                    test_status,
                    failure_class,
                    heartbeat_at
                )
                VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,
                    NULL,NULL,%s,NULL,NULL,%s
                )
                RETURNING *
                """,
                (
                    execution_id,
                    run_id,
                    caller,
                    key,
                    batch.batch_id,
                    batch.authority,
                    fingerprint,
                    "ADMITTED",
                    accepted,
                    1,
                    accepted,
                ),
            ).fetchone()

            db.execute(
                """
                INSERT INTO batch_attempts (
                    batch_execution_id,
                    attempt,
                    status,
                    heartbeat_at
                )
                VALUES (%s,%s,%s,%s)
                """,
                (
                    execution_id,
                    1,
                    "ADMITTED",
                    accepted,
                ),
            )

            self._event(
                db,
                run_id,
                execution_id,
                "BATCH_ADMITTED",
                {
                    "batch_id": batch.batch_id,
                    "authority": batch.authority,
                },
            )

            return dict(row)

    def get_batch(self, execution_id: str, caller: str):
        with self.transaction() as db:
            row = db.execute(
                """
                SELECT *
                FROM batch_executions
                WHERE batch_execution_id=%s
                  AND caller_identity=%s
                """,
                (execution_id, caller),
            ).fetchone()

            if not row:
                raise LedgerError("batch execution not found")

            return dict(row)

    def transition(
        self,
        execution_id: str,
        state: str,
        *,
        test_status=None,
        failure_class=None,
    ):
        with self.transaction() as db:
            row = db.execute(
                """
                SELECT *
                FROM batch_executions
                WHERE batch_execution_id=%s
                FOR UPDATE
                """,
                (execution_id,),
            ).fetchone()

            if (
                not row
                or state not in TRANSITIONS.get(row["status"], set())
            ):
                raise LedgerError(
                    "invalid batch state transition"
                )

            timestamp = now()

            started = (
                timestamp
                if state == "RUNNING"
                else row["started_at"]
            )

            finished = (
                timestamp
                if state in TERMINAL
                else None
            )

            updated = db.execute(
                """
                UPDATE batch_executions
                SET status=%s,
                    started_at=%s,
                    finished_at=%s,
                    test_status=%s,
                    failure_class=%s,
                    heartbeat_at=%s
                WHERE batch_execution_id=%s
                RETURNING *
                """,
                (
                    state,
                    started,
                    finished,
                    test_status,
                    failure_class,
                    timestamp,
                    execution_id,
                ),
            ).fetchone()

            self._event(
                db,
                row["transform_run_id"],
                execution_id,
                state,
                {"failure_class": failure_class},
            )

            return dict(updated)

    def _event(
        self,
        db,
        run_id,
        execution_id,
        event_type,
        metadata,
    ):
        db.execute(
            """
            INSERT INTO execution_events (
                event_id,
                transform_run_id,
                batch_execution_id,
                event_type,
                event_at,
                metadata_json
            )
            VALUES (%s,%s,%s,%s,%s,%s::jsonb)
            """,
            (
                uuid4().hex,
                run_id,
                execution_id,
                event_type,
                now(),
                json.dumps(metadata, sort_keys=True),
            ),
        )


def _sqlite_schema_version(self):
    return 3


def _pg_schema_version(self):
    with self.transaction() as db:
        row=db.execute('SELECT max(version) AS version FROM transform_schema_version').fetchone()
        if not row or row['version'] is None:
            raise LedgerError('transform ledger schema is not migrated')
        return int(row['version'])

TransformLedger.schema_version=_sqlite_schema_version
PostgresTransformLedger.schema_version=_pg_schema_version
