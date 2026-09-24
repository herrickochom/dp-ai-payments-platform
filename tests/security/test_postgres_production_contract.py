"""Connection hardening without changing C8 lease SQL."""

from pathlib import Path

import pytest

from orchestration.transform_runtime.postgres_connection import validate_database_url


SECURE_URL = (
    "postgresql://transform_app:secret@postgres.example:5432/ledger"
    "?sslmode=verify-full&sslrootcert=%2Fetc%2Fssl%2Fpostgres-ca.pem"
    "&connect_timeout=5&options=-c%20statement_timeout%3D30000"
)


def test_production_database_connection_requires_tls_and_bounded_timeouts(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    assert validate_database_url(SECURE_URL) == SECURE_URL
    for replacement in (
        SECURE_URL.replace("verify-full", "disable"),
        SECURE_URL.replace("connect_timeout=5", "connect_timeout=0"),
        SECURE_URL.replace("statement_timeout%3D30000", "statement_timeout%3D0"),
        SECURE_URL.replace("transform_app", "postgres"),
        SECURE_URL.replace("sslrootcert=%2Fetc%2Fssl%2Fpostgres-ca.pem&", ""),
    ):
        with pytest.raises(ValueError):
            validate_database_url(replacement)


def test_local_database_urls_keep_existing_semantics(monkeypatch):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)
    assert validate_database_url("postgresql+psycopg://localhost/ledger") == "postgresql://localhost/ledger"


def test_c8_lease_and_unknown_write_safety_remain_in_place():
    root = Path(__file__).resolve().parents[2]
    queue = (root / "orchestration/transform_runtime/durable_queue.py").read_text()
    worker = (root / "orchestration/job_runner/transform_worker.py").read_text()
    assert "FOR UPDATE SKIP LOCKED" in queue
    assert "clock_timestamp()" in queue
    assert "lease_token" in queue
    assert "status='ORPHANED'" in queue
    assert "WORKER_LEASE_LOST_REQUIRES_RECONCILIATION" in worker


def test_runtime_and_migration_database_authorities_are_separate():
    root = Path(__file__).resolve().parents[2]

    compose = (root / "docker-compose.yaml").read_text()
    queue = (
        root / "orchestration/transform_runtime/durable_queue.py"
    ).read_text()
    ledger = (
        root / "orchestration/transform_runtime/ledger.py"
    ).read_text()
    migrate = (
        root / "orchestration/transform_runtime/migrate.py"
    ).read_text()

    assert "TRANSFORM_LEDGER_APP_DATABASE_URL" in queue
    assert "TRANSFORM_LEDGER_MIGRATION_DATABASE_URL" not in queue

    assert "TRANSFORM_LEDGER_APP_DATABASE_URL" in ledger
    assert "TRANSFORM_LEDGER_MIGRATION_DATABASE_URL" not in ledger

    assert "TRANSFORM_LEDGER_MIGRATION_DATABASE_URL" in migrate
    assert "TRANSFORM_LEDGER_APP_DATABASE_URL" not in migrate

    runtime = compose.split(
        "  transform-runtime:\n", 1
    )[1].split(
        "\n  platform-job-runner:", 1
    )[0]

    runner = compose.split(
        "  platform-job-runner:\n", 1
    )[1].split(
        "\n  airflow-init:", 1
    )[0]

    assert "TRANSFORM_LEDGER_APP_DATABASE_URL" in runtime
    assert "TRANSFORM_LEDGER_MIGRATION_DATABASE_URL" not in runtime

    assert "TRANSFORM_LEDGER_APP_DATABASE_URL" in runner
    assert "TRANSFORM_LEDGER_MIGRATION_DATABASE_URL" not in runner


def test_application_database_role_rejects_admin_identity(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    admin_url = SECURE_URL.replace(
        "transform_app",
        "postgres",
    )

    with pytest.raises(ValueError, match="non-administrative"):
        validate_database_url(admin_url)


def test_migration_database_role_may_have_schema_authority(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    migration_url = SECURE_URL.replace(
        "transform_app",
        "transform_migration",
    )

    assert (
        validate_database_url(
            migration_url,
            allow_schema_admin=True,
        )
        == migration_url
    )
