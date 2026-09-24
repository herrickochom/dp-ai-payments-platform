"""Explicit PostgreSQL transform-ledger migration entry point."""
from __future__ import annotations

from pathlib import Path

from orchestration.transform_runtime.postgres_connection import validate_database_url
from services.shared.security.secret_provider import require_secret


def main() -> None:
    import psycopg

    database_url = validate_database_url(
        require_secret("TRANSFORM_LEDGER_MIGRATION_DATABASE_URL"),
        allow_schema_admin=True,
    )

    migration_dir = Path(__file__).with_name("migrations")

    with psycopg.connect(database_url) as connection:
        with connection.transaction():
            for migration in sorted(migration_dir.glob("*.sql")):
                connection.execute(migration.read_text())

    print("TRANSFORM_LEDGER_MIGRATIONS=APPLIED")


if __name__ == "__main__":
    main()
