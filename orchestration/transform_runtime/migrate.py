"""Explicit PostgreSQL transform-ledger migration entry point."""
from __future__ import annotations

import os
from pathlib import Path


def main() -> None:
    import psycopg

    database_url = os.environ["TRANSFORM_LEDGER_DATABASE_URL"].replace(
        "postgresql+psycopg://",
        "postgresql://",
        1,
    )

    migration_dir = Path(__file__).with_name("migrations")

    with psycopg.connect(database_url) as connection:
        with connection.transaction():
            for migration in sorted(migration_dir.glob("*.sql")):
                connection.execute(migration.read_text())

    print("TRANSFORM_LEDGER_MIGRATIONS=APPLIED")


if __name__ == "__main__":
    main()
