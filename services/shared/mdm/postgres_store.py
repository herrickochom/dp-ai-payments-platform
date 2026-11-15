"""Atomic persistence of the established ten MDM datasets."""

from __future__ import annotations

import os
import re
from typing import Any


TABLES = (
    "beneficiary_master_sources", "sacco_master_sources", "agent_master_sources",
    "geography_master_sources", "golden_beneficiaries_restricted",
    "golden_saccos", "golden_agents", "golden_geography",
    "source_crosswalk", "beneficiary_identity_alerts_restricted",
)


def persist_mdm(datasets: dict[str, list[dict[str, Any]]]) -> None:
    """Replace one complete materialisation in a single PostgreSQL transaction.

    DELETE is used rather than TRUNCATE so the narrow CDC publication can observe
    removals as well as inserts. Any failure rolls back the whole transaction.
    """
    if set(datasets) != set(TABLES):
        raise ValueError("MDM dataset set does not match the ten-table contract")

    required = ("PDM_SOURCE_HOST", "PDM_SOURCE_DB", "PDM_SOURCE_USER", "PDM_SOURCE_PASSWORD")
    settings = {name: os.environ.get(name, "").strip() for name in required}
    if not all(settings.values()):
        raise RuntimeError("PDM source PostgreSQL connection variables are required")

    # Explicit migration-only gate. No automatic full-table replacement.
    if os.environ.get("PDM_MDM_FULL_REPLACE_APPROVED") != "I_APPROVE_FULL_MDM_REPLACEMENT":
        raise RuntimeError("Full MDM replacement DENIED; explicit migration approval required")
    if os.environ.get("PDM_MDM_TARGET") != "cloudsql":
        raise RuntimeError("Full MDM replacement DENIED; target must be cloudsql")
    if os.environ.get("PDM_SOURCE_SSLMODE") != "verify-full":
        raise RuntimeError("Cloud SQL MDM requires sslmode=verify-full")
    if not os.environ.get("PDM_SOURCE_SSLROOTCERT", "").strip():
        raise RuntimeError("Cloud SQL MDM requires an explicit CA certificate path")
    if not os.environ.get("PDM_SOURCE_CONNECT_TIMEOUT", "").isdigit() or not 1 <= int(os.environ["PDM_SOURCE_CONNECT_TIMEOUT"]) <= 30:
        raise RuntimeError("Cloud SQL MDM requires a 1..30 second connection timeout")
    if not os.environ.get("PDM_SOURCE_STATEMENT_TIMEOUT_MS", "").isdigit() or not 1000 <= int(os.environ["PDM_SOURCE_STATEMENT_TIMEOUT_MS"]) <= 300000:
        raise RuntimeError("Cloud SQL MDM requires a 1000..300000 ms statement timeout")
    if settings["PDM_SOURCE_USER"].lower() in {"postgres", "root", "admin"}:
        raise RuntimeError("Cloud SQL MDM requires a non-admin writer identity")

    import psycopg2
    from psycopg2.extras import execute_values

    connection = psycopg2.connect(
        host=settings["PDM_SOURCE_HOST"], port=int(os.environ.get("PDM_SOURCE_DB_PORT", "5432")),
        dbname=settings["PDM_SOURCE_DB"], user=settings["PDM_SOURCE_USER"],
        password=settings["PDM_SOURCE_PASSWORD"],
        sslmode=os.environ["PDM_SOURCE_SSLMODE"],
        sslrootcert=os.environ["PDM_SOURCE_SSLROOTCERT"],
        connect_timeout=int(os.environ["PDM_SOURCE_CONNECT_TIMEOUT"]),
        options=f"-c statement_timeout={int(os.environ['PDM_SOURCE_STATEMENT_TIMEOUT_MS'])}",
    )
    try:
        with connection:
            with connection.cursor() as cursor:
                for table in TABLES:
                    cursor.execute(f"DELETE FROM mdm.{table}")
                for table in TABLES:
                    rows = datasets[table]
                    if not rows:
                        continue
                    columns = tuple(rows[0])
                    if not columns or any(re.fullmatch(r"[a-z_]+", column) is None
                                          for column in columns):
                        raise ValueError("Invalid MDM column name")
                    if any(tuple(row) != columns for row in rows):
                        raise ValueError("Inconsistent MDM record shape")
                    statement = f"INSERT INTO mdm.{table} ({', '.join(columns)}) VALUES %s"
                    execute_values(cursor, statement,
                                   [tuple(row[column] for column in columns) for row in rows])
    finally:
        connection.close()
