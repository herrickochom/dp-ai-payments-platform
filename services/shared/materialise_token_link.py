"""Materialise the restricted beneficiary token-link in Iceberg/Nessie.

Bounded operation:
  iceberg.bronze.br_pdm_pdmis_beneficiaries
      -> canonical beneficiary_id
      -> validated HMAC tokeniser
      -> restricted Parquet
      -> iceberg.silver_vault.vlt_pdm_beneficiary_token_link

The execution-scoped private Trino owns Nessie/Iceberg catalogue access.
DuckDB is used only to write the bounded Parquet payload.

No identifiers, tokens, or secrets are printed.
Fails closed if configuration is absent, source IDs are invalid/duplicated,
or the target table already exists.
"""

from __future__ import annotations

import os
import re
import sys
from urllib.parse import unquote, urlsplit

import duckdb
import trino
from trino.auth import BasicAuthentication

from services.shared.security.runtime_security import validate_object_store_security
from tokeniser_job import build_token_links


CATALOG = "iceberg"
SOURCE = "iceberg.bronze.br_pdm_pdmis_beneficiaries"
SCHEMA = "silver_vault"
TABLE = "vlt_pdm_beneficiary_token_link"
TARGET = f"{CATALOG}.{SCHEMA}.{TABLE}"


def require_env(name: str) -> str:
    value = os.getenv(name, "")
    if not value.strip():
        raise RuntimeError(f"{name} is required; fail closed")
    return value


def object_store_settings() -> dict[str, object]:
    endpoint, use_ssl, ca_bundle = validate_object_store_security(
        require_env("S3_ENDPOINT"),
        use_ssl=require_env("S3_USE_SSL"),
        ca_bundle=os.getenv("S3_CA_BUNDLE"),
    )
    style = require_env("DBT_S3_URL_STYLE")
    if style not in {"path", "vhost"}:
        raise ValueError("DBT_S3_URL_STYLE must be path or vhost")
    return {
        "region": require_env("OBJECT_STORE_REGION"),
        "endpoint": urlsplit(endpoint).netloc,
        "url_style": style,
        "use_ssl": use_ssl,
        "ca_bundle": ca_bundle,
    }


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _trino_connection():
    user = require_env("DBT_TRINO_USER")
    password = require_env("DBT_TRINO_PASSWORD")
    return trino.dbapi.connect(
        host=require_env("DBT_TRINO_HOST"),
        port=int(require_env("DBT_TRINO_PORT")),
        user=user,
        http_scheme="https",
        auth=BasicAuthentication(user, password),
        verify=False,
    )


def _target_exists(cursor) -> bool:
    cursor.execute(
        "SELECT count(*) FROM iceberg.information_schema.tables "
        f"WHERE table_schema = '{SCHEMA}' "
        f"AND table_name = '{TABLE}'"
    )
    rows = cursor.fetchall()
    if rows in ([[0]], [(0,)]):
        return False
    if rows in ([[1]], [(1,)]):
        return True
    raise RuntimeError("ambiguous restricted token-link table state")


def _table_location(cursor) -> str:
    cursor.execute(f"SHOW CREATE TABLE {TARGET}")
    text = "\n".join(str(row[0]) for row in cursor.fetchall())

    matches = re.findall(
        r"location\s*=\s*'((?:[^']|'')+)'",
        text,
        flags=re.IGNORECASE,
    )
    if len(matches) != 1:
        raise RuntimeError("ambiguous Iceberg table location")

    location = matches[0].replace("''", "'").rstrip("/")
    warehouse = require_env("NESSIE_WAREHOUSE").rstrip("/")
    bucket = require_env("OBJECT_STORE_BUCKET")

    parsed = urlsplit(location)
    warehouse_parsed = urlsplit(warehouse)

    if (
        parsed.scheme != "s3"
        or parsed.netloc != bucket
        or warehouse_parsed.scheme != "s3"
        or warehouse_parsed.netloc != bucket
    ):
        raise RuntimeError("invalid restricted Iceberg table location")

    location_path = unquote(parsed.path)
    warehouse_path = unquote(warehouse_parsed.path).rstrip("/")

    if not location_path.startswith(warehouse_path + "/"):
        raise RuntimeError(
            "restricted Iceberg table location is outside governed warehouse"
        )

    return location


def _parquet_path(location: str) -> str:
    execution_id = require_env("TRANSFORM_EXECUTION_ID")
    if not re.fullmatch(r"be_[a-f0-9]{32}", execution_id):
        raise RuntimeError("invalid governed transform execution identity")
    return f"{location}/data/{execution_id}-{TABLE}.parquet"


def _duckdb_writer(storage: dict[str, object]):
    con = duckdb.connect("/tmp/pdm-token-link.duckdb")

    extension_dir = os.getenv(
        "DUCKDB_EXTENSION_DIRECTORY",
        "/opt/duckdb/extensions/v1.5.5/linux_amd64",
    )
    con.execute(
        f"LOAD '{_sql_literal(extension_dir)}/httpfs.duckdb_extension'"
    )

    con.execute("SET s3_region = ?", [storage["region"]])
    con.execute("SET s3_endpoint = ?", [storage["endpoint"]])
    con.execute("SET s3_url_style = ?", [storage["url_style"]])
    con.execute("SET s3_use_ssl = ?", [storage["use_ssl"]])
    if storage["ca_bundle"]:
        con.execute("SET ca_cert_file = ?", [storage["ca_bundle"]])

    con.execute(
        "SET s3_access_key_id = ?",
        [require_env("RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID")],
    )
    con.execute(
        "SET s3_secret_access_key = ?",
        [require_env("RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY")],
    )
    return con


def main() -> int:
    require_env("DP_TOKEN_KEY")
    version = require_env("DP_TOKEN_KEY_VERSION")

    # Keep these explicit requirements as part of the restricted boundary.
    require_env("RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID")
    require_env("RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY")

    storage = object_store_settings()

    connection = _trino_connection()
    cursor = connection.cursor()
    duck = None
    target_created = False

    try:
        cursor.execute("SELECT 1")
        if cursor.fetchall() not in ([[1]], [(1,)]):
            raise RuntimeError("private Trino readiness result is invalid")

        if _target_exists(cursor):
            raise RuntimeError(
                "target token-link already exists; refusing destructive replacement"
            )

        cursor.execute(
            f"""
            SELECT beneficiary_id
            FROM (
                SELECT
                    beneficiary_id,
                    row_number() OVER (
                        PARTITION BY beneficiary_id
                        ORDER BY kafka_timestamp DESC NULLS LAST,
                                 kafka_offset DESC NULLS LAST
                    ) AS _row_number
                FROM {SOURCE}
                WHERE beneficiary_id IS NOT NULL
            ) ranked
            WHERE _row_number = 1
            ORDER BY beneficiary_id
            """
        )
        beneficiary_ids = [str(row[0]) for row in cursor.fetchall()]

        if not beneficiary_ids:
            raise RuntimeError("no canonical beneficiary IDs found; fail closed")

        if len(beneficiary_ids) != len(set(beneficiary_ids)):
            raise RuntimeError("duplicate canonical beneficiary IDs; fail closed")

        links = build_token_links(
            beneficiary_ids,
            version=version,
        )

        if len(links) != len(beneficiary_ids):
            raise RuntimeError("token-link population mismatch; fail closed")

        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

        cursor.execute(
            f"""
            CREATE TABLE {TARGET} (
                beneficiary_id VARCHAR,
                beneficiary_key_internal VARCHAR,
                token_version VARCHAR,
                beneficiary_token VARCHAR,
                is_active BOOLEAN,
                generated_at VARCHAR
            )
            WITH (
                format = 'PARQUET'
            )
            """
        )
        target_created = True

        location = _table_location(cursor)
        parquet_path = _parquet_path(location)

        duck = _duckdb_writer(storage)
        duck.execute(
            """
            CREATE TABLE token_links (
                beneficiary_id VARCHAR NOT NULL,
                beneficiary_key_internal VARCHAR NOT NULL,
                token_version VARCHAR NOT NULL,
                beneficiary_token VARCHAR NOT NULL,
                is_active BOOLEAN NOT NULL,
                generated_at VARCHAR NOT NULL
            )
            """
        )
        duck.executemany(
            """
            INSERT INTO token_links VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.beneficiary_id,
                    row.beneficiary_key_internal,
                    row.token_version,
                    row.beneficiary_token,
                    row.is_active,
                    row.generated_at,
                )
                for row in links
            ],
        )

        duck.execute(
            f"""
            COPY token_links
            TO '{_sql_literal(parquet_path)}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD
            )
            """
        )

        cursor.execute(
            f"""
            ALTER TABLE {TARGET}
            EXECUTE add_files(
                location => '{_sql_literal(parquet_path)}',
                format => 'PARQUET',
                recursive_directory => 'FAIL'
            )
            """
        )

        cursor.execute(
            f"""
            SELECT
                count(*) AS rows,
                count(DISTINCT beneficiary_id) AS distinct_ids,
                count(DISTINCT beneficiary_token) AS distinct_tokens,
                count(*) FILTER (WHERE is_active) AS active_rows,
                count(*) FILTER (
                    WHERE beneficiary_id IS NULL
                       OR beneficiary_key_internal IS NULL
                       OR token_version IS NULL
                       OR beneficiary_token IS NULL
                       OR generated_at IS NULL
                ) AS null_contract_rows
            FROM {TARGET}
            """
        )
        stats = cursor.fetchone()

        rows, distinct_ids, distinct_tokens, active_rows, null_contract_rows = stats

        if not (
            rows == distinct_ids == distinct_tokens == active_rows
            and null_contract_rows == 0
        ):
            raise RuntimeError("post-write token-link validation failed")

        print(f"PASS: restricted token-link materialised: {rows} rows")
        print(f"PASS: unique canonical IDs: {distinct_ids}")
        print(f"PASS: unique canonical tokens: {distinct_tokens}")
        print(f"PASS: active rows: {active_rows}")
        print("PASS: null contract rows: 0")
        print("PASS: no identifiers, tokens, or secrets displayed")

        return 0

    except Exception:
        if target_created:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {TARGET}")
            except Exception:
                pass
        raise
    finally:
        if duck is not None:
            duck.close()
        cursor.close()
        connection.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(
            "STOP: token-link materialisation failed: "
            f"{type(exc).__name__}: {exc}"
        )
        sys.exit(1)
