"""Materialise the Gate 2 restricted beneficiary token-link in Iceberg/Nessie.

Bounded operation:
  bronze.br_pdm_pdmis_beneficiaries
      -> canonical beneficiary_id
      -> validated HMAC tokeniser
      -> silver_vault.vlt_pdm_beneficiary_token_link

No identifiers, tokens, or secrets are printed.
Fails closed if configuration is absent, source IDs are invalid/duplicated,
or the target table already exists.
"""

from __future__ import annotations

import os
import sys
from urllib.parse import urlsplit

from services.shared.security.runtime_security import validate_object_store_security

import duckdb

from tokeniser_job import build_token_links


TARGET = "lakehouse.silver_vault.vlt_pdm_beneficiary_token_link"
SOURCE = "lakehouse.bronze.br_pdm_pdmis_beneficiaries"


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


def main() -> int:
    require_env("DP_TOKEN_KEY")
    version = require_env("DP_TOKEN_KEY_VERSION")
    minio_user = require_env("RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID")
    minio_password = require_env("RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY")
    nessie_endpoint = require_env("NESSIE_ENDPOINT")
    storage = object_store_settings()

    con = duckdb.connect("/tmp/pdm-token-link.duckdb")

    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")

    # Configure MinIO without interpolating secrets into logged SQL.
    con.execute("SET s3_region = ?", [storage["region"]])
    con.execute("SET s3_endpoint = ?", [storage["endpoint"]])
    con.execute("SET s3_url_style = ?", [storage["url_style"]])
    con.execute("SET s3_use_ssl = ?", [storage["use_ssl"]])
    if storage["ca_bundle"]:
        con.execute("SET ca_cert_file = ?", [storage["ca_bundle"]])
    con.execute("SET s3_access_key_id = ?", [minio_user])
    con.execute("SET s3_secret_access_key = ?", [minio_password])

    endpoint = nessie_endpoint.rstrip("/") + "/iceberg"

    # Endpoint is infrastructure configuration, not a credential.
    con.execute(
        f"""
        ATTACH 'payments' AS lakehouse (
            TYPE iceberg,
            ENDPOINT '{endpoint}',
            AUTHORIZATION_TYPE none
        )
        """
    )

    # Fail closed if the restricted target already exists.
    # Iceberg REST attachments do not expose lakehouse.information_schema.
    try:
        con.execute(f"SELECT 1 FROM {TARGET} LIMIT 1")
    except duckdb.CatalogException:
        pass
    else:
        raise RuntimeError(
            "target token-link already exists; refusing destructive replacement"
        )

    records = con.execute(
        f"""
        SELECT beneficiary_id
        FROM {SOURCE}
        WHERE beneficiary_id IS NOT NULL
        QUALIFY row_number() OVER (
            PARTITION BY beneficiary_id
            ORDER BY kafka_timestamp DESC NULLS LAST,
                     kafka_offset DESC NULLS LAST
        ) = 1
        ORDER BY beneficiary_id
        """
    ).fetchall()

    beneficiary_ids = [str(row[0]) for row in records]

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

    con.execute("CREATE SCHEMA IF NOT EXISTS lakehouse.silver_vault")

    con.execute(
        f"""
        CREATE TABLE {TARGET} (
            beneficiary_id VARCHAR NOT NULL,
            beneficiary_key_internal VARCHAR NOT NULL,
            token_version VARCHAR NOT NULL,
            beneficiary_token VARCHAR NOT NULL,
            is_active BOOLEAN NOT NULL,
            generated_at VARCHAR NOT NULL
        )
        """
    )

    try:
        con.executemany(
            f"""
            INSERT INTO {TARGET} (
                beneficiary_id,
                beneficiary_key_internal,
                token_version,
                beneficiary_token,
                is_active,
                generated_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
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
    except Exception:
        # Do not leave a partial security mapping behind.
        con.execute(f"DROP TABLE IF EXISTS {TARGET}")
        raise

    stats = con.execute(
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
    ).fetchone()

    rows, distinct_ids, distinct_tokens, active_rows, null_contract_rows = stats

    if not (
        rows == distinct_ids == distinct_tokens == active_rows
        and null_contract_rows == 0
    ):
        con.execute(f"DROP TABLE IF EXISTS {TARGET}")
        raise RuntimeError(
            "post-write token-link validation failed; target removed"
        )

    print(f"PASS: restricted token-link materialised: {rows} rows")
    print(f"PASS: unique canonical IDs: {distinct_ids}")
    print(f"PASS: unique canonical tokens: {distinct_tokens}")
    print(f"PASS: active rows: {active_rows}")
    print("PASS: null contract rows: 0")
    print("PASS: no identifiers, tokens, or secrets displayed")

    con.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"STOP: token-link materialisation failed: {type(exc).__name__}: {exc}")
        sys.exit(1)
