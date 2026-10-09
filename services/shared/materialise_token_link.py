"""Crash-safe initial creation of the restricted beneficiary token link."""
from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

from services.shared.security.runtime_security import validate_object_store_security
from services.shared.security.secret_provider import require_mounted_secret
CATALOG = "iceberg"
SOURCE = "iceberg.bronze.br_pdm_pdmis_beneficiaries"
SCHEMA = "silver_vault"
TABLE = "vlt_pdm_beneficiary_token_link"
TARGET = f"{CATALOG}.{SCHEMA}.{TABLE}"
OPERATION_ID = "initial_token_link_creation"
OPERATION_VERSION = "1"
AUTHORITY = "restricted_identity_transform"
EXECUTION_RE = re.compile(r"be_[a-f0-9]{32}")
EXPECTED_COLUMNS = (
    ("beneficiary_id", "varchar"),
    ("beneficiary_key_internal", "varchar"),
    ("token_version", "varchar"),
    ("beneficiary_token", "varchar"),
    ("is_active", "boolean"),
    ("generated_at", "varchar"),
)


class PublicationIndeterminate(RuntimeError):
    """Publication may have committed and must be reconciled, never retried."""


@dataclass(frozen=True)
class OperationContext:
    execution_id: str
    staging_table: str
    staging_target: str
    token_version: str


def require_env(name: str) -> str:
    value = os.getenv(name, "")
    if not value.strip():
        raise RuntimeError(f"{name} is required; fail closed")
    return value


def operation_context() -> OperationContext:
    execution_id = require_env("TRANSFORM_EXECUTION_ID")
    if EXECUTION_RE.fullmatch(execution_id) is None:
        raise RuntimeError("invalid governed transform execution identity")
    if require_env("PROTECTED_BOOTSTRAP_OPERATION") != OPERATION_ID:
        raise RuntimeError("invalid protected bootstrap operation identity")
    if require_env("PROTECTED_BOOTSTRAP_VERSION") != OPERATION_VERSION:
        raise RuntimeError("invalid protected bootstrap operation version")
    if require_env("TRANSFORM_AUTHORITY") != AUTHORITY:
        raise RuntimeError("invalid protected bootstrap authority")
    # Token key material is resolved from the mount only. An ambient
    # environment variable of the same name must never substitute for a
    # key the operator provisioned on the mount: the mount is the sole
    # authoritative source for this operation, and an empty or absent
    # file is a hard failure rather than a prompt to fall back.
    require_mounted_secret("DP_TOKEN_KEY")
    token_version = require_mounted_secret("DP_TOKEN_KEY_VERSION")
    if not token_version.strip():
        raise RuntimeError("DP_TOKEN_KEY_VERSION is required; fail closed")
    staging_table = f"__bootstrap_{execution_id.removeprefix('be_')}"
    return OperationContext(execution_id, staging_table, f"{CATALOG}.{SCHEMA}.{staging_table}", token_version)


def object_store_settings() -> dict[str, object]:
    endpoint, use_ssl, ca_bundle = validate_object_store_security(
        require_env("S3_ENDPOINT"), use_ssl=require_env("S3_USE_SSL"),
        ca_bundle=os.getenv("S3_CA_BUNDLE"),
    )
    style = require_env("DBT_S3_URL_STYLE")
    if style not in {"path", "vhost"}:
        raise ValueError("DBT_S3_URL_STYLE must be path or vhost")
    return {"region": require_env("OBJECT_STORE_REGION"), "endpoint": urlsplit(endpoint).netloc,
            "url_style": style, "use_ssl": use_ssl, "ca_bundle": ca_bundle}


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _trino_connection():
    import trino
    from trino.auth import BasicAuthentication
    user = require_env("DBT_TRINO_USER")
    password = require_env("DBT_TRINO_PASSWORD")
    return trino.dbapi.connect(host=require_env("DBT_TRINO_HOST"),
        port=int(require_env("DBT_TRINO_PORT")), user=user, http_scheme="https",
        auth=BasicAuthentication(user, password), verify=False)


def relation_exists(cursor, table: str) -> bool:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", table):
        raise RuntimeError("invalid governed relation name")
    cursor.execute("SELECT count(*) FROM iceberg.information_schema.tables "
                   f"WHERE table_schema = '{SCHEMA}' AND table_name = '{table}'")
    rows = cursor.fetchall()
    if rows in ([[0]], [(0,)]): return False
    if rows in ([[1]], [(1,)]): return True
    raise RuntimeError("ambiguous restricted token-link table state")


def _show_create(cursor, target: str) -> str:
    cursor.execute(f"SHOW CREATE TABLE {target}")
    return "\n".join(str(row[0]) for row in cursor.fetchall())


def _table_location(cursor, target: str) -> str:
    matches = re.findall(r"location\s*=\s*'((?:[^']|'')+)'", _show_create(cursor, target), re.I)
    if len(matches) != 1: raise RuntimeError("ambiguous Iceberg table location")
    location = matches[0].replace("''", "'").rstrip("/")
    warehouse, bucket = require_env("NESSIE_WAREHOUSE").rstrip("/"), require_env("OBJECT_STORE_BUCKET")
    parsed, base = urlsplit(location), urlsplit(warehouse)
    if parsed.scheme != "s3" or parsed.netloc != bucket or base.scheme != "s3" or base.netloc != bucket:
        raise RuntimeError("invalid restricted Iceberg table location")
    if not unquote(parsed.path).startswith(unquote(base.path).rstrip("/") + "/"):
        raise RuntimeError("restricted Iceberg table location is outside governed warehouse")
    return location


def _duckdb_writer(storage: dict[str, object]):
    import duckdb
    con = duckdb.connect(f"/tmp/pdm-token-link-{require_env('TRANSFORM_EXECUTION_ID')}.duckdb")
    extension_dir = os.getenv("DUCKDB_EXTENSION_DIRECTORY", "/opt/duckdb/extensions/v1.5.5/linux_amd64")
    con.execute(f"LOAD '{_sql_literal(extension_dir)}/httpfs.duckdb_extension'")
    for setting, value in (("s3_region", storage["region"]), ("s3_endpoint", storage["endpoint"]),
                           ("s3_url_style", storage["url_style"]), ("s3_use_ssl", storage["use_ssl"])):
        con.execute(f"SET {setting} = ?", [value])
    if storage["ca_bundle"]: con.execute("SET ca_cert_file = ?", [storage["ca_bundle"]])
    con.execute("SET s3_access_key_id = ?", [require_env("RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID")])
    con.execute("SET s3_secret_access_key = ?", [require_env("RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY")])
    return con


def validate_table(cursor, target: str, expected_rows: int | None = None,
                   expected_version: str | None = None) -> int:
    table = target.rsplit(".", 1)[-1]
    cursor.execute("SELECT column_name,data_type FROM iceberg.information_schema.columns "
                   f"WHERE table_schema='{SCHEMA}' AND table_name='{table}' ORDER BY ordinal_position")
    if tuple((str(a), str(b).lower()) for a, b in cursor.fetchall()) != EXPECTED_COLUMNS:
        raise RuntimeError("token-link schema contract validation failed")
    cursor.execute(f"""SELECT count(*),count(DISTINCT beneficiary_id),
        count(DISTINCT beneficiary_token),count(*) FILTER (WHERE is_active),
        count(*) FILTER (WHERE beneficiary_id IS NULL OR beneficiary_key_internal IS NULL
          OR token_version IS NULL OR beneficiary_token IS NULL OR is_active IS NULL OR generated_at IS NULL),
        count(DISTINCT token_version),min(token_version) FROM {target}""")
    row = cursor.fetchone()
    if row is None or len(row) != 7: raise RuntimeError("token-link validation result is invalid")
    rows, ids, tokens, active, nulls, versions, version = row
    if not rows or rows != ids or rows != tokens or rows != active or nulls or versions != 1:
        raise RuntimeError("token-link physical contract validation failed")
    if expected_rows is not None and rows != expected_rows: raise RuntimeError("token-link population mismatch")
    if expected_version is not None and version != expected_version:
        raise RuntimeError("token-link version contract validation failed")
    return int(rows)


def _provenance_matches(cursor, target: str, context: OperationContext) -> bool:
    return (f"protected-bootstrap:{context.execution_id}" in _show_create(cursor, target)
            and context.staging_table in _table_location(cursor, target))


def reconcile_publication(cursor, context: OperationContext, expected_rows: int | None) -> str:
    final_exists, staging_exists = relation_exists(cursor, TABLE), relation_exists(cursor, context.staging_table)
    if final_exists and not staging_exists:
        if not _provenance_matches(cursor, TARGET, context):
            raise PublicationIndeterminate("final relation provenance is not owned by this execution")
        validate_table(cursor, TARGET, expected_rows, context.token_version)
        return "PUBLISHED"
    if staging_exists and not final_exists: return "STAGED"
    raise PublicationIndeterminate("publication state requires governed reconciliation")


def _drop_owned_staging(cursor, context: OperationContext) -> None:
    if context.staging_table != f"__bootstrap_{context.execution_id.removeprefix('be_')}":
        raise RuntimeError("staging ownership cannot be proven")
    cursor.execute(f"DROP TABLE IF EXISTS {context.staging_target}")


def main() -> int:
    context = operation_context()
    if os.getenv("PROTECTED_BOOTSTRAP_MODE", "create") == "reconcile":
        connection = _trino_connection()
        cursor = connection.cursor()
        try:
            if reconcile_publication(cursor, context, None) != "PUBLISHED":
                raise PublicationIndeterminate("publication is not complete")
            print("PASS: protected bootstrap publication reconciled")
            return 0
        finally:
            # Cleanup failures must never change the outcome of a completed
            # operation. Each close is attempted independently and its own
            # exceptions are suppressed so the return value of main() is
            # preserved.
            try:
                cursor.close()
            except Exception:
                pass
            try:
                connection.close()
            except Exception:
                pass
    try:
        from tokeniser_job import build_token_links
    except ImportError:
        from services.shared.tokeniser_job import build_token_links
    require_env("RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID"); require_env("RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY")
    connection, duck = _trino_connection(), None
    cursor, staging_created, publication_attempted = connection.cursor(), False, False
    try:
        cursor.execute("SELECT 1")
        if cursor.fetchall() not in ([[1]], [(1,)]): raise RuntimeError("private Trino readiness result is invalid")
        if relation_exists(cursor, TABLE): raise RuntimeError("target token-link already exists; refusing replacement")
        if relation_exists(cursor, context.staging_table): raise RuntimeError("staging exists; reconciliation required")
        cursor.execute(f"""SELECT beneficiary_id FROM (SELECT beneficiary_id,row_number() OVER
            (PARTITION BY beneficiary_id ORDER BY kafka_timestamp DESC NULLS LAST,kafka_offset DESC NULLS LAST) n
            FROM {SOURCE} WHERE beneficiary_id IS NOT NULL) x WHERE n=1 ORDER BY beneficiary_id""")
        ids = [str(row[0]) for row in cursor.fetchall()]
        if not ids or len(ids) != len(set(ids)): raise RuntimeError("canonical beneficiary IDs are absent or duplicated")
        links = build_token_links(ids, version=context.token_version)
        if len(links) != len(ids): raise RuntimeError("token-link population mismatch")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
        cursor.execute(f"""CREATE TABLE {context.staging_target} (beneficiary_id VARCHAR,
            beneficiary_key_internal VARCHAR,token_version VARCHAR,beneficiary_token VARCHAR,
            is_active BOOLEAN,generated_at VARCHAR) COMMENT 'protected-bootstrap:{context.execution_id}'
            WITH (format='PARQUET')""")
        staging_created = True
        location = _table_location(cursor, context.staging_target)
        parquet = f"{location}/data/{context.execution_id}-{TABLE}.parquet"
        duck = _duckdb_writer(object_store_settings())
        duck.execute("""CREATE TABLE token_links (beneficiary_id VARCHAR NOT NULL,
          beneficiary_key_internal VARCHAR NOT NULL,token_version VARCHAR NOT NULL,
          beneficiary_token VARCHAR NOT NULL,is_active BOOLEAN NOT NULL,generated_at VARCHAR NOT NULL)""")
        duck.executemany("INSERT INTO token_links VALUES (?,?,?,?,?,?)", [(r.beneficiary_id,
            r.beneficiary_key_internal,r.token_version,r.beneficiary_token,r.is_active,r.generated_at) for r in links])
        duck.execute(f"COPY token_links TO '{_sql_literal(parquet)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
        cursor.execute(f"ALTER TABLE {context.staging_target} EXECUTE add_files(location => "
                       f"'{_sql_literal(parquet)}',format => 'PARQUET',recursive_directory => 'FAIL')")
        validate_table(cursor, context.staging_table, len(ids), context.token_version)
        if relation_exists(cursor, TABLE): raise RuntimeError("target appeared before publication; refusing replacement")
        publication_attempted = True
        try:
            cursor.execute(f"ALTER TABLE {context.staging_target} RENAME TO {TABLE}")
        except Exception as exc:
            if reconcile_publication(cursor, context, len(ids)) != "PUBLISHED":
                raise PublicationIndeterminate("publication incomplete; blind retry forbidden") from exc
        validate_table(cursor, TARGET, len(ids), context.token_version)
        if not _provenance_matches(cursor, TARGET, context):
            raise PublicationIndeterminate("published relation provenance validation failed")
        print(f"PASS: restricted token-link published: {len(ids)} rows")
        print("PASS: no identifiers, tokens, or secrets displayed")
        return 0
    except Exception:
        if staging_created and not publication_attempted and relation_exists(cursor, context.staging_table):
            _drop_owned_staging(cursor, context)
        raise
    finally:
        # Cleanup failures must never change the outcome of a completed
        # operation. In particular, a successful publication must not be
        # reported as a failure because DuckDB or Trino closed a stream
        # that had already terminated. Each close is attempted
        # independently, and each close's exceptions are suppressed.
        #
        # Do not re-raise: the process exit code is determined by the
        # return value of main(), and by the time this finally runs that
        # value has already been settled.
        if duck is not None:
            try:
                duck.close()
            except Exception:
                pass
        try:
            cursor.close()
        except Exception:
            pass
        try:
            connection.close()
        except Exception:
            pass


if __name__ == "__main__":
    try: sys.exit(main())
    except Exception as exc:
        print(f"STOP: token-link bootstrap failed: {type(exc).__name__}")
        sys.exit(1)