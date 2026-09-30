"""Governed Raw-to-Bronze execution.

DuckDB transforms Raw Avro directly into a permanent Parquet data file
inside the Iceberg-assigned table location. Private, execution-scoped Trino
then registers that exact file on the run-specific Nessie branch.

Python orchestrates execution and metadata only. Raw rows are never
materialised in Python.
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote, urlsplit

import duckdb
import trino
from trino.auth import BasicAuthentication


DBT_PROJECT_DIR = Path("/app/dbt")
BRONZE_MODEL_DIR = DBT_PROJECT_DIR / "models" / "bronze"
DUCKDB_EXTENSION_DIRECTORY = "/opt/duckdb/extensions"
ICEBERG_CATALOG = "iceberg"
BRONZE_SCHEMA = "bronze"

Column = tuple[str, str]


class AmbiguousPublicationState(RuntimeError):
    """The executor cannot safely prove whether publication completed."""


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"required environment variable is unavailable: {name}")
    return value


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _quoted_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _table_name(model_name: str) -> str:
    if not re.fullmatch(r"[a-z0-9_]+", model_name):
        raise RuntimeError("invalid Bronze model name")
    return (
        f"{ICEBERG_CATALOG}.{BRONZE_SCHEMA}."
        f"{_quoted_identifier(model_name)}"
    )


def _trino_type(duckdb_type: str) -> str:
    """Map the scalar DuckDB types emitted by Bronze models to Trino."""

    value = " ".join(duckdb_type.upper().split())
    direct = {
        "BOOLEAN": "BOOLEAN",
        "TINYINT": "TINYINT",
        "SMALLINT": "SMALLINT",
        "INTEGER": "INTEGER",
        "BIGINT": "BIGINT",
        "FLOAT": "REAL",
        "REAL": "REAL",
        "DOUBLE": "DOUBLE",
        "DATE": "DATE",
        "VARCHAR": "VARCHAR",
        "BLOB": "VARBINARY",
        "UUID": "UUID",
    }

    if value in direct:
        return direct[value]
    if re.fullmatch(r"DECIMAL\(\d+,\s*\d+\)", value):
        return value.replace(" ", "")
    if value.startswith("TIMESTAMP"):
        if "WITH TIME ZONE" in value:
            return "TIMESTAMP(6) WITH TIME ZONE"
        return "TIMESTAMP(6)"

    raise RuntimeError(f"unsupported DuckDB Bronze column type: {duckdb_type}")


def _render_model_sql(model_name: str) -> str:
    """Render the deliberately small DuckDB-owned dbt/Jinja surface."""

    _table_name(model_name)
    model_path = BRONZE_MODEL_DIR / f"{model_name}.sql"

    if not model_path.is_file() or model_path.is_symlink():
        raise RuntimeError(f"Bronze model is unavailable: {model_name}")

    sql = model_path.read_text(encoding="utf-8")
    sql = re.sub(
        r"\A\s*\{\{\s*config\(.*?\)\s*\}\}\s*",
        "",
        sql,
        flags=re.S,
    )
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = sql.replace(
        '{{ var("s3_bucket") }}',
        _required_environment("OBJECT_STORE_BUCKET"),
    )
    sql = sql.replace(
        '{{ var("s3_path") }}',
        _required_environment("RAW_ROOT").strip("/"),
    )
    sql = re.sub(
        r"\{\{\s*extract_json\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)\s*\}\}",
        lambda match: (
            f"json_extract_string({match.group(1)}, "
            f"'{match.group(2)}')"
        ),
        sql,
    )

    if "{{" in sql or "{%" in sql or "{#" in sql:
        raise RuntimeError(
            f"unsupported Jinja remains in Bronze model: {model_name}"
        )

    return sql.strip().rstrip(";")


def _duckdb_connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(
        ":memory:",
        config={"extension_directory": DUCKDB_EXTENSION_DIRECTORY},
    )
    connection.load_extension("httpfs")
    connection.load_extension("avro")

    authority = _required_environment("TRANSFORM_AUTHORITY")
    credential_names = {
        "ordinary_transform": (
            "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID",
            "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY",
        ),
        "restricted_identity_transform": (
            "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID",
            "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY",
        ),
        "ml_prediction_transform": (
            "ML_TRANSFORM_S3_ACCESS_KEY_ID",
            "ML_TRANSFORM_S3_SECRET_ACCESS_KEY",
        ),
    }

    try:
        access_name, secret_name = credential_names[authority]
    except KeyError as exc:
        raise RuntimeError("unsupported transform authority") from exc

    endpoint = _required_environment("S3_ENDPOINT")
    endpoint = endpoint.removeprefix("http://").removeprefix("https://")
    access_key = _required_environment(access_name)
    secret_key = _required_environment(secret_name)
    region = _required_environment("OBJECT_STORE_REGION")
    use_ssl = _required_environment("S3_USE_SSL").lower() == "true"

    try:
        connection.execute(
            f"""
            CREATE SECRET raw_s3 (
                TYPE S3,
                KEY_ID '{_sql_literal(access_key)}',
                SECRET '{_sql_literal(secret_key)}',
                ENDPOINT '{_sql_literal(endpoint)}',
                REGION '{_sql_literal(region)}',
                URL_STYLE 'path',
                USE_SSL {'true' if use_ssl else 'false'}
            )
            """
        )
    except Exception:
        connection.close()
        raise RuntimeError(
            "failed to configure DuckDB object-store access"
        ) from None

    return connection


def _trino_connection():
    user = _required_environment("DBT_TRINO_USER")
    password = _required_environment("DBT_TRINO_PASSWORD")
    return trino.dbapi.connect(
        host=_required_environment("DBT_TRINO_HOST"),
        port=int(_required_environment("DBT_TRINO_PORT")),
        user=user,
        http_scheme="https",
        auth=BasicAuthentication(user, password),
        verify=False,
    )


def transformation_metadata(
    connection: duckdb.DuckDBPyConnection,
    model_name: str,
) -> tuple[str, int, tuple[Column, ...]]:
    """Render and inspect transformed data without carrying Raw rows."""

    sql = _render_model_sql(model_name)
    row_count = connection.execute(
        f"SELECT count(*) FROM ({sql}) AS transformed"
    ).fetchone()[0]
    described = connection.execute(
        f"DESCRIBE SELECT * FROM ({sql}) AS transformed"
    ).fetchall()
    columns = tuple((str(row[0]), str(row[1])) for row in described)

    if not columns:
        raise RuntimeError(f"Bronze model has no columns: {model_name}")

    return sql, int(row_count), columns


def _validate_s3_root(value: str, expected_bucket: str) -> tuple[str, str]:
    if (
        not value
        or "%" in value
        or any(character.isspace() for character in value)
    ):
        raise RuntimeError("invalid Iceberg table location")

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise RuntimeError("invalid Iceberg table location") from None

    if (
        parsed.scheme != "s3"
        or parsed.netloc != expected_bucket
        or parsed.hostname != expected_bucket
        or parsed.username is not None
        or parsed.password is not None
        or port is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/")
        or parsed.path in {"", "/"}
        or "\\" in parsed.path
    ):
        raise RuntimeError("invalid Iceberg table location")

    decoded_path = unquote(parsed.path)
    segments = decoded_path.split("/")[1:]
    if (
        not segments
        or any(not segment or segment in {".", ".."} for segment in segments)
    ):
        raise RuntimeError("invalid Iceberg table location")

    return parsed.netloc, "/" + "/".join(segments)


def validate_table_location(location: str) -> str:
    """Validate a SHOW CREATE location against governed storage boundaries."""

    bucket = _required_environment("OBJECT_STORE_BUCKET")
    warehouse = _required_environment("NESSIE_WAREHOUSE").rstrip("/")
    _, warehouse_path = _validate_s3_root(warehouse, bucket)
    _, location_path = _validate_s3_root(location.rstrip("/"), bucket)

    if not location_path.startswith(warehouse_path.rstrip("/") + "/"):
        raise RuntimeError(
            "Iceberg table location is outside the governed warehouse"
        )

    return f"s3://{bucket}{location_path}"


def extract_table_location(show_create_rows: Iterable[Iterable[object]]) -> str:
    """Extract exactly one Iceberg location property from SHOW CREATE."""

    text = "\n".join(
        str(value)
        for row in show_create_rows
        for value in row
        if value is not None
    )
    matches = re.findall(
        r"(?i)\blocation\s*=\s*'((?:''|[^'])+)'",
        text,
    )

    if len(matches) != 1:
        raise RuntimeError("ambiguous Iceberg table location")

    return validate_table_location(matches[0].replace("''", "'"))


def permanent_parquet_path(location: str, model_name: str) -> str:
    execution_id = _required_environment("TRANSFORM_EXECUTION_ID")
    if not re.fullmatch(r"be_[a-f0-9]{32}", execution_id):
        raise RuntimeError("invalid governed transform execution identity")
    _table_name(model_name)
    location = validate_table_location(location)
    return f"{location}/data/{execution_id}-{model_name}.parquet"


def _table_exists(cursor, model_name: str) -> bool:
    cursor.execute(
        "SELECT count(*) FROM iceberg.information_schema.tables "
        "WHERE table_schema = 'bronze' "
        f"AND table_name = '{_sql_literal(model_name)}'"
    )
    rows = cursor.fetchall()
    if rows in ([[0]], [(0,)]):
        return False
    if rows in ([[1]], [(1,)]):
        return True
    raise AmbiguousPublicationState(
        f"ambiguous existing table state: {model_name}"
    )


def _create_table(cursor, model_name: str, columns: Iterable[Column]) -> None:
    definitions = ",\n".join(
        f"{_quoted_identifier(name)} {_trino_type(data_type)}"
        for name, data_type in columns
    )
    if not definitions:
        raise RuntimeError(f"Bronze model has no columns: {model_name}")
    cursor.execute(
        f"""
        CREATE TABLE {_table_name(model_name)} (
            {definitions}
        )
        WITH (
            format = 'PARQUET'
        )
        """
    )


def _show_table_location(cursor, model_name: str) -> str:
    cursor.execute(f"SHOW CREATE TABLE {_table_name(model_name)}")
    return extract_table_location(cursor.fetchall())


def _expected_trino_columns(columns: Iterable[Column]) -> tuple[Column, ...]:
    return tuple((name, _trino_type(data_type)) for name, data_type in columns)


def _table_columns(cursor, model_name: str) -> tuple[Column, ...]:
    cursor.execute(f"DESCRIBE {_table_name(model_name)}")
    return tuple(
        (str(row[0]), " ".join(str(row[1]).upper().split()))
        for row in cursor.fetchall()
    )


def _registered_files(cursor, model_name: str) -> tuple[tuple[str, int], ...]:
    metadata_table = _quoted_identifier(f"{model_name}$files")
    cursor.execute(
        "SELECT file_path, record_count FROM "
        f"{ICEBERG_CATALOG}.{BRONZE_SCHEMA}.{metadata_table}"
    )
    return tuple((str(row[0]), int(row[1])) for row in cursor.fetchall())


def _permanent_file_exists(
    connection: duckdb.DuckDBPyConnection,
    parquet_path: str,
) -> bool:
    try:
        connection.execute(
            "SELECT * FROM parquet_file_metadata(?) LIMIT 1",
            [parquet_path],
        ).fetchone()
    except duckdb.HTTPException as exc:
        message = str(exc)
        if (
            re.search(r"\bHTTP\s+404\b", message, flags=re.I)
            and re.search(r"\bNot\s+Found\b", message, flags=re.I)
        ):
            return False
        raise RuntimeError(
            "unable to inspect permanent Parquet file state"
        ) from None
    except duckdb.Error:
        raise RuntimeError(
            "unable to inspect permanent Parquet file state"
        ) from None
    except Exception:
        raise RuntimeError(
            "unable to inspect permanent Parquet file state"
        ) from None
    return True


def _verify_table(
    cursor,
    model_name: str,
    expected_rows: int,
    expected_columns: tuple[Column, ...],
) -> None:
    actual_columns = _table_columns(cursor, model_name)
    normalised_expected = tuple(
        (name, " ".join(data_type.upper().split()))
        for name, data_type in expected_columns
    )
    if actual_columns != normalised_expected:
        raise RuntimeError(
            f"published Bronze column mismatch: {model_name}"
        )

    cursor.execute(f"SELECT count(*) FROM {_table_name(model_name)}")
    rows = cursor.fetchall()
    if rows not in ([[expected_rows]], [(expected_rows,)]):
        raise RuntimeError(
            f"published Bronze row count mismatch: {model_name}"
        )


def _write_parquet(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    parquet_path: str,
) -> None:
    connection.execute(
        f"""
        COPY (
            {sql}
        )
        TO '{_sql_literal(parquet_path)}'
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
        """
    )


def _register_file(cursor, model_name: str, parquet_path: str) -> None:
    cursor.execute(
        f"""
        ALTER TABLE {_table_name(model_name)}
        EXECUTE add_files(
            location => '{_sql_literal(parquet_path)}',
            format => 'PARQUET',
            recursive_directory => 'FAIL'
        )
        """
    )


def execute_model(
    duck: duckdb.DuckDBPyConnection,
    cursor,
    model_name: str,
) -> str:
    """Publish one model or conclusively recognize its completed retry."""

    print(f"RAW_TO_BRONZE_START model={model_name}")
    sql, row_count, columns = transformation_metadata(duck, model_name)
    expected_columns = _expected_trino_columns(columns)
    print(
        "RAW_TO_BRONZE_TRANSFORM "
        f"model={model_name} rows={row_count} columns={len(columns)}"
    )

    exists = _table_exists(cursor, model_name)
    if not exists:
        _create_table(cursor, model_name, columns)
        print(f"RAW_TO_BRONZE_TABLE_CREATED model={model_name}")

    location = _show_table_location(cursor, model_name)
    parquet_path = permanent_parquet_path(location, model_name)
    files = _registered_files(cursor, model_name)

    if exists:
        if files != ((parquet_path, row_count),):
            raise AmbiguousPublicationState(
                f"partial or ambiguous publication state: {model_name}"
            )
        _verify_table(
            cursor,
            model_name,
            row_count,
            expected_columns,
        )
        print(
            "RAW_TO_BRONZE_ALREADY_PUBLISHED "
            f"model={model_name} rows={row_count} "
            f"columns={len(columns)}"
        )
        return "already_published"

    if files or _permanent_file_exists(duck, parquet_path):
        raise AmbiguousPublicationState(
            f"partial or ambiguous publication state: {model_name}"
        )

    _write_parquet(duck, sql, parquet_path)
    print(f"RAW_TO_BRONZE_PARQUET_WRITTEN model={model_name}")
    _register_file(cursor, model_name, parquet_path)
    print(f"RAW_TO_BRONZE_REGISTERED model={model_name}")
    _verify_table(cursor, model_name, row_count, expected_columns)
    print(
        "RAW_TO_BRONZE_VERIFIED "
        f"model={model_name} rows={row_count} columns={len(columns)}"
    )
    return "published"


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        raise RuntimeError("at least one Bronze model is required")

    _required_environment("TRANSFORM_EXECUTION_ID")
    duck = _duckdb_connection()
    connection = _trino_connection()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT 1")
            if cursor.fetchall() not in ([[1]], [(1,)]):
                raise RuntimeError("private Trino readiness result is invalid")
            for model_name in argv:
                execute_model(duck, cursor, model_name)
        finally:
            cursor.close()
    finally:
        connection.close()
        duck.close()

    print(f"RAW_TO_BRONZE_PUBLICATION=PASS models={len(argv)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
