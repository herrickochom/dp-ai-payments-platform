import duckdb
import pytest

from orchestration.job_runner import raw_to_bronze_executor as executor
from orchestration.job_runner.transform_execution import build_transform_command


MODEL = "br_example"
EXECUTION_ID = "be_" + "a" * 32
WAREHOUSE = "s3://dp-ai-payment/warehouse"
LOCATION = WAREHOUSE + "/bronze/br_example-123"
PARQUET = LOCATION + f"/data/{EXECUTION_ID}-{MODEL}.parquet"


class ScriptedCursor:
    def __init__(self, results):
        self.results = list(results)
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)

    def fetchall(self):
        return self.results.pop(0)


def configure_storage(monkeypatch):
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "dp-ai-payment")
    monkeypatch.setenv("NESSIE_WAREHOUSE", WAREHOUSE)
    monkeypatch.setenv("TRANSFORM_EXECUTION_ID", EXECUTION_ID)


def transform_source_environment():
    return {
        "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID": "ordinary-access",
        "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY": "ordinary-secret",
        "NESSIE_TRANSFORM_TOKEN": "nessie-token",
        "DBT_TRINO_PASSWORD": "dbt-password",
        "S3_ENDPOINT": "http://minio:9000",
        "S3_USE_SSL": "false",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "dp-ai-payment",
        "RAW_ROOT": "raw",
        "RAW_VERSION": "v2",
        "RAW_PREFIX": "raw/v2",
        "WAREHOUSE_PREFIX": "warehouse",
        "WAREHOUSE_URI": WAREHOUSE,
        "NESSIE_WAREHOUSE": WAREHOUSE,
        "S3_PATH_STYLE_ACCESS": "true",
        "DBT_S3_URL_STYLE": "path",
    }


def test_governed_execution_id_crosses_raw_child_boundary():
    command = build_transform_command(
        "C4_RAW_02",
        EXECUTION_ID,
        transform_source_environment(),
    )
    assert command.environment["TRANSFORM_EXECUTION_ID"] == EXECUTION_ID


def test_source_contains_no_staging_or_replacement_architecture():
    source = executor.Path(executor.__file__).read_text()
    assert "_raw_to_bronze_staging" not in source
    assert "CREATE OR REPLACE" not in source
    assert "DROP TABLE" not in source


def render_test_model(monkeypatch, tmp_path, sql):
    monkeypatch.setattr(executor, "BRONZE_MODEL_DIR", tmp_path)
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "dp-ai-payment")
    monkeypatch.setenv("RAW_ROOT", "raw")
    (tmp_path / "br_example.sql").write_text(sql, encoding="utf-8")
    return executor._render_model_sql("br_example")


def test_single_line_extract_json_rendering(monkeypatch, tmp_path):
    rendered = render_test_model(
        monkeypatch,
        tmp_path,
        "{{ extract_json('parsed_event_data', '$.event_id') }}",
    )
    assert rendered == (
        "json_extract_string(parsed_event_data, '$.event_id')"
    )


def test_multiline_extract_json_rendering_matches_single_line(
    monkeypatch,
    tmp_path,
):
    rendered = render_test_model(
        monkeypatch,
        tmp_path,
        """{{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.transaction_id'
        ) }}""",
    )
    assert rendered == (
        "json_extract_string("
        "parsed_event_data, "
        "'$.xml.AgentTransaction.transaction_id')"
    )


def test_unrecognised_jinja_still_fails_closed(monkeypatch, tmp_path):
    with pytest.raises(RuntimeError, match="unsupported Jinja remains"):
        render_test_model(
            monkeypatch,
            tmp_path,
            "{{ ref('not_allowed') }}",
        )


def test_agent_transactions_model_renders_without_remaining_jinja(
    monkeypatch,
):
    monkeypatch.setattr(
        executor,
        "BRONZE_MODEL_DIR",
        executor.Path("transform/dbt/models/bronze").resolve(),
    )
    monkeypatch.setenv("OBJECT_STORE_BUCKET", "dp-ai-payment")
    monkeypatch.setenv("RAW_ROOT", "raw")

    rendered = executor._render_model_sql(
        "br_pdm_agent_transactions"
    )

    assert "{{" not in rendered
    assert "{%" not in rendered
    assert "{#" not in rendered
    assert (
        "json_extract_string("
        "parsed_event_data, "
        "'$.xml.AgentTransaction.transaction_id')"
    ) in rendered


def test_extracts_location_from_show_create(monkeypatch):
    configure_storage(monkeypatch)
    rows = [(
        "CREATE TABLE iceberg.bronze.br_example (id varchar) "
        "WITH (format = 'PARQUET', "
        f"location = '{LOCATION}')",
    )]
    assert executor.extract_table_location(rows) == LOCATION


@pytest.mark.parametrize(
    "location",
    (
        "https://dp-ai-payment/warehouse/table",
        "s3://user@dp-ai-payment/warehouse/table",
        "s3://dp-ai-payment/warehouse/../outside",
        "s3://dp-ai-payment/warehouse/%2e%2e/outside",
        "s3://dp-ai-payment/warehouse/table?query=yes",
        "s3://dp-ai-payment/warehouse/table#fragment",
        "s3://dp-ai-payment/",
        "not-a-uri",
    ),
)
def test_invalid_location_rejected(monkeypatch, location):
    configure_storage(monkeypatch)
    with pytest.raises(RuntimeError, match="location"):
        executor.validate_table_location(location)


def test_wrong_bucket_rejected(monkeypatch):
    configure_storage(monkeypatch)
    with pytest.raises(RuntimeError, match="location"):
        executor.validate_table_location(
            "s3://wrong-bucket/warehouse/bronze/table"
        )


def test_outside_warehouse_rejected(monkeypatch):
    configure_storage(monkeypatch)
    with pytest.raises(RuntimeError, match="outside"):
        executor.validate_table_location(
            "s3://dp-ai-payment/other/bronze/table"
        )


def test_show_create_requires_exactly_one_location(monkeypatch):
    configure_storage(monkeypatch)
    with pytest.raises(RuntimeError, match="ambiguous"):
        executor.extract_table_location([("CREATE TABLE x (id bigint)",)])


def test_deterministic_permanent_filename(monkeypatch):
    configure_storage(monkeypatch)
    assert executor.permanent_parquet_path(LOCATION, MODEL) == PARQUET


def test_execution_id_is_not_derived(monkeypatch):
    configure_storage(monkeypatch)
    monkeypatch.setenv("TRANSFORM_EXECUTION_ID", "transform_" + EXECUTION_ID)
    with pytest.raises(RuntimeError, match="execution identity"):
        executor.permanent_parquet_path(LOCATION, MODEL)


def test_create_table_has_no_location_or_replacement():
    cursor = ScriptedCursor([])
    executor._create_table(cursor, MODEL, (("event_id", "VARCHAR"),))
    statement = cursor.statements[0]
    assert "CREATE TABLE iceberg.bronze.\"br_example\"" in statement
    assert "CREATE OR REPLACE" not in statement
    assert "location" not in statement.lower()


def test_add_files_uses_exact_file_and_non_recursive_mode():
    cursor = ScriptedCursor([])
    executor._register_file(cursor, MODEL, PARQUET)
    statement = cursor.statements[0]
    assert f"location => '{PARQUET}'" in statement
    assert "recursive_directory => 'FAIL'" in statement
    assert "_raw_to_bronze_staging" not in statement


def test_table_absent_is_first_publication_decision():
    cursor = ScriptedCursor([[(0,)]])
    assert executor._table_exists(cursor, MODEL) is False


def test_same_execution_already_published_is_noop(monkeypatch, capsys):
    configure_storage(monkeypatch)
    duck = object()
    cursor = ScriptedCursor([
        [(1,)],
        [(f"CREATE TABLE x WITH (location = '{LOCATION}')",)],
        [(PARQUET, 3)],
        [("event_id", "varchar")],
        [(3,)],
    ])
    monkeypatch.setattr(
        executor,
        "transformation_metadata",
        lambda *_: ("select 1", 3, (("event_id", "VARCHAR"),)),
    )
    monkeypatch.setattr(
        executor,
        "_write_parquet",
        lambda *_: pytest.fail("retry rewrote permanent file"),
    )
    monkeypatch.setattr(
        executor,
        "_register_file",
        lambda *_: pytest.fail("retry called add_files twice"),
    )

    assert executor.execute_model(duck, cursor, MODEL) == "already_published"
    assert "RAW_TO_BRONZE_ALREADY_PUBLISHED" in capsys.readouterr().out
    assert not any("add_files" in sql for sql in cursor.statements)


@pytest.mark.parametrize(
    "files",
    (
        (),
        ((PARQUET, 2),),
        (("s3://dp-ai-payment/warehouse/other.parquet", 3),),
        ((PARQUET, 3), ("s3://dp-ai-payment/warehouse/other.parquet", 1)),
    ),
)
def test_ambiguous_existing_table_fails_closed(monkeypatch, files):
    configure_storage(monkeypatch)
    cursor = ScriptedCursor([
        [(1,)],
        [(f"CREATE TABLE x WITH (location = '{LOCATION}')",)],
        list(files),
    ])
    monkeypatch.setattr(
        executor,
        "transformation_metadata",
        lambda *_: ("select 1", 3, (("event_id", "VARCHAR"),)),
    )
    with pytest.raises(executor.AmbiguousPublicationState):
        executor.execute_model(object(), cursor, MODEL)


def test_existing_table_schema_difference_fails(monkeypatch):
    configure_storage(monkeypatch)
    cursor = ScriptedCursor([
        [(1,)],
        [(f"CREATE TABLE x WITH (location = '{LOCATION}')",)],
        [(PARQUET, 3)],
        [("different_column", "varchar")],
    ])
    monkeypatch.setattr(
        executor,
        "transformation_metadata",
        lambda *_: ("select 1", 3, (("event_id", "VARCHAR"),)),
    )
    with pytest.raises(RuntimeError, match="column mismatch"):
        executor.execute_model(object(), cursor, MODEL)


def test_row_mismatch_fails():
    cursor = ScriptedCursor([
        [("event_id", "varchar")],
        [(2,)],
    ])
    with pytest.raises(RuntimeError, match="row count mismatch"):
        executor._verify_table(
            cursor,
            MODEL,
            3,
            (("event_id", "VARCHAR"),),
        )


def test_column_mismatch_fails():
    cursor = ScriptedCursor([[("event_id", "varchar")]])
    with pytest.raises(RuntimeError, match="column mismatch"):
        executor._verify_table(
            cursor,
            MODEL,
            3,
            (
                ("event_id", "VARCHAR"),
                ("status", "VARCHAR"),
            ),
        )


def test_first_publication_writes_registers_and_verifies(monkeypatch):
    configure_storage(monkeypatch)
    cursor = ScriptedCursor([
        [(0,)],
        [(f"CREATE TABLE x WITH (location = '{LOCATION}')",)],
        [],
        [("event_id", "varchar")],
        [(3,)],
    ])
    calls = []
    monkeypatch.setattr(
        executor,
        "transformation_metadata",
        lambda *_: ("select 1", 3, (("event_id", "VARCHAR"),)),
    )
    monkeypatch.setattr(
        executor,
        "_permanent_file_exists",
        lambda *_: False,
    )
    monkeypatch.setattr(
        executor,
        "_write_parquet",
        lambda *args: calls.append(("write", args[-1])),
    )
    monkeypatch.setattr(
        executor,
        "_register_file",
        lambda *args: calls.append(("register", args[-1])),
    )

    assert executor.execute_model(object(), cursor, MODEL) == "published"
    assert calls == [("write", PARQUET), ("register", PARQUET)]


def test_existing_permanent_file_before_registration_fails(monkeypatch):
    configure_storage(monkeypatch)
    cursor = ScriptedCursor([
        [(0,)],
        [(f"CREATE TABLE x WITH (location = '{LOCATION}')",)],
        [],
    ])
    monkeypatch.setattr(
        executor,
        "transformation_metadata",
        lambda *_: ("select 1", 3, (("event_id", "VARCHAR"),)),
    )
    monkeypatch.setattr(
        executor,
        "_permanent_file_exists",
        lambda *_: True,
    )
    with pytest.raises(executor.AmbiguousPublicationState):
        executor.execute_model(object(), cursor, MODEL)


def test_accessible_parquet_object_exists(tmp_path):
    output = tmp_path / "existing.parquet"
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            "COPY (SELECT 1 AS value) TO ? (FORMAT PARQUET)",
            [str(output)],
        )
        assert executor._permanent_file_exists(
            connection,
            str(output),
        ) is True
    finally:
        connection.close()


def test_confirmed_http_404_means_object_is_absent():
    class Connection:
        def execute(self, statement, parameters):
            assert "parquet_file_metadata(?)" in statement
            assert "glob(" not in statement
            raise duckdb.HTTPException(
                "HTTP Error: HTTP 404 Not Found"
            )

    assert executor._permanent_file_exists(
        Connection(),
        PARQUET,
    ) is False


def test_non_404_http_error_fails_closed():
    class Connection:
        def execute(self, statement, parameters):
            raise duckdb.HTTPException(
                "HTTP Error: HTTP 403 Forbidden"
            )

    with pytest.raises(RuntimeError, match="unable to inspect"):
        executor._permanent_file_exists(Connection(), PARQUET)


def test_malformed_parquet_fails_closed(tmp_path):
    malformed = tmp_path / "malformed.parquet"
    malformed.write_text("not parquet", encoding="utf-8")
    connection = duckdb.connect(":memory:")
    try:
        with pytest.raises(RuntimeError, match="unable to inspect"):
            executor._permanent_file_exists(
                connection,
                str(malformed),
            )
    finally:
        connection.close()


def test_materialisation_uses_duckdb_without_python_rows(
    monkeypatch,
    tmp_path,
):
    output = tmp_path / "bronze.parquet"
    connection = duckdb.connect(":memory:")
    try:
        executor._write_parquet(
            connection,
            "select 7::integer as event_count",
            str(output),
        )
        assert connection.execute(
            "select event_count from read_parquet(?)",
            [str(output)],
        ).fetchall() == [(7,)]
    finally:
        connection.close()


def test_unsupported_duckdb_type_fails_closed():
    with pytest.raises(RuntimeError, match="unsupported DuckDB"):
        executor._trino_type("STRUCT(value VARCHAR)")


def test_secret_values_are_not_in_configuration_errors(monkeypatch):
    class Connection:
        def load_extension(self, _):
            pass

        def close(self):
            pass

    monkeypatch.setattr(
        executor.duckdb,
        "connect",
        lambda *args, **kwargs: Connection(),
    )
    secret = "never-print-this-secret"
    monkeypatch.setenv(
        "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY",
        secret,
    )
    monkeypatch.setenv("TRANSFORM_AUTHORITY", "not-authorized")
    with pytest.raises(RuntimeError) as exc:
        executor._duckdb_connection()
    assert secret not in str(exc.value)
