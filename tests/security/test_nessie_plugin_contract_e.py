"""Part 5: secrecy, lifecycle, alias, authority (stub-safe)."""
from __future__ import annotations
import importlib, sys, types
import pytest
from test_nessie_plugin_contract_a import (
    ROOT, PLUGIN_PATH, MACRO_PATH, VALID_TR, SYNTHETIC_TOKEN, _load_plugin, _env, _FakeConn, _STUBBED,
)
WORKER_PATH = ROOT / "orchestration/job_runner/transform_worker.py"

@pytest.fixture(autouse=True)
def _restore_sys_modules_e():
    saved = {name: sys.modules.get(name) for name in _STUBBED}
    try:
        yield
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod

def test_real_duckdb_attach_accepts_bound_endpoint():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SECRET test_secret (TYPE iceberg, TOKEN ?)", [SYNTHETIC_TOKEN])
    try:
        con.execute("ATTACH 'whX' AS lakehouse (TYPE iceberg, ENDPOINT ?, SECRET test_secret, ACCESS_DELEGATION_MODE none)", ["http://127.0.0.1:9/iceberg/branch"])
    except Exception as exc:
        assert SYNTHETIC_TOKEN not in str(exc)
        assert type(exc).__name__ != "BinderException"

def test_token_absent_from_sql_and_exceptions(monkeypatch):
    module, _ = _load_plugin()
    _env(monkeypatch, branch=VALID_TR, token=SYNTHETIC_TOKEN)
    conn = _FakeConn()
    module.Plugin(name="p", plugin_config={}, credentials=None).configure_connection(conn)
    for sql, _p in conn.calls:
        assert SYNTHETIC_TOKEN not in sql
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SECRET test_secret (TYPE iceberg, TOKEN ?)", [SYNTHETIC_TOKEN])
    try:
        con.execute("ATTACH 'whX' AS lakehouse (TYPE iceberg, ENDPOINT ?, SECRET test_secret, ACCESS_DELEGATION_MODE none)", ["http://127.0.0.1:9/iceberg/branch"])
    except Exception as exc:
        assert SYNTHETIC_TOKEN not in str(exc)

def test_secret_is_temporary_session_scoped():
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SECRET nessie_transform (TYPE iceberg, TOKEN ?)", [SYNTHETIC_TOKEN])
    rows = con.execute("SELECT name, type, persistent FROM duckdb_secrets()").fetchall()
    assert ("nessie_transform", "iceberg", False) in rows
    with pytest.raises(Exception, match="already exists"):
        con.execute("CREATE SECRET nessie_transform (TYPE iceberg, TOKEN ?)", [SYNTHETIC_TOKEN])
    other = duckdb.connect(":memory:")
    assert other.execute("SELECT name FROM duckdb_secrets()").fetchall() == []

def test_fixed_lakehouse_alias_agreement():
    plugin_source = PLUGIN_PATH.read_text()
    macro_source = MACRO_PATH.read_text()
    assert '_CATALOG_ALIAS = "lakehouse"' in plugin_source
    assert "AS {_CATALOG_ALIAS}" in plugin_source
    assert "database='lakehouse'" in macro_source or 'database="lakehouse"' in macro_source
    assert "target.database" not in macro_source

def test_no_root_or_aws_credentials_in_child_env():
    from orchestration.job_runner.transform_execution import build_transform_command
    source = {"ML_S3_ACCESS_KEY_ID": "ml-key", "ML_S3_SECRET_ACCESS_KEY": "ml-secret", "NESSIE_TRANSFORM_TOKEN": "catalog-token", "S3_ENDPOINT": "http://minio:9000", "S3_USE_SSL": "false", "OBJECT_STORE_REGION": "us-east-1", "OBJECT_STORE_BUCKET": "dp-ai-payment", "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2", "WAREHOUSE_PREFIX": "warehouse", "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse", "DBT_S3_URL_STYLE": "path", "NESSIE_ENDPOINT": "http://nessie:19120", "MINIO_ROOT_USER": "x", "MINIO_ROOT_PASSWORD": "x", "AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x"}
    env = build_transform_command("C4_ML_01", "be_" + "a" * 32, source).environment
    for forbidden in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        assert forbidden not in env
    assert env["NESSIE_TRANSFORM_TOKEN"] == "catalog-token"

def test_plugin_never_interpolates_token_into_sql():
    source = PLUGIN_PATH.read_text()
    assert "TOKEN ?" in source and "ENDPOINT ?" in source
    assert "TOKEN {" not in source
