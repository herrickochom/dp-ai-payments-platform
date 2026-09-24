"""Part 4: binding (stub-safe)."""
from __future__ import annotations
import importlib, sys
import pytest
from test_nessie_plugin_contract_a import (
    ROOT, PLUGIN_PATH, MACRO_PATH, VALID_TR, SYNTHETIC_TOKEN, _load_plugin, _env, _FakeConn, _STUBBED,
)
WORKER_PATH = ROOT / "orchestration/job_runner/transform_worker.py"

@pytest.fixture(autouse=True)
def _restore_sys_modules_d():
    saved = {name: sys.modules.get(name) for name in _STUBBED}
    try:
        yield
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod

def test_create_secret_uses_parameter_binding(monkeypatch):
    duckdb = pytest.importorskip("duckdb")
    assert duckdb.__version__.startswith("1.5.5")
    module, _ = _load_plugin()
    _env(monkeypatch, branch=VALID_TR, token=SYNTHETIC_TOKEN)
    conn = _FakeConn()
    module.Plugin(name="p", plugin_config={}, credentials=None).configure_connection(conn)
    secret_sql, secret_params = conn.calls[0]
    assert "TOKEN ?" in secret_sql
    assert secret_params == [SYNTHETIC_TOKEN]
    assert SYNTHETIC_TOKEN not in secret_sql
    real = duckdb.connect(":memory:")
    real.execute("CREATE SECRET test_secret (TYPE iceberg, TOKEN ?)", [SYNTHETIC_TOKEN])

def test_attach_endpoint_uses_parameter_binding(monkeypatch):
    module, _ = _load_plugin()
    _env(monkeypatch, branch=VALID_TR, token=SYNTHETIC_TOKEN)
    conn = _FakeConn()
    module.Plugin(name="p", plugin_config={}, credentials=None).configure_connection(conn)
    attach_sql, attach_params = conn.calls[1]
    assert "ENDPOINT ?" in attach_sql
    assert attach_params and attach_params[0].endswith(f"/iceberg/{VALID_TR}")
    assert SYNTHETIC_TOKEN not in attach_sql
