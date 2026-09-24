"""Part 3: branch/warehouse/token validation (stub-safe)."""
from __future__ import annotations
import sys
import pytest
from test_nessie_plugin_contract_a import VALID_TR, VALID_BE, SYNTHETIC_TOKEN, _load_plugin, _env, _FakeConn, _STUBBED

@pytest.fixture(autouse=True)
def _restore_sys_modules_c():
    saved = {name: sys.modules.get(name) for name in _STUBBED}
    try:
        yield
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod

@pytest.mark.parametrize("branch", [VALID_TR, VALID_BE])
def test_valid_execution_branches_accepted(monkeypatch, branch):
    module, _ = _load_plugin()
    _env(monkeypatch, branch=branch)
    conn = _FakeConn()
    module.Plugin(name="p", plugin_config={}, credentials=None).configure_connection(conn)
    assert len(conn.calls) == 2

@pytest.mark.parametrize("branch", ["main", "", "transform_tr_short", "transform_be_" + "Z" * 32, "transform_tr_" + "b" * 32 + ";DROP", "transform_tr_" + "b" * 32 + "/evil"])
def test_invalid_branches_rejected(monkeypatch, branch):
    module, _ = _load_plugin()
    _env(monkeypatch, branch=branch)
    with pytest.raises(ValueError, match="invalid execution-scoped Nessie branch"):
        module.Plugin(name="p", plugin_config={}, credentials=None).configure_connection(_FakeConn())

def test_malformed_execution_ids_rejected():
    from orchestration.job_runner.transform_execution import build_transform_command
    source = {"ML_S3_ACCESS_KEY_ID": "k", "ML_S3_SECRET_ACCESS_KEY": "s", "NESSIE_TRANSFORM_TOKEN": "t", "NESSIE_ENDPOINT": "http://nessie:19120", "S3_ENDPOINT": "http://minio:9000", "S3_USE_SSL": "false", "OBJECT_STORE_REGION": "us-east-1", "OBJECT_STORE_BUCKET": "dp-ai-payment", "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2", "WAREHOUSE_PREFIX": "warehouse", "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse", "DBT_S3_URL_STYLE": "path"}
    with pytest.raises(ValueError, match="invalid execution identity"):
        build_transform_command("C4_ML_01", "../../escape", source)

def test_warehouse_validation(monkeypatch):
    module, _ = _load_plugin()
    _env(monkeypatch, branch=VALID_TR)
    monkeypatch.setenv("NESSIE_WAREHOUSE", "../evil warehouse!")
    with pytest.raises(ValueError, match="invalid Nessie warehouse"):
        module.Plugin(name="p", plugin_config={}, credentials=None).configure_connection(_FakeConn())

def test_missing_token_fails_closed(monkeypatch):
    module, _ = _load_plugin()
    _env(monkeypatch, branch=VALID_TR, token="")
    with pytest.raises(ValueError):
        module.Plugin(name="p", plugin_config={}, credentials=None).configure_connection(_FakeConn())

def test_nessie_security_validator_invoked(monkeypatch):
    module, calls = _load_plugin()
    _env(monkeypatch, branch=VALID_TR)
    module.Plugin(name="p", plugin_config={}, credentials=None).configure_connection(_FakeConn())
    assert calls["endpoint"] == "http://nessie:19120"
    assert calls["token"] == SYNTHETIC_TOKEN
