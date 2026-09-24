"""Part 1: helpers + importability/validation (stub-safe, restores sys.modules)."""
from __future__ import annotations
import importlib, importlib.util, sys, types
from pathlib import Path
import pytest
ROOT = Path(__file__).resolve().parents[2]
PLUGIN_PATH = ROOT / "orchestration/job_runner/nessie_iceberg_plugin.py"
PROFILE_PATH = ROOT / "transform/dbt/profiles.yml"
MACRO_PATH = ROOT / "transform/dbt/macros/iceberg_table.sql"
VALID_TR = "transform_tr_" + "b" * 32
VALID_BE = "transform_be_" + "a" * 32
SYNTHETIC_TOKEN = "synthetic-test-token-xyz-12345"
_STUBBED = ("dbt", "dbt.adapters", "dbt.adapters.duckdb", "dbt.adapters.duckdb.plugins", "services", "services.shared", "services.shared.security", "services.shared.security.runtime_security", "nessie_iceberg_plugin_isolated", "nessie_iceberg_plugin")

@pytest.fixture(autouse=True)
def _restore_sys_modules():
    saved = {name: sys.modules.get(name) for name in _STUBBED}
    try:
        yield
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod

def _install_stubs(calls):
    dbt_pkg = types.ModuleType("dbt")
    adapters_pkg = types.ModuleType("dbt.adapters")
    duckdb_pkg = types.ModuleType("dbt.adapters.duckdb")
    plugins_pkg = types.ModuleType("dbt.adapters.duckdb.plugins")
    class BasePlugin:
        def __init__(self, name, plugin_config, credentials=None):
            self.name = name
            self.initialize(plugin_config)
        def initialize(self, plugin_config):
            pass
        @classmethod
        def create(cls, module, *, config=None, alias=None, credentials=None):
            mod = importlib.import_module(module)
            return mod.Plugin(name=alias or module, plugin_config=config or {}, credentials=credentials)
    plugins_pkg.BasePlugin = BasePlugin
    sec_pkg = types.ModuleType("services")
    shared_pkg = types.ModuleType("services.shared")
    security_pkg = types.ModuleType("services.shared.security")
    runtime_pkg = types.ModuleType("services.shared.security.runtime_security")
    def fake_validate(endpoint, *, auth_mode=None, token=None):
        calls["endpoint"] = endpoint
        calls["auth_mode"] = auth_mode
        calls["token"] = token
    runtime_pkg.validate_nessie_security = fake_validate
    for name, mod in {"dbt": dbt_pkg, "dbt.adapters": adapters_pkg, "dbt.adapters.duckdb": duckdb_pkg, "dbt.adapters.duckdb.plugins": plugins_pkg, "services": sec_pkg, "services.shared": shared_pkg, "services.shared.security": security_pkg, "services.shared.security.runtime_security": runtime_pkg}.items():
        sys.modules[name] = mod

def _load_plugin():
    source = PLUGIN_PATH.read_text()
    assert "sys.path" not in source
    calls: dict = {}
    _install_stubs(calls)
    spec = importlib.util.spec_from_file_location("nessie_iceberg_plugin_isolated", PLUGIN_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["nessie_iceberg_plugin_isolated"] = module
    spec.loader.exec_module(module)
    return module, calls

def _env(monkeypatch, *, branch=VALID_TR, token=SYNTHETIC_TOKEN, endpoint="http://nessie:19120", warehouse="dp-ai-payment"):
    monkeypatch.setenv("NESSIE_ENDPOINT", endpoint)
    monkeypatch.setenv("NESSIE_TRANSFORM_TOKEN", token)
    monkeypatch.setenv("DBT_NESSIE_BRANCH", branch)
    monkeypatch.setenv("NESSIE_WAREHOUSE", warehouse)
    monkeypatch.delenv("NESSIE_AUTH_MODE", raising=False)

class _FakeConn:
    def __init__(self):
        self.calls = []
    def execute(self, sql, params=None):
        self.calls.append((sql, list(params) if params else None))
        return self
