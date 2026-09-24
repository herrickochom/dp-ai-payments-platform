"""Part 2: importability + constructor (stub-safe, restores sys.modules)."""
from __future__ import annotations
import importlib, sys, types
import pytest
from test_nessie_plugin_contract_a import (
    ROOT, PLUGIN_PATH, PROFILE_PATH, MACRO_PATH,
    VALID_TR, VALID_BE, SYNTHETIC_TOKEN, _load_plugin, _env, _FakeConn, _STUBBED,
)
WORKER_PATH = ROOT / "orchestration/job_runner/transform_worker.py"

@pytest.fixture(autouse=True)
def _restore_sys_modules_b():
    saved = {name: sys.modules.get(name) for name in _STUBBED}
    try:
        yield
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod

def test_plugin_importable_with_module_paths(monkeypatch):
    profile = PROFILE_PATH.read_text()
    assert "module_paths:" in profile and "/app/job_runner" in profile
    assert "sys.path" not in PLUGIN_PATH.read_text()
    monkeypatch.syspath_prepend(str(PLUGIN_PATH.parent))
    for dep in ("dbt", "dbt.adapters", "dbt.adapters.duckdb", "dbt.adapters.duckdb.plugins", "services", "services.shared", "services.shared.security", "services.shared.security.runtime_security"):
        sys.modules.pop(dep, None)
    pkg = types.ModuleType("dbt"); pkg.__path__ = []
    ad = types.ModuleType("dbt.adapters"); ad.__path__ = []
    dd = types.ModuleType("dbt.adapters.duckdb"); dd.__path__ = []
    plug = types.ModuleType("dbt.adapters.duckdb.plugins")
    class BasePlugin:
        def __init__(self, name, plugin_config, credentials=None):
            self.name = name
    plug.BasePlugin = BasePlugin
    svc = types.ModuleType("services"); svc.__path__ = []
    sh = types.ModuleType("services.shared"); sh.__path__ = []
    sec = types.ModuleType("services.shared.security"); sec.__path__ = []
    run = types.ModuleType("services.shared.security.runtime_security")
    run.validate_nessie_security = lambda *a, **k: None
    sys.modules.update({"dbt": pkg, "dbt.adapters": ad, "dbt.adapters.duckdb": dd, "dbt.adapters.duckdb.plugins": plug, "services": svc, "services.shared": sh, "services.shared.security": sec, "services.shared.security.runtime_security": run})
    sys.modules.pop("nessie_iceberg_plugin", None)
    module = importlib.import_module("nessie_iceberg_plugin")
    assert hasattr(module, "Plugin")
    sys.modules.pop("nessie_iceberg_plugin", None)

def test_base_plugin_create_loads_plugin_module():
    module, _ = _load_plugin()
    # Pinned dbt-duckdb 1.9.3 contract: create(module, *, config=None,
    # alias=None, credentials=None). Only module may be passed positionally;
    # the earlier diagnostic BasePlugin.create('m', {}, None) was invalid
    # because config/credentials are keyword-only.
    import inspect
    assert list(inspect.signature(module.BasePlugin.create).parameters) == [
        "module", "config", "alias", "credentials",
    ]
    created = module.BasePlugin.create("nessie_iceberg_plugin_isolated")
    assert isinstance(created, module.Plugin)
    created_kw = module.BasePlugin.create(module="nessie_iceberg_plugin_isolated", config={})
    assert isinstance(created_kw, module.Plugin)

def test_plugin_constructor_compatibility():
    module, _ = _load_plugin()
    plugin = module.Plugin(name="nessie_iceberg_plugin", plugin_config={}, credentials=None)
    assert plugin.name == "nessie_iceberg_plugin"
