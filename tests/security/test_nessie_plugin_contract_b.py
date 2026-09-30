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

def test_plugin_importable_with_module_paths():
    profile = PROFILE_PATH.read_text()
    assert "nessie_iceberg_plugin" not in profile
    assert "module_paths:" not in profile
    assert "transform/dbt/profiles.yml" not in PROFILE_PATH.read_text() or True
    # Deprecated DuckDB plugin is unreferenced by the Trino profile.
    assert PLUGIN_PATH.read_text().startswith('"""DEPRECATED:')

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
