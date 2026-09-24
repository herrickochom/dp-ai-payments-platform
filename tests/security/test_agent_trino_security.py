"""Production transport-security boundary tests for Agent API -> Trino."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"

if str(AGENT_API_DIR) not in sys.path:
    sys.path.insert(0, str(AGENT_API_DIR))


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


config = load_module(
    "agent_trino_security_config",
    AGENT_API_DIR / "config.py",
)

# trino_gateway imports `Settings` through the local module name `config`.
# Supply the synthetic config only while loading the gateway, then restore
# the interpreter's prior module state so this test cannot contaminate
# unrelated Agent API tests during pytest collection.
_previous_config = sys.modules.get("config")
try:
    sys.modules["config"] = config
    gateway_module = load_module(
        "agent_trino_security_gateway",
        AGENT_API_DIR / "trino_gateway.py",
    )
finally:
    if _previous_config is None:
        sys.modules.pop("config", None)
    else:
        sys.modules["config"] = _previous_config

Settings = config.Settings
TrinoGateway = gateway_module.TrinoGateway


def test_development_http_reaches_trino_connect(monkeypatch):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)

    calls = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(
        gateway_module.trino.dbapi,
        "connect",
        fake_connect,
    )

    settings = Settings(
        trino_http_scheme="http",
        trino_tls_ca=None,
    )

    connection = TrinoGateway(settings)._connection()

    assert connection is not None
    assert len(calls) == 1
    assert calls[0]["http_scheme"] == "http"
    assert "verify" not in calls[0]


def test_production_http_fails_before_trino_connect(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    calls = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(
        gateway_module.trino.dbapi,
        "connect",
        fake_connect,
    )

    settings = Settings(
        trino_http_scheme="http",
        trino_tls_ca="/run/secrets/trino-ca.pem",
    )

    with pytest.raises(
        ValueError,
        match="Trino clients must use HTTPS",
    ):
        TrinoGateway(settings)._connection()

    assert calls == []


def test_production_https_without_ca_fails_before_trino_connect(
    monkeypatch,
):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    calls = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(
        gateway_module.trino.dbapi,
        "connect",
        fake_connect,
    )

    settings = Settings(
        trino_http_scheme="https",
        trino_tls_ca=None,
    )

    with pytest.raises(
        ValueError,
        match="TRINO_TLS_CA is required",
    ):
        TrinoGateway(settings)._connection()

    assert calls == []


def test_production_https_with_ca_reaches_verified_connect(
    monkeypatch,
):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    calls = []

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(
        gateway_module.trino.dbapi,
        "connect",
        fake_connect,
    )

    ca_path = "/run/secrets/trino-ca.pem"

    settings = Settings(
        trino_http_scheme="https",
        trino_tls_ca=ca_path,
    )

    connection = TrinoGateway(settings)._connection()

    assert connection is not None
    assert len(calls) == 1
    assert calls[0]["http_scheme"] == "https"
    assert calls[0]["verify"] == ca_path
