from pathlib import Path

import orchestration.job_runner.transform_trino_runtime as runtime


def test_startup_diagnostics_redacts_transform_trino_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("TRANSFORM_TRINO_DIAGNOSTIC_CAP_BYTES", "16384")

    environment = {
        "TRINO_S3_ACCESS_KEY_ID": "test-access-key",
        "TRINO_S3_SECRET_ACCESS_KEY": "test-secret-key",
        "TRINO_KEYSTORE_PASSWORD": "test-keystore-password",
        "TRINO_INTERNAL_SHARED_SECRET": "test-internal-secret",
    }

    log = tmp_path / "startup.log"
    log.write_text(
        "access=test-access-key\n"
        "secret=test-secret-key\n"
        "keystore=test-keystore-password\n"
        "internal=test-internal-secret\n"
        "safe=Trino startup failed\n",
        encoding="utf-8",
    )

    result = runtime._startup_diagnostics(log, environment)

    assert "test-access-key" not in result
    assert "test-secret-key" not in result
    assert "test-keystore-password" not in result
    assert "test-internal-secret" not in result
    assert result.count("<REDACTED>") == 4
    assert "Trino startup failed" in result


def test_startup_diagnostics_are_bounded_to_tail(tmp_path, monkeypatch):
    monkeypatch.setenv("TRANSFORM_TRINO_DIAGNOSTIC_CAP_BYTES", "64")

    log = tmp_path / "startup.log"
    log.write_bytes(b"A" * 256 + b"TAIL_MARKER")

    result = runtime._startup_diagnostics(log, {})

    assert result.startswith("[truncated to last 64 bytes]")
    assert "TAIL_MARKER" in result
    assert "A" * 256 not in result


def test_startup_diagnostic_cap_has_hard_maximum(monkeypatch):
    monkeypatch.setenv(
        "TRANSFORM_TRINO_DIAGNOSTIC_CAP_BYTES",
        str(1024 * 1024),
    )

    assert runtime._diagnostic_cap_bytes() == 64 * 1024


def test_invalid_diagnostic_cap_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("TRANSFORM_TRINO_DIAGNOSTIC_CAP_BYTES", "invalid")

    assert runtime._diagnostic_cap_bytes() == 16 * 1024


def test_missing_startup_log_does_not_raise(tmp_path):
    result = runtime._startup_diagnostics(
        tmp_path / "missing.log",
        {},
    )

    assert result == "startup diagnostics unavailable"
