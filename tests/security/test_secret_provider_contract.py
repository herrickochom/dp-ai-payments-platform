"""Core secret-provider contract: provider neutrality and fail-closed selection.

Covers C10.3D.1 only. No caller has been migrated to the resolver yet, so
these tests exercise the contract in isolation: no network, no container, no
external secret manager, and no reliance on ambient secret configuration.
"""

from __future__ import annotations

import ast
import importlib.util
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from orchestration.job_runner.transform_execution import AUTHORITY_ENV, FORBIDDEN_ENV
from services.shared.security.secret_provider import (
    ALLOWED_SECRET_SOURCES,
    EnvSecretProvider,
    FileSecretProvider,
    SecretConfigurationError,
    SecretNameError,
    SecretProvider,
    SecretUnavailable,
    require_secret,
    resolve_secret,
    validate_secret_source,
)


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "services/shared/security/secret_provider.py"

PRESENT = "DP_CONTRACT_PRESENT"
ABSENT = "DP_CONTRACT_ABSENT_SECRET"
SENSITIVE_VALUE = "contract-canary-b7f1-not-a-real-secret"


@pytest.fixture(autouse=True)
def isolated_secret_configuration(monkeypatch):
    """Keep every test independent of ambient secret configuration."""
    for name in ("DP_SECURITY_MODE", "DP_SECRET_SOURCE", "DP_SECRET_DIR", PRESENT, ABSENT):
        monkeypatch.delenv(name, raising=False)


def test_1_env_provider_returns_configured_value(monkeypatch):
    monkeypatch.setenv(PRESENT, SENSITIVE_VALUE)

    assert EnvSecretProvider().get(PRESENT) == SENSITIVE_VALUE


def test_2_missing_env_secret_returns_none(monkeypatch):
    monkeypatch.delenv(ABSENT, raising=False)

    assert EnvSecretProvider().get(ABSENT) is None
    assert resolve_secret(ABSENT) is None


def test_3_require_secret_treats_empty_value_as_unavailable(monkeypatch):
    monkeypatch.setenv(PRESENT, "")

    with pytest.raises(SecretUnavailable, match=PRESENT):
        require_secret(PRESENT)


def test_4_local_resolution_preserves_environment_behaviour(monkeypatch):
    monkeypatch.setenv(PRESENT, SENSITIVE_VALUE)

    assert validate_secret_source() == "environment"
    assert resolve_secret(PRESENT) == SENSITIVE_VALUE
    assert require_secret(PRESENT) == SENSITIVE_VALUE
    assert resolve_secret(ABSENT) is None


def test_5_mounted_file_supplies_value(monkeypatch, tmp_path):
    (tmp_path / PRESENT).write_text(SENSITIVE_VALUE + "\n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))

    assert resolve_secret(PRESENT) == SENSITIVE_VALUE
    assert require_secret(PRESENT) == SENSITIVE_VALUE


@pytest.mark.parametrize("ending", ["\n", "\r\n", "\r"])
def test_6_only_trailing_line_endings_are_stripped(monkeypatch, tmp_path, ending):
    (tmp_path / PRESENT).write_text(SENSITIVE_VALUE + ending, encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))

    assert resolve_secret(PRESENT) == SENSITIVE_VALUE


def test_6b_surrounding_whitespace_is_preserved(monkeypatch, tmp_path):
    (tmp_path / PRESENT).write_text("  spaced value  \n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))

    assert resolve_secret(PRESENT) == "  spaced value  "


def test_7_mounted_file_wins_over_environment(monkeypatch, tmp_path):
    (tmp_path / PRESENT).write_text("file-value\n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))
    monkeypatch.setenv(PRESENT, "environment-value")

    assert resolve_secret(PRESENT) == "file-value"


def test_8_missing_file_falls_back_to_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))
    monkeypatch.setenv(PRESENT, "environment-value")

    assert resolve_secret(PRESENT) == "environment-value"


def test_8b_empty_file_is_unavailable_and_falls_back_to_environment(monkeypatch, tmp_path):
    (tmp_path / PRESENT).write_text("\n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))
    monkeypatch.setenv(PRESENT, "environment-value")

    assert resolve_secret(PRESENT) == "environment-value"


def test_9_unset_secret_dir_does_not_consult_the_filesystem(monkeypatch):
    monkeypatch.setenv(PRESENT, SENSITIVE_VALUE)

    def refuse(*_args, **_kwargs):
        raise AssertionError("filesystem must not be consulted without DP_SECRET_DIR")

    monkeypatch.setattr(FileSecretProvider, "__init__", refuse)
    monkeypatch.setattr(FileSecretProvider, "get", refuse)

    assert resolve_secret(PRESENT) == SENSITIVE_VALUE


@pytest.mark.parametrize(
    "name",
    ["../DP_CONTRACT_PRESENT", "..", "A/../B", "A/..", "./A", "A/B", "dir/name"],
)
def test_10_path_traversal_names_are_rejected(monkeypatch, tmp_path, name):
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))

    with pytest.raises(SecretNameError):
        resolve_secret(name)
    with pytest.raises(SecretNameError):
        EnvSecretProvider().get(name)
    with pytest.raises(SecretNameError):
        FileSecretProvider(str(tmp_path)).get(name)


@pytest.mark.parametrize(
    "name",
    ["", "/etc/passwd", "\\WINDOWS\\SYSTEM32", "/", "lower_case", "Mixed_Case",
     "WITH-DASH", "WITH.DOT", "WITH SPACE", "1LEADING_DIGIT"],
)
def test_11_absolute_path_like_and_non_environment_names_are_rejected(monkeypatch, tmp_path, name):
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))

    with pytest.raises(SecretNameError):
        resolve_secret(name)
    with pytest.raises(SecretNameError):
        require_secret(name)


@pytest.mark.parametrize("name", [None, 7, b"BYTES", ["LIST"]])
def test_11b_non_string_names_are_rejected(name):
    with pytest.raises(SecretNameError):
        resolve_secret(name)


def test_12_unavailable_error_names_the_secret_but_never_a_value(monkeypatch):
    monkeypatch.setenv(PRESENT, SENSITIVE_VALUE)

    with pytest.raises(SecretUnavailable) as excinfo:
        require_secret(ABSENT)

    message = str(excinfo.value)
    assert ABSENT in message
    assert SENSITIVE_VALUE not in message
    assert PRESENT not in message


def test_12b_invalid_name_error_never_echoes_the_supplied_text():
    supplied = SENSITIVE_VALUE.lower()

    with pytest.raises(SecretNameError) as excinfo:
        resolve_secret(supplied)

    assert supplied not in str(excinfo.value)


def test_13_exception_repr_str_and_args_carry_no_secret_value(monkeypatch):
    monkeypatch.setenv(PRESENT, SENSITIVE_VALUE)

    with pytest.raises(SecretUnavailable) as excinfo:
        require_secret(ABSENT)

    error = excinfo.value
    rendered = " ".join([str(error), repr(error), repr(error.args), f"{error.args!r}"])
    assert error.args and all(isinstance(argument, str) for argument in error.args)
    assert SENSITIVE_VALUE not in rendered


def test_13b_configuration_error_carries_no_secret_value(monkeypatch):
    monkeypatch.setenv(PRESENT, SENSITIVE_VALUE)
    monkeypatch.setenv("DP_SECRET_SOURCE", "unsupported-source")

    with pytest.raises(SecretConfigurationError) as excinfo:
        resolve_secret(PRESENT)

    error = excinfo.value
    rendered = " ".join([str(error), repr(error), repr(error.args)])
    assert SENSITIVE_VALUE not in rendered


def test_14_production_requires_an_explicit_secret_source(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(SecretConfigurationError, match="DP_SECRET_SOURCE"):
        validate_secret_source()
    with pytest.raises(SecretConfigurationError, match="DP_SECRET_SOURCE"):
        resolve_secret(PRESENT)
    with pytest.raises(SecretConfigurationError, match="DP_SECRET_SOURCE"):
        require_secret(PRESENT)

    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv(PRESENT, SENSITIVE_VALUE)

    assert validate_secret_source() == "environment"
    assert resolve_secret(PRESENT) == SENSITIVE_VALUE


def test_15_mounted_files_source_requires_a_valid_secret_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("DP_SECRET_SOURCE", "mounted-files")
    not_a_directory = tmp_path / "not-a-directory"
    not_a_directory.write_text("not a directory\n", encoding="utf-8")

    for directory in (
        "",
        "relative/secrets",
        str(tmp_path / "missing"),
        str(not_a_directory),
    ):
        monkeypatch.setenv("DP_SECRET_DIR", directory)
        with pytest.raises(SecretConfigurationError):
            validate_secret_source()

    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))
    assert validate_secret_source() == "mounted-files"


@pytest.mark.skipif(os.geteuid() == 0, reason="root bypasses directory permissions")
def test_15b_unreadable_secret_directory_fails_closed(monkeypatch, tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o000)
    try:
        monkeypatch.setenv("DP_SECRET_SOURCE", "mounted-files")
        monkeypatch.setenv("DP_SECRET_DIR", str(locked))
        with pytest.raises(SecretConfigurationError):
            validate_secret_source()
    finally:
        locked.chmod(0o700)


@pytest.mark.parametrize(
    "source",
    ["vault", "aws_secrets_manager", "aws-secrets-manager", "gcp", "azure", "file", "files", "none"],
)
def test_16_unsupported_secret_source_fails_closed(monkeypatch, source):
    assert source not in ALLOWED_SECRET_SOURCES
    monkeypatch.setenv("DP_SECRET_SOURCE", source)

    with pytest.raises(SecretConfigurationError, match="DP_SECRET_SOURCE"):
        validate_secret_source()
    with pytest.raises(SecretConfigurationError):
        resolve_secret(PRESENT)

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    with pytest.raises(SecretConfigurationError):
        resolve_secret(PRESENT)


ALLOWED_MODULE_IMPORTS = {
    "__future__",
    "os",
    "re",
    "pathlib",
    "typing",
    "services.shared.security.runtime_security",
}

FORBIDDEN_DEPENDENCY_PREFIXES = (
    "boto3", "botocore", "boto", "google", "azure", "hvac", "vault",
    "docker", "requests", "urllib3", "kubernetes", "confluent",
)


def _module_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    return imported


def test_17_provider_module_has_no_external_or_provider_dependency():
    imported = _module_imports(MODULE_PATH)

    assert imported <= ALLOWED_MODULE_IMPORTS
    for name in imported:
        assert not name.lower().startswith(FORBIDDEN_DEPENDENCY_PREFIXES), name


def test_18_module_and_resolution_emit_no_secret_material(monkeypatch, capsys):
    source = MODULE_PATH.read_text(encoding="utf-8")
    for token in ("print(", "logging", "logger", "sys.stderr", "sys.stdout"):
        assert token not in source, token

    monkeypatch.setenv(PRESENT, SENSITIVE_VALUE)
    assert resolve_secret(PRESENT) == SENSITIVE_VALUE

    with pytest.raises(SecretUnavailable):
        require_secret(ABSENT)

    captured = capsys.readouterr()
    assert SENSITIVE_VALUE not in captured.out + captured.err


def _object_store_identities():
    path = ROOT / "platform/minio/provision_object_store_identities.py"
    spec = importlib.util.spec_from_file_location("object_store_provisioner_contract", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.IDENTITIES


EXPECTED_AUTHORITY_ENV = {
    "ordinary_transform": frozenset({
        "ORDINARY_S3_ACCESS_KEY_ID", "ORDINARY_S3_SECRET_ACCESS_KEY", "NESSIE_TRANSFORM_TOKEN",
    }),
    "restricted_identity_transform": frozenset({
        "RESTRICTED_S3_ACCESS_KEY_ID", "RESTRICTED_S3_SECRET_ACCESS_KEY", "NESSIE_TRANSFORM_TOKEN",
    }),
    "ml_prediction_transform": frozenset({
        "ML_S3_ACCESS_KEY_ID", "ML_S3_SECRET_ACCESS_KEY", "NESSIE_TRANSFORM_TOKEN",
    }),
}

EXPECTED_FORBIDDEN_ENV = frozenset({
    "MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
})

EXPECTED_IDENTITIES = (
    ("raw_ingest", "raw-ingest", "RAW_INGEST_S3_ACCESS_KEY_ID", "RAW_INGEST_S3_SECRET_ACCESS_KEY"),
    ("cdc_quarantine", "cdc-quarantine", "CDC_QUARANTINE_S3_ACCESS_KEY_ID", "CDC_QUARANTINE_S3_SECRET_ACCESS_KEY"),
    ("platform_raw_read", "platform-raw-read", "PLATFORM_RAW_READ_ACCESS_KEY", "PLATFORM_RAW_READ_SECRET_KEY"),
    ("nessie_catalog", "nessie-catalog", "NESSIE_S3_ACCESS_KEY", "NESSIE_S3_SECRET_KEY"),
    ("trino_iceberg", "trino-iceberg-read", "TRINO_S3_ACCESS_KEY_ID", "TRINO_S3_SECRET_ACCESS_KEY"),
    ("ordinary_transform", "ordinary-transform", "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID", "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY"),
    ("restricted_identity_transform", "restricted-transform", "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID", "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY"),
    ("ml_prediction_transform", "ml-transform", "ML_TRANSFORM_S3_ACCESS_KEY_ID", "ML_TRANSFORM_S3_SECRET_ACCESS_KEY"),
)


def test_19_split_authority_constants_remain_unchanged():
    assert AUTHORITY_ENV == EXPECTED_AUTHORITY_ENV
    assert FORBIDDEN_ENV == EXPECTED_FORBIDDEN_ENV
    assert tuple(_object_store_identities()) == EXPECTED_IDENTITIES

    access_keys = {row[2] for row in EXPECTED_IDENTITIES}
    secret_keys = {row[3] for row in EXPECTED_IDENTITIES}
    assert len(access_keys) == 8 and len(secret_keys) == 8
    assert access_keys.isdisjoint(secret_keys)


def test_20_env_file_remains_untracked():
    git = shutil.which("git")
    if git is None:
        pytest.skip("git is unavailable")

    tracked = subprocess.run(
        [git, "ls-files", "--", ".env"], cwd=ROOT, text=True, capture_output=True, check=False,
    )
    assert tracked.returncode == 0
    assert tracked.stdout.strip() == ""

    ignored = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert any(line.strip() == ".env" for line in ignored)


def test_21_secret_values_are_never_cached(monkeypatch, tmp_path):
    monkeypatch.setenv(PRESENT, "first-value")
    assert resolve_secret(PRESENT) == "first-value"

    monkeypatch.setenv(PRESENT, "second-value")
    assert resolve_secret(PRESENT) == "second-value"

    (tmp_path / PRESENT).write_text("file-one\n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))
    assert resolve_secret(PRESENT) == "file-one"

    (tmp_path / PRESENT).write_text("file-two\n", encoding="utf-8")
    assert resolve_secret(PRESENT) == "file-two"


def test_22_a_name_resolving_outside_the_secret_directory_is_rejected(monkeypatch, tmp_path):
    outside = tmp_path / "outside.txt"
    outside.write_text("outside-value\n", encoding="utf-8")
    secret_dir = tmp_path / "secrets"
    secret_dir.mkdir()
    (secret_dir / PRESENT).symlink_to(outside)
    monkeypatch.setenv("DP_SECRET_DIR", str(secret_dir))

    with pytest.raises(SecretConfigurationError):
        resolve_secret(PRESENT)


def test_23_providers_satisfy_the_declared_protocol(tmp_path):
    assert isinstance(EnvSecretProvider(), SecretProvider)
    assert isinstance(FileSecretProvider(str(tmp_path)), SecretProvider)
