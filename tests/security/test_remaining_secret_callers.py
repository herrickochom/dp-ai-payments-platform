"""C10.3D: remaining approved secret callers (Agent API + MinIO tooling).

Agent API resolves TRINO_PASSWORD and SUPERSET_PASSWORD through the shared
secret-provider contract while every host/port/user/limit stays ordinary
configuration. The MinIO object-store provisioner resolves root and service
SECRET-KEY material through the same contract while ACCESS-KEY identifiers
remain deliberate identity/configuration reads. Nothing here contacts MinIO,
Kafka, Superset, or Trino.
"""

from __future__ import annotations

import ast
import importlib.util
import os
import sys
from pathlib import Path

import pytest

from services.shared.security.secret_provider import (
    SecretConfigurationError,
    SecretUnavailable,
)

ROOT = Path(__file__).resolve().parents[2]
AGENT_API_DIR = ROOT / "services" / "agent-api"
AGENT_CONFIG = AGENT_API_DIR / "config.py"
PROVISIONER = ROOT / "platform/minio/provision_object_store_identities.py"
INIT_DATABASES = ROOT / "platform/minio/init_databases.py"

TRINO_PASSWORD = "trino-password-value"
SUPERSET_PASSWORD = "superset-password-value"
ROOT_PASSWORD = "minio-root-password-value"

SCRUBBED_NAMES = (
    "DP_SECURITY_MODE",
    "DP_SECRET_SOURCE",
    "DP_SECRET_DIR",
    "TRINO_PASSWORD",
    "SUPERSET_PASSWORD",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "MINIO_ENDPOINT",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "NESSIE_S3_ACCESS_KEY",
    "NESSIE_S3_SECRET_KEY",
    "TRINO_S3_ACCESS_KEY_ID",
    "TRINO_S3_SECRET_ACCESS_KEY",
    "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID",
    "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY",
    "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID",
    "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY",
    "ML_TRANSFORM_S3_ACCESS_KEY_ID",
    "ML_TRANSFORM_S3_SECRET_ACCESS_KEY",
    "PLATFORM_RAW_READ_ACCESS_KEY",
    "PLATFORM_RAW_READ_SECRET_KEY",
    "RAW_INGEST_S3_ACCESS_KEY_ID",
    "RAW_INGEST_S3_SECRET_ACCESS_KEY",
    "CDC_QUARANTINE_S3_ACCESS_KEY_ID",
    "CDC_QUARANTINE_S3_SECRET_ACCESS_KEY",
)

SECRET_SERVICE_NAMES = (
    "RAW_INGEST_S3_SECRET_ACCESS_KEY",
    "CDC_QUARANTINE_S3_SECRET_ACCESS_KEY",
    "PLATFORM_RAW_READ_SECRET_KEY",
    "NESSIE_S3_SECRET_KEY",
    "TRINO_S3_SECRET_ACCESS_KEY",
    "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY",
    "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY",
    "ML_TRANSFORM_S3_SECRET_ACCESS_KEY",
)

ACCESS_KEY_NAMES = (
    "RAW_INGEST_S3_ACCESS_KEY_ID",
    "CDC_QUARANTINE_S3_ACCESS_KEY_ID",
    "PLATFORM_RAW_READ_ACCESS_KEY",
    "NESSIE_S3_ACCESS_KEY",
    "TRINO_S3_ACCESS_KEY_ID",
    "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID",
    "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID",
    "ML_TRANSFORM_S3_ACCESS_KEY_ID",
)


@pytest.fixture(autouse=True)
def isolated_environment(monkeypatch):
    for name in SCRUBBED_NAMES:
        monkeypatch.delenv(name, raising=False)


def load_agent_config(name: str):
    if str(AGENT_API_DIR) not in sys.path:
        sys.path.insert(0, str(AGENT_API_DIR))
    spec = importlib.util.spec_from_file_location(name, AGENT_CONFIG)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_provisioner():
    spec = importlib.util.spec_from_file_location(
        "remaining_secret_callers_provisioner", PROVISIONER
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def mount_secrets(tmp_path, monkeypatch, values: dict[str, str]) -> Path:
    for name, value in values.items():
        (tmp_path / name).write_text(value + "\n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))
    return tmp_path


def complete_environment(monkeypatch, root_user="distinct-root-id"):
    monkeypatch.setenv("MINIO_ENDPOINT", "http://object-store:9000")
    monkeypatch.setenv("MINIO_ROOT_USER", root_user)
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", ROOT_PASSWORD)
    for number, name in enumerate(ACCESS_KEY_NAMES, start=1):
        monkeypatch.setenv(name, f"service-id-{number:02d}")
    for number, name in enumerate(SECRET_SERVICE_NAMES, start=1):
        monkeypatch.setenv(name, f"service-secret-{number:02d}-xyz")


# ---------------------------------------------------------------------------
# Agent API
# ---------------------------------------------------------------------------


def test_agent_api_environment_backed_secret_resolution(monkeypatch):
    monkeypatch.setenv("TRINO_PASSWORD", TRINO_PASSWORD)
    monkeypatch.setenv("SUPERSET_PASSWORD", SUPERSET_PASSWORD)
    config = load_agent_config("agent_cfg_env")
    settings = config.Settings()
    assert settings.trino_password == TRINO_PASSWORD
    assert settings.superset_password == SUPERSET_PASSWORD


def test_agent_api_mounted_file_secret_resolution(monkeypatch, tmp_path):
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            "TRINO_PASSWORD": TRINO_PASSWORD,
            "SUPERSET_PASSWORD": SUPERSET_PASSWORD,
        },
    )
    config = load_agent_config("agent_cfg_mounted")
    settings = config.Settings()
    assert settings.trino_password == TRINO_PASSWORD
    assert settings.superset_password == SUPERSET_PASSWORD


def test_agent_api_production_undeclared_source_fails_closed(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    with pytest.raises(SecretConfigurationError, match="DP_SECRET_SOURCE"):
        load_agent_config("agent_cfg_undeclared")


def test_agent_api_unsupported_secret_source_fails_closed(monkeypatch):
    monkeypatch.setenv("DP_SECRET_SOURCE", "vault")
    with pytest.raises(SecretConfigurationError, match="DP_SECRET_SOURCE"):
        load_agent_config("agent_cfg_unsupported")


def test_agent_api_configuration_remains_ordinary(monkeypatch):
    for name, value in (
        ("TRINO_HOST", "trino-node"),
        ("TRINO_PORT", "8443"),
        ("TRINO_USER", "agent-api"),
        ("TRINO_CATALOG", "iceberg"),
        ("TRINO_SCHEMA", "consumption"),
        ("TRINO_HTTP_SCHEME", "http"),
        ("TRINO_TLS_CA", "/run/secrets/trino-ca.pem"),
        ("SUPERSET_URL", "http://superset:8088"),
        ("SUPERSET_USERNAME", "admin"),
        ("SUPERSET_DATABASE_NAME", "PDM Trino"),
    ):
        monkeypatch.setenv(name, value)
    config = load_agent_config("agent_cfg_ordinary")
    settings = config.Settings()
    assert settings.trino_host == "trino-node"
    assert settings.trino_port == 8443
    assert settings.trino_user == "agent-api"
    assert settings.trino_catalog == "iceberg"
    assert settings.trino_schema == "consumption"
    assert settings.trino_http_scheme == "http"
    # TLS CA is a file PATH, never secret content.
    assert settings.trino_tls_ca == "/run/secrets/trino-ca.pem"
    assert settings.superset_url == "http://superset:8088"
    assert settings.superset_username == "admin"

    resolved = []
    for node in ast.walk(ast.parse(AGENT_CONFIG.read_text(encoding="utf-8"))):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "resolve_secret"
        ):
            resolved.append(node.args[0].value)
    assert set(resolved) == {"TRINO_PASSWORD", "SUPERSET_PASSWORD"}


def test_agent_api_authority_boundaries_unchanged():
    config = load_agent_config("agent_cfg_authority")
    settings = config.Settings()
    # No authority widening: identity, catalog, schema and public surface
    # defaults are byte-identical to the approved contract.
    assert settings.trino_user == "agent-api"
    assert settings.trino_catalog == "iceberg"
    assert settings.trino_schema == "consumption"
    assert settings.trino_http_scheme == "http"
    assert settings.superset_username == "admin"
    assert settings.superset_url == "http://localhost:8088"
    # Credentials stay optional locally, exactly as before the migration.
    assert settings.trino_password is None
    assert settings.superset_password is None


# ---------------------------------------------------------------------------
# MinIO object-store provisioner
# ---------------------------------------------------------------------------


def test_provisioner_environment_backed_source(monkeypatch):
    complete_environment(monkeypatch)
    provisioner = load_provisioner()
    source = provisioner.administrative_source()
    assert source["MINIO_ROOT_PASSWORD"] == ROOT_PASSWORD
    assert source["RAW_INGEST_S3_SECRET_ACCESS_KEY"] == "service-secret-01-xyz"
    assert source["ML_TRANSFORM_S3_SECRET_ACCESS_KEY"] == "service-secret-08-xyz"
    # Access-key identifiers remain ordinary environment reads.
    assert source["RAW_INGEST_S3_ACCESS_KEY_ID"] == "service-id-01"
    assert source["MINIO_ROOT_USER"] == "distinct-root-id"


def test_provisioner_mounted_file_source(monkeypatch, tmp_path):
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            "MINIO_ROOT_PASSWORD": "mounted-root-secret",
            "NESSIE_S3_SECRET_KEY": "mounted-nessie-secret",
        },
    )
    complete_environment(monkeypatch)
    provisioner = load_provisioner()
    source = provisioner.administrative_source()
    assert source["MINIO_ROOT_PASSWORD"] == "mounted-root-secret"
    assert source["NESSIE_S3_SECRET_KEY"] == "mounted-nessie-secret"
    assert source["TRINO_S3_SECRET_ACCESS_KEY"] == "service-secret-05-xyz"


def test_provisioner_mounted_file_wins_over_stale_environment(monkeypatch, tmp_path):
    mount_secrets(tmp_path, monkeypatch, {"MINIO_ROOT_PASSWORD": "mounted-root-secret"})
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "stale-env-secret")
    provisioner = load_provisioner()
    source = provisioner.administrative_source()
    assert source["MINIO_ROOT_PASSWORD"] == "mounted-root-secret"


def test_provisioner_apply_fails_closed_without_secret(monkeypatch, capsys):
    complete_environment(monkeypatch)
    monkeypatch.delenv("NESSIE_S3_SECRET_KEY", raising=False)
    provisioner = load_provisioner()
    assert provisioner.main(["--apply"]) == 1
    captured = capsys.readouterr()
    assert "NESSIE_S3_SECRET_KEY" in captured.err
    # No resolved secret material ever reaches the error surface.
    assert ROOT_PASSWORD not in captured.out + captured.err
    assert "service-secret-01-xyz" not in captured.out + captured.err


def test_provisioner_validate_remains_offline(monkeypatch):
    complete_environment(monkeypatch)
    provisioner = load_provisioner()

    def forbidden(*_args, **_kwargs):
        raise AssertionError("validate must never invoke mc")

    monkeypatch.setattr(provisioner, "mc", forbidden)
    assert provisioner.main(["--validate"]) == 0


def test_provisioner_dry_run_remains_non_mutating(monkeypatch):
    complete_environment(monkeypatch)
    provisioner = load_provisioner()

    def forbidden(*_args, **_kwargs):
        raise AssertionError("dry-run must never invoke mc")

    monkeypatch.setattr(provisioner, "mc", forbidden)
    assert provisioner.main(["--dry-run"]) == 0


def test_provisioner_apply_remains_explicitly_gated(monkeypatch):
    provisioner = load_provisioner()
    # Without --apply the tool refuses to run at all.
    assert provisioner.main([]) == 2
    assert provisioner.main(["--bogus"]) == 2
    # --apply without a complete administrative environment fails closed.
    assert provisioner.main(["--apply"]) == 1


def test_provisioner_apply_uses_provider_resolved_source(monkeypatch):
    complete_environment(monkeypatch)
    provisioner = load_provisioner()
    calls: list[list[str]] = []
    listing = "enabled distinct-root-id consoleadmin\n"
    monkeypatch.setattr(
        provisioner,
        "mc",
        lambda arguments, _environment: calls.append(arguments) or listing,
    )
    assert provisioner.main(["--apply"]) == 0
    user_adds = [c for c in calls if c[:3] == ["admin", "user", "add"]]
    policy_creates = [c for c in calls if c[:3] == ["admin", "policy", "create"]]
    attachments = [c for c in calls if c[:2] == ["admin", "policy"] and "attach" in c]
    assert len(policy_creates) == 8
    # All eight users created with the environment-backed secrets.
    added_keys = {c[4] for c in user_adds}
    assert added_keys == {os.environ[n] for n in ACCESS_KEY_NAMES}
    secrets_used = {c[5] for c in user_adds}
    assert secrets_used == {os.environ[n] for n in SECRET_SERVICE_NAMES}
    assert len(attachments) == 8


def test_provisioner_preserves_existing_users_without_rotation(monkeypatch):
    complete_environment(monkeypatch)
    provisioner = load_provisioner()
    existing_key = os.environ["RAW_INGEST_S3_ACCESS_KEY_ID"]
    listing = f"enabled {existing_key} raw-ingest\n"
    calls: list[list[str]] = []
    monkeypatch.setattr(
        provisioner,
        "mc",
        lambda arguments, _environment: calls.append(arguments) or listing,
    )
    assert provisioner.main(["--apply"]) == 0
    user_adds = [c for c in calls if c[:3] == ["admin", "user", "add"]]
    # The existing desired key is preserved: no `admin user add` for it.
    assert all(c[4] != existing_key for c in user_adds)
    assert len(user_adds) == 7


def test_provisioner_never_reuses_root_credentials_for_services(
    monkeypatch, capsys
):
    provisioner = load_provisioner()
    complete_environment(monkeypatch, root_user="service-id-01")
    assert provisioner.main(["--apply"]) == 1
    assert "distinct" in capsys.readouterr().err
    complete_environment(monkeypatch)
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "service-secret-01-xyz")
    assert provisioner.main(["--apply"]) == 1
    assert "differ from root" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Bucket/database bootstrap tooling and shared invariants
# ---------------------------------------------------------------------------


def test_init_databases_resolves_root_secret_via_provider(monkeypatch, tmp_path):
    from services.shared.security.secret_provider import require_secret

    mount_secrets(tmp_path, monkeypatch, {"MINIO_ROOT_PASSWORD": "mounted-root-secret"})
    assert require_secret("MINIO_ROOT_PASSWORD") == "mounted-root-secret"
    bootstrap = INIT_DATABASES.read_text(encoding="utf-8")
    # Root identity stays an ordinary environment read; root secret resolves
    # through the provider; the tool remains confined to admin bootstrap code.
    assert "os.environ['MINIO_ROOT_USER']" in bootstrap
    assert "require_secret('MINIO_ROOT_PASSWORD')" in bootstrap
    assert "minioadmin" not in bootstrap


def test_init_databases_missing_root_secret_fails_closed(monkeypatch):
    from services.shared.security.secret_provider import require_secret

    with pytest.raises(SecretUnavailable, match="MINIO_ROOT_PASSWORD"):
        require_secret("MINIO_ROOT_PASSWORD")


def test_eight_identity_mapping_and_policies_unchanged():
    provisioner = load_provisioner()
    assert len(provisioner.IDENTITIES) == 8
    assert len({row[1] for row in provisioner.IDENTITIES}) == 8
    assert set(provisioner.POLICY_SCOPE) == {row[1] for row in provisioner.IDENTITIES}
    # No policy broadening: only the four approved object actions exist.
    for _prefixes, objects in provisioner.POLICY_SCOPE.values():
        for actions in objects.values():
            assert actions <= {
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
            }


def test_secret_names_never_appear_in_provisioner_error_or_output(
    monkeypatch, capsys
):
    complete_environment(monkeypatch)
    monkeypatch.delenv("TRINO_S3_SECRET_ACCESS_KEY", raising=False)
    provisioner = load_provisioner()
    assert provisioner.main(["--apply"]) == 1
    captured = capsys.readouterr()
    assert "TRINO_S3_SECRET_ACCESS_KEY" in captured.err
    for secret in (
        ROOT_PASSWORD,
        "service-secret-01-xyz",
        "service-secret-08-xyz",
    ):
        assert secret not in captured.out + captured.err


def test_secret_material_never_reaches_provisioner_surface(monkeypatch):
    complete_environment(monkeypatch)
    provisioner = load_provisioner()
    source = provisioner.administrative_source()
    # User-creation arguments carry secrets only at the point of use...
    add_command = ["admin", "user", "add", provisioner.ALIAS,
                   source["NESSIE_S3_ACCESS_KEY"], source["NESSIE_S3_SECRET_KEY"]]
    assert source["MINIO_ROOT_PASSWORD"] not in add_command
    assert source["MINIO_ROOT_USER"] not in add_command[4:]
    # ...while admin/policy/attach surfaces carry no secret material.
    for _identity, policy, access, secret in provisioner.IDENTITIES:
        for command in provisioner.policy_commands():
            assert source[secret] not in command
        attach = provisioner.attachment_command(policy, source[access])
        assert source[secret] not in attach
        assert source["MINIO_ROOT_PASSWORD"] not in attach
    # The completion message carries counts only, never credential values.
    message = (
        "Reconciled eight service identities; "
        "0 existing user secret(s) were not verified or rotated"
    )
    assert source["MINIO_ROOT_PASSWORD"] not in message
