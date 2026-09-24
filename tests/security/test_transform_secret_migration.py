"""C10.3D.2A: transform and control-plane callers resolve secrets via the provider.

Each test proves one property of the migration: environment compatibility,
mounted-file support per authority class, preserved fail-closed behaviour,
authority separation, child-environment containment, and that configuration
variables are deliberately not routed through the secret provider. Nothing here
uses a network, a container, or an external secret manager.
"""

from __future__ import annotations

import ast
import sys
import types
from pathlib import Path

import pytest
from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "services" / "shared"))
JOB_RUNNER = str(ROOT / "orchestration" / "job_runner")
if JOB_RUNNER not in sys.path:
    sys.path.insert(0, JOB_RUNNER)

import data_protection as dp  # noqa: E402  (path-based import, as the tokeniser runtime uses)
from orchestration.job_runner import transform_jobs  # noqa: E402
from orchestration.job_runner import api as job_api  # noqa: E402
from orchestration.job_runner.transform_execution import (  # noqa: E402
    AUTHORITY_ENV,
    AUTHORITY_SECRET_NAMES,
    FORBIDDEN_ENV,
    build_transform_command,
)
from orchestration.transform_runtime import ledger, migrate  # noqa: E402
from orchestration.transform_runtime.auth import (  # noqa: E402
    require_airflow_identity,
    runner_headers,
)
from orchestration.transform_runtime.durable_queue import (  # noqa: E402
    DurableQueueError,
    PostgresDurableQueue,
    application_database_url as queue_database_url,
)
from orchestration.transform_runtime.execution_plan import (  # noqa: E402
    EXECUTION_BATCHES,
    PROTECTED_EXTERNAL_PREREQUISITES,
)
from orchestration.transform_runtime.nessie_publication import (  # noqa: E402
    NessiePublicationError,
    NessiePublisher,
    publication_token,
)
from orchestration.transform_runtime.postgres_connection import (  # noqa: E402
    validate_database_url,
)
from services.shared.security.secret_provider import (  # noqa: E402
    SecretConfigurationError,
    SecretUnavailable,
    require_secret,
)


TRANSFORM_TOKEN = "transform-authority-token"
PUBLICATION_TOKEN = "publication-authority-token"
AIRFLOW_TOKEN = "control-plane-airflow-token"
RUNNER_TOKEN = "control-plane-runner-token"
APP_URL = "postgresql://transform_app:app-password@ledger.example:5432/ledger"
MIGRATION_URL = "postgresql://transform_migration:migration-password@ledger.example:5432/ledger"
TOKEN_KEY = "gate2-mounted-file-key"

EXECUTION_ID = "be_" + ("a" * 32)

# Non-secret configuration the child command requires. These stay ordinary
# environment reads and must never be resolved through the secret provider.
OBJECT_STORE_CONFIG = {
    "S3_ENDPOINT": "http://minio:9000",
    "S3_USE_SSL": "false",
    "OBJECT_STORE_REGION": "us-east-1",
    "OBJECT_STORE_BUCKET": "dp-ai-payment",
    "RAW_ROOT": "raw",
    "RAW_VERSION": "v2",
    "RAW_PREFIX": "raw/v2",
    "WAREHOUSE_PREFIX": "warehouse",
    "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse",
    "DBT_S3_URL_STYLE": "path",
    "NESSIE_ENDPOINT": "http://nessie:19120",
}

SECRET_SOURCE_NAMES = (
    "DP_SECURITY_MODE",
    "DP_SECRET_SOURCE",
    "DP_SECRET_DIR",
    "NESSIE_PUBLICATION_TOKEN",
    "NESSIE_TRANSFORM_TOKEN",
    "TRANSFORM_RUNTIME_AIRFLOW_TOKEN",
    "TRANSFORM_RUNTIME_RUNNER_TOKEN",
    "TRANSFORM_LEDGER_APP_DATABASE_URL",
    "TRANSFORM_LEDGER_MIGRATION_DATABASE_URL",
    "DP_TOKEN_KEY",
    "DP_TOKEN_KEY_VERSION",
    "DP_NO_SUCH_SECRET",
    "MINIO_ROOT_USER",
    "MINIO_ROOT_PASSWORD",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    *OBJECT_STORE_CONFIG,
    *sorted(AUTHORITY_SECRET_NAMES),
)


@pytest.fixture(autouse=True)
def isolated_secret_environment(monkeypatch):
    """No test depends on ambient secret or configuration state."""
    for name in SECRET_SOURCE_NAMES:
        monkeypatch.delenv(name, raising=False)


def mount_secrets(tmp_path, monkeypatch, values: dict[str, str]) -> Path:
    """Mount one file per secret name and point DP_SECRET_DIR at them."""
    for name, value in values.items():
        (tmp_path / name).write_text(value + "\n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))
    return tmp_path


def set_object_store_config(monkeypatch) -> None:
    for name, value in OBJECT_STORE_CONFIG.items():
        monkeypatch.setenv(name, value)


def authority_credential_names(authority: str) -> tuple[str, str]:
    names = AUTHORITY_ENV[authority]
    access = next(name for name in sorted(names) if name.endswith("_ACCESS_KEY_ID"))
    secret = next(name for name in sorted(names) if name.endswith("_SECRET_ACCESS_KEY"))
    return access, secret


def authority_batch(authority: str):
    return next(
        batch
        for batch in EXECUTION_BATCHES.values()
        if batch.authority == authority
        and not (batch.model_allowlist & PROTECTED_EXTERNAL_PREREQUISITES)
    )


def test_local_environment_reads_are_unchanged(monkeypatch):
    monkeypatch.setenv("NESSIE_PUBLICATION_TOKEN", PUBLICATION_TOKEN)
    monkeypatch.setenv("TRANSFORM_RUNTIME_AIRFLOW_TOKEN", AIRFLOW_TOKEN)
    monkeypatch.setenv("TRANSFORM_RUNTIME_RUNNER_TOKEN", RUNNER_TOKEN)
    monkeypatch.setenv("TRANSFORM_LEDGER_APP_DATABASE_URL", APP_URL)

    assert publication_token() == PUBLICATION_TOKEN
    assert NessiePublisher("http://nessie:19120").token == PUBLICATION_TOKEN
    assert require_airflow_identity(f"Bearer {AIRFLOW_TOKEN}") == "airflow"
    assert runner_headers() == {"Authorization": f"Bearer {RUNNER_TOKEN}"}
    assert job_api.require_runtime(f"Bearer {RUNNER_TOKEN}") is None
    assert ledger.application_database_url() == APP_URL
    assert queue_database_url() == APP_URL


def test_control_plane_tokens_resolve_from_mounted_files(monkeypatch, tmp_path):
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            "TRANSFORM_RUNTIME_AIRFLOW_TOKEN": AIRFLOW_TOKEN,
            "TRANSFORM_RUNTIME_RUNNER_TOKEN": RUNNER_TOKEN,
        },
    )

    assert require_airflow_identity(f"Bearer {AIRFLOW_TOKEN}") == "airflow"
    assert runner_headers() == {"Authorization": f"Bearer {RUNNER_TOKEN}"}
    assert job_api.require_runtime(f"Bearer {RUNNER_TOKEN}") is None

    sent = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return b"{}"

    def fake_urlopen(request, timeout=None):
        sent["authorization"] = request.headers.get("Authorization")
        return Response()

    monkeypatch.setattr(transform_jobs.urllib.request, "urlopen", fake_urlopen)
    transform_jobs._request("GET", "/v1/contract")

    assert sent["authorization"] == f"Bearer {AIRFLOW_TOKEN}"


@pytest.mark.parametrize("authority", sorted(AUTHORITY_ENV))
def test_mounted_file_supplies_each_authority_class(monkeypatch, tmp_path, authority):
    set_object_store_config(monkeypatch)
    access, secret = authority_credential_names(authority)
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            access: f"{authority}-access-from-file",
            secret: f"{authority}-secret-from-file",
            "NESSIE_TRANSFORM_TOKEN": TRANSFORM_TOKEN,
        },
    )

    batch = authority_batch(authority)
    command = build_transform_command(batch.batch_id, EXECUTION_ID)

    assert command.authority == authority
    assert command.environment["TRANSFORM_S3_ACCESS_KEY_ID"] == f"{authority}-access-from-file"
    assert command.environment["TRANSFORM_S3_SECRET_ACCESS_KEY"] == f"{authority}-secret-from-file"
    assert command.environment["NESSIE_TRANSFORM_TOKEN"] == TRANSFORM_TOKEN


def test_child_environment_carries_only_authorised_resolved_credentials(monkeypatch, tmp_path):
    set_object_store_config(monkeypatch)
    monkeypatch.setenv("MINIO_ROOT_USER", "root-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "root-password")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "global-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "global-secret")
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            "ORDINARY_S3_ACCESS_KEY_ID": "ordinary-access",
            "ORDINARY_S3_SECRET_ACCESS_KEY": "ordinary-secret",
            "NESSIE_TRANSFORM_TOKEN": TRANSFORM_TOKEN,
            "RESTRICTED_S3_ACCESS_KEY_ID": "restricted-access",
            "RESTRICTED_S3_SECRET_ACCESS_KEY": "restricted-secret",
            "ML_S3_ACCESS_KEY_ID": "ml-access",
            "ML_S3_SECRET_ACCESS_KEY": "ml-secret",
            "NESSIE_PUBLICATION_TOKEN": PUBLICATION_TOKEN,
        },
    )

    command = build_transform_command(
        authority_batch("ordinary_transform").batch_id, EXECUTION_ID
    )
    environment = command.environment

    assert environment["TRANSFORM_S3_ACCESS_KEY_ID"] == "ordinary-access"
    assert environment["TRANSFORM_S3_SECRET_ACCESS_KEY"] == "ordinary-secret"

    for absent in (
        "RESTRICTED_S3_ACCESS_KEY_ID",
        "RESTRICTED_S3_SECRET_ACCESS_KEY",
        "ML_S3_ACCESS_KEY_ID",
        "ML_S3_SECRET_ACCESS_KEY",
        "NESSIE_PUBLICATION_TOKEN",
        "DP_SECRET_DIR",
        "DP_SECRET_SOURCE",
        "TRANSFORM_RUNTIME_AIRFLOW_TOKEN",
    ):
        assert absent not in environment, absent

    assert not FORBIDDEN_ENV.intersection(environment)
    for foreign_value in (
        "restricted-access",
        "restricted-secret",
        "ml-access",
        "ml-secret",
        PUBLICATION_TOKEN,
        "root-user",
        "root-password",
        "global-access",
        "global-secret",
    ):
        assert foreign_value not in environment.values()


def test_required_authority_absence_still_fails_closed(monkeypatch):
    set_object_store_config(monkeypatch)

    with pytest.raises(ValueError, match="required transform authority credentials are unavailable"):
        build_transform_command(authority_batch("ordinary_transform").batch_id, EXECUTION_ID)


def test_control_plane_absence_still_fails_closed():
    with pytest.raises(RuntimeError, match="runner service credential is unavailable"):
        runner_headers()

    with pytest.raises(HTTPException) as unauthorised:
        require_airflow_identity(f"Bearer {AIRFLOW_TOKEN}")
    assert unauthorised.value.status_code == 401

    with pytest.raises(HTTPException) as runner_unauthorised:
        job_api.require_runtime(f"Bearer {RUNNER_TOKEN}")
    assert runner_unauthorised.value.status_code == 401


def test_ledger_absence_keeps_the_existing_required_credential_error():
    assert ledger.application_database_url() == ""
    assert queue_database_url() == ""

    with pytest.raises(SecretUnavailable):
        require_secret("TRANSFORM_LEDGER_APP_DATABASE_URL")

    with pytest.raises(DurableQueueError, match="TRANSFORM_LEDGER_APP_DATABASE_URL is required"):
        PostgresDurableQueue()


def test_publication_authority_stays_separate_from_transform_authority(monkeypatch, tmp_path):
    set_object_store_config(monkeypatch)
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            "NESSIE_TRANSFORM_TOKEN": TRANSFORM_TOKEN,
            "NESSIE_PUBLICATION_TOKEN": PUBLICATION_TOKEN,
            "ORDINARY_S3_ACCESS_KEY_ID": "ordinary-access",
            "ORDINARY_S3_SECRET_ACCESS_KEY": "ordinary-secret",
        },
    )

    assert NessiePublisher("http://nessie:19120").token == PUBLICATION_TOKEN

    command = build_transform_command(
        authority_batch("ordinary_transform").batch_id, EXECUTION_ID
    )
    assert command.environment["NESSIE_TRANSFORM_TOKEN"] == TRANSFORM_TOKEN
    assert PUBLICATION_TOKEN not in command.environment.values()


def test_publication_token_is_never_derived_from_the_transform_token(monkeypatch, tmp_path):
    mount_secrets(tmp_path, monkeypatch, {"NESSIE_TRANSFORM_TOKEN": TRANSFORM_TOKEN})

    assert publication_token() == ""
    with pytest.raises(NessiePublicationError, match="publication token unavailable"):
        NessiePublisher("http://nessie:19120")


def test_ledger_identities_resolve_separately_from_mounted_files(monkeypatch, tmp_path):
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            "TRANSFORM_LEDGER_APP_DATABASE_URL": APP_URL,
            "TRANSFORM_LEDGER_MIGRATION_DATABASE_URL": MIGRATION_URL,
        },
    )

    assert ledger.application_database_url() == APP_URL
    assert queue_database_url() == APP_URL

    statements = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def transaction(self):
            return self

        def execute(self, statement):
            statements.append(statement)

    def connect(url):
        statements.append(url)
        return Connection()

    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=connect))

    migrate.main()

    assert statements[0] == MIGRATION_URL
    assert APP_URL not in statements
    assert APP_URL != MIGRATION_URL


def test_tokenisation_key_resolves_from_a_mounted_file(monkeypatch, tmp_path):
    monkeypatch.setenv("DP_TOKEN_KEY_VERSION", "vTest1")
    monkeypatch.setenv("DP_TOKEN_KEY", TOKEN_KEY)
    from_environment = dp.keyed_token("BENEFICIARY-000001")

    monkeypatch.delenv("DP_TOKEN_KEY", raising=False)
    mount_secrets(tmp_path, monkeypatch, {"DP_TOKEN_KEY": TOKEN_KEY})

    assert dp.keyed_token("BENEFICIARY-000001") == from_environment
    assert from_environment.startswith("vTest1:")


def test_tokenisation_key_absence_still_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setenv("DP_TOKEN_KEY_VERSION", "vTest1")

    with pytest.raises(dp.TokenisationKeyMissing, match="tokenisation is mandatory"):
        dp.keyed_token("BENEFICIARY-000001")

    (tmp_path / "DP_TOKEN_KEY").write_text("   \n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))

    with pytest.raises(dp.TokenisationKeyMissing, match="tokenisation is mandatory"):
        dp.keyed_token("BENEFICIARY-000001")


def test_secret_values_never_reach_errors_or_output(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "mounted-files")
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            "NESSIE_PUBLICATION_TOKEN": PUBLICATION_TOKEN,
            "TRANSFORM_LEDGER_APP_DATABASE_URL": APP_URL,
        },
    )

    with pytest.raises(NessiePublicationError) as publication:
        NessiePublisher("http://nessie:19120")

    with pytest.raises(ValueError) as rejected:
        validate_database_url(ledger.application_database_url())

    captured = capsys.readouterr()
    for rendered in (
        str(publication.value),
        repr(publication.value),
        str(rejected.value),
        repr(rejected.value),
        captured.out,
        captured.err,
    ):
        assert PUBLICATION_TOKEN not in rendered
        assert "app-password" not in rendered


def test_undeclared_secret_source_fails_closed_for_a_migrated_caller(monkeypatch, tmp_path):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    mount_secrets(tmp_path, monkeypatch, {"NESSIE_PUBLICATION_TOKEN": PUBLICATION_TOKEN})

    with pytest.raises(SecretConfigurationError, match="DP_SECRET_SOURCE"):
        publication_token()


def test_configuration_variables_are_not_read_through_the_provider(monkeypatch, tmp_path):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "mounted-files")
    monkeypatch.setenv("NESSIE_AUTH_MODE", "bearer")
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            "NESSIE_PUBLICATION_TOKEN": PUBLICATION_TOKEN,
            # A decoy file for a configuration name: if NESSIE_AUTH_MODE were
            # routed through the provider this value would win and production
            # validation would reject the bearer contract.
            "NESSIE_AUTH_MODE": "development-insecure",
        },
    )

    publisher = NessiePublisher("https://nessie.example")

    assert publisher.endpoint == "https://nessie.example"
    assert publisher.token == PUBLICATION_TOKEN


MIGRATED_MODULES = (
    "orchestration/job_runner/api.py",
    "orchestration/job_runner/transform_execution.py",
    "orchestration/job_runner/transform_jobs.py",
    "orchestration/transform_runtime/auth.py",
    "orchestration/transform_runtime/durable_queue.py",
    "orchestration/transform_runtime/ledger.py",
    "orchestration/transform_runtime/migrate.py",
    "orchestration/transform_runtime/nessie_publication.py",
    "services/shared/data_protection.py",
)

PROVIDER_SECRET_NAMES = frozenset(
    {
        *AUTHORITY_SECRET_NAMES,
        "NESSIE_PUBLICATION_TOKEN",
        "TRANSFORM_RUNTIME_AIRFLOW_TOKEN",
        "TRANSFORM_RUNTIME_RUNNER_TOKEN",
        "TRANSFORM_LEDGER_APP_DATABASE_URL",
        "TRANSFORM_LEDGER_MIGRATION_DATABASE_URL",
        "DP_TOKEN_KEY",
    }
)

EXPECTED_AUTHORITY_NAMES = frozenset(
    {
        "ORDINARY_S3_ACCESS_KEY_ID",
        "ORDINARY_S3_SECRET_ACCESS_KEY",
        "RESTRICTED_S3_ACCESS_KEY_ID",
        "RESTRICTED_S3_SECRET_ACCESS_KEY",
        "ML_S3_ACCESS_KEY_ID",
        "ML_S3_SECRET_ACCESS_KEY",
        "NESSIE_TRANSFORM_TOKEN",
    }
)


def test_provider_is_only_ever_asked_for_secret_material():
    literal_names = set()
    dynamic_sites = []

    for relative in MIGRATED_MODULES:
        tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in {"resolve_secret", "require_secret"}
            ):
                if node.args and isinstance(node.args[0], ast.Constant):
                    literal_names.add(node.args[0].value)
                else:
                    dynamic_sites.append(relative)

    assert literal_names
    assert literal_names <= PROVIDER_SECRET_NAMES

    # The only dynamic call site is the authority loop, bounded by AUTHORITY_ENV.
    assert set(dynamic_sites) == {"orchestration/job_runner/transform_execution.py"}
    assert AUTHORITY_SECRET_NAMES == EXPECTED_AUTHORITY_NAMES

    # Every non-authority name is requested literally, so nothing is missed.
    assert EXPECTED_AUTHORITY_NAMES >= (PROVIDER_SECRET_NAMES - literal_names)
