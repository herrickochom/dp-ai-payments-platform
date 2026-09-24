"""Nessie caller and deployment authentication boundaries."""

from pathlib import Path

import pytest
import yaml

from orchestration.transform_runtime.nessie_publication import NessiePublicationError, NessiePublisher


ROOT = Path(__file__).resolve().parents[2]


def test_publication_requires_its_own_token(monkeypatch):
    monkeypatch.delenv("NESSIE_PUBLICATION_TOKEN", raising=False)
    monkeypatch.setenv("NESSIE_TRANSFORM_TOKEN", "worker-only-token")
    monkeypatch.delenv("NESSIE_AUTH_MODE", raising=False)
    with pytest.raises(NessiePublicationError, match="publication token unavailable"):
        NessiePublisher("http://nessie:19120")
    monkeypatch.setenv("NESSIE_PUBLICATION_TOKEN", "publisher-token")
    publisher = NessiePublisher("http://nessie:19120")
    assert publisher.token == "publisher-token"


def test_production_rejects_insecure_nessie_modes(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("NESSIE_AUTH_MODE", "development-insecure")
    with pytest.raises(
        NessiePublicationError,
        match="Nessie must use bearer authentication",
    ):
        NessiePublisher("https://nessie.example", "token")
    monkeypatch.setenv("NESSIE_AUTH_MODE", "bearer")
    with pytest.raises(
        NessiePublicationError,
        match="Nessie must use HTTPS",
    ):
        NessiePublisher("http://nessie:19120", "token")
    assert NessiePublisher("https://nessie.example", "token").token == "token"


def test_server_trino_and_worker_use_separate_contracts():
    compose = (ROOT / "docker-compose.yaml").read_text()
    nessie = compose.split("  nessie:\n", 1)[1].split("\n  nessie-rest-proxy:", 1)[0]
    runtime = compose.split("  transform-runtime:\n", 1)[1].split("\n  platform-job-runner:", 1)[0]
    runner = compose.split("  platform-job-runner:\n", 1)[1].split("\n  airflow-init:", 1)[0]
    trino = compose.split("  trino:\n", 1)[1].split("\n  pdm-ml-features:", 1)[0]
    assert "NESSIE_SERVER_AUTHENTICATION_ENABLED" in nessie
    assert "QUARKUS_OIDC_AUTH_SERVER_URL" in nessie
    assert "QUARKUS_OIDC_CLIENT_ID" in nessie
    assert "NESSIE_PUBLICATION_TOKEN" in runtime and "NESSIE_TRANSFORM_TOKEN" not in runtime
    assert "NESSIE_TRANSFORM_TOKEN" in runner and "NESSIE_PUBLICATION_TOKEN" not in runner
    assert "NESSIE_TRINO_READ_TOKEN" in trino
    catalog = (ROOT / "platform/trino/catalog/iceberg.properties").read_text()
    assert "iceberg.rest-catalog.security=OAUTH2" in catalog
    assert "${env:NESSIE_TRINO_READ_TOKEN}" in catalog
    assert "/q/health/ready" in nessie


def test_transform_and_trino_endpoints_are_deployment_configurable(monkeypatch):
    from orchestration.job_runner.transform_execution import (
        build_transform_command,
    )

    compose = yaml.safe_load(
        (ROOT / "docker-compose.yaml").read_text(encoding="utf-8")
    )
    profile = (ROOT / "transform/dbt/profiles.yml").read_text(encoding="utf-8")
    catalog = (
        ROOT / "platform/trino/catalog/iceberg.properties"
    ).read_text(encoding="utf-8")
    plugin = (
        ROOT / "orchestration/job_runner/nessie_iceberg_plugin.py"
    ).read_text(encoding="utf-8")

    # Worker/runtime receives the Nessie endpoint through deployment
    # configuration. Parsed via YAML: textual quoting of the interpolation
    # is semantically irrelevant once compose parses the document.
    runner_env = compose["services"]["platform-job-runner"]["environment"]
    assert runner_env["NESSIE_ENDPOINT"] == "${NESSIE_ENDPOINT}"

    # Governed child maps/preserves NESSIE_ENDPOINT into its runtime env.
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)
    monkeypatch.setenv("NESSIE_AUTH_MODE", "bearer")
    command = build_transform_command(
        "C4_ML_01",
        "be_" + "4" * 32,
        _transform_credentials("https://nessie.example"),
    )
    assert command.environment["NESSIE_ENDPOINT"] == "https://nessie.example"

    # Plugin consumes NESSIE_ENDPOINT and constructs the execution-scoped
    # REST catalog endpoint for the governed branch. Parameter-binding
    # security remains covered by test_nessie_plugin_contract_*.
    assert 'os.environ.get("NESSIE_ENDPOINT"' in plugin
    assert "/iceberg/{branch}" in plugin

    # profiles.yml loads the plugin through dbt-duckdb's supported credential;
    # the transform token and endpoint stay outside the profile (they are
    # runtime-scoped, read by the plugin from the governed child env).
    assert "module_paths:" in profile
    assert "- module: nessie_iceberg_plugin" in profile
    assert "NESSIE_TRANSFORM_TOKEN" not in profile
    assert "NESSIE_ENDPOINT" not in profile

    # Trino keeps its own deployment-configurable read endpoint.
    trino_env = compose["services"]["trino"]["environment"]
    assert "NESSIE_TRINO_ENDPOINT" in trino_env
    assert (
        "iceberg.rest-catalog.uri="
        "${env:NESSIE_TRINO_ENDPOINT}/iceberg"
    ) in catalog


def test_nessie_authorities_are_not_cross_wired():
    compose = (ROOT / "docker-compose.yaml").read_text()

    runtime = compose.split(
        "  transform-runtime:\n", 1
    )[1].split(
        "\n  platform-job-runner:", 1
    )[0]

    runner = compose.split(
        "  platform-job-runner:\n", 1
    )[1].split(
        "\n  airflow-init:", 1
    )[0]

    trino = compose.split(
        "  trino:\n", 1
    )[1].split(
        "\n  pdm-ml-features:", 1
    )[0]

    assert "NESSIE_PUBLICATION_TOKEN" in runtime
    assert "NESSIE_TRANSFORM_TOKEN" not in runtime
    assert "NESSIE_TRINO_READ_TOKEN" not in runtime

    assert "NESSIE_TRANSFORM_TOKEN" in runner
    assert "NESSIE_PUBLICATION_TOKEN" not in runner
    assert "NESSIE_TRINO_READ_TOKEN" not in runner

    assert "NESSIE_TRINO_READ_TOKEN" in trino
    assert "NESSIE_TRANSFORM_TOKEN" not in trino
    assert "NESSIE_PUBLICATION_TOKEN" not in trino


def _transform_credentials(endpoint: str) -> dict[str, str]:
    return {
        "ML_S3_ACCESS_KEY_ID": "ml-access",
        "ML_S3_SECRET_ACCESS_KEY": "ml-secret",
        "NESSIE_TRANSFORM_TOKEN": "transform-token",
        "NESSIE_ENDPOINT": endpoint,
        "S3_ENDPOINT": "https://object-store.example.test",
        "S3_USE_SSL": "true",
        "OBJECT_STORE_REGION": "eu-central-2",
        "OBJECT_STORE_BUCKET": "dp-ai-payment",
        "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2",
        "WAREHOUSE_PREFIX": "warehouse",
        "WAREHOUSE_URI": "s3://dp-ai-payment/warehouse",
        "DBT_S3_URL_STYLE": "path",
        "DBT_DATABASE": "lakehouse",
    }


def test_production_transform_rejects_http_nessie_before_execution(
    monkeypatch,
):
    from orchestration.job_runner.transform_execution import (
        build_transform_command,
    )

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("NESSIE_AUTH_MODE", "bearer")

    with pytest.raises(ValueError, match="Nessie must use HTTPS"):
        build_transform_command(
            "C4_ML_01",
            "be_" + "1" * 32,
            _transform_credentials("http://nessie:19120"),
        )


def test_production_transform_accepts_https_nessie_contract(
    monkeypatch,
):
    from orchestration.job_runner.transform_execution import (
        build_transform_command,
    )

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("NESSIE_AUTH_MODE", "bearer")

    command = build_transform_command(
        "C4_ML_01",
        "be_" + "2" * 32,
        _transform_credentials("https://nessie.example"),
    )

    assert command.environment["NESSIE_ENDPOINT"] == (
        "https://nessie.example"
    )
    assert command.environment["NESSIE_TRANSFORM_TOKEN"] == (
        "transform-token"
    )


def test_development_transform_allows_http_nessie(monkeypatch):
    from orchestration.job_runner.transform_execution import (
        build_transform_command,
    )

    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)
    monkeypatch.setenv("NESSIE_AUTH_MODE", "bearer")

    command = build_transform_command(
        "C4_ML_01",
        "be_" + "3" * 32,
        _transform_credentials("http://nessie:19120"),
    )

    assert command.environment["NESSIE_ENDPOINT"] == (
        "http://nessie:19120"
    )
