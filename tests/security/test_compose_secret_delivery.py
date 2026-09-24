"""C10.3D.3: Compose delivers the secret-provider seam and Kafka/SR client config.

Static Compose-contract tests: the repository Compose file must expose the
configuration seam required by the approved secret-provider contract and the
Kafka / Schema Registry client variables, without making production-only
material mandatory locally, without committing secret values, and without
broadening credentials into unrelated services. Runtime security behaviour
itself remains owned by runtime_security.py.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yaml"

COMPOSE_TEXT = COMPOSE.read_text(encoding="utf-8")

KAFKA_SECRET_CALLERS = ("payment-producer", "payment-consumer-events", "kafka-init")
SR_CLIENTS = ("payment-producer", "payment-consumer-events", "schema-registry-init")
DP_SEAM_SERVICES = (
    "payment-producer",
    "payment-consumer-events",
    "platform-job-runner",
    "transform-runtime",
    "transform-ledger-migrate",
    "agent-api",
    "minio-init-databases",
)
# One-shot MinIO IAM bootstrapper: resolves root/IAM secret material through
# resolve_secret() (provision_object_store_identities) with an explicitly
# pinned environment-backed seam and no host-supplied DP_SECRET_DIR.
DP_PINNED_SEAM_SERVICES = ("minio-init-iam",)

KAFKA_CLIENT_VARS = (
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SSL_CA_LOCATION",
    "KAFKA_SSL_CERTIFICATE_LOCATION",
    "KAFKA_SSL_KEY_LOCATION",
)

SR_CLIENT_VARS = (
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_AUTH_REQUIRED",
    "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO",
)


def services() -> dict:
    return yaml.safe_load(COMPOSE_TEXT)["services"]


def environment_of(name: str) -> dict:
    return services()[name].get("environment", {})


def local_render() -> dict:
    """Render Compose with an empty environment: local-only defaults."""
    def substitute(match: re.Match) -> str:
        variable, separator, default = (
            match.group(1),
            match.group(2),
            match.group(3),
        )
        value = os.environ.get(variable)
        if value is not None:
            return value
        if separator == ":-":
            return default or ""
        return ""
    return re.sub(
        r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:?-)?([^}]*)\}",
        substitute,
        COMPOSE_TEXT,
    )


# ---------------------------------------------------------------------------
# 1-3. Kafka client configuration delivery
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("service", KAFKA_SECRET_CALLERS)
def test_kafka_client_services_receive_security_protocol(service):
    env = environment_of(service)
    assert "KAFKA_SECURITY_PROTOCOL" in env
    assert env["KAFKA_SECURITY_PROTOCOL"].startswith("${KAFKA_SECURITY_PROTOCOL:-")
    assert env["KAFKA_SECURITY_PROTOCOL"].endswith("}")


@pytest.mark.parametrize("service", KAFKA_SECRET_CALLERS)
@pytest.mark.parametrize(
    "variable",
    ("KAFKA_SASL_MECHANISM", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD"),
)
def test_kafka_sasl_delivery_seams(service, variable):
    env = environment_of(service)
    assert env.get(variable) == f"${{{variable}:-}}"


@pytest.mark.parametrize("service", KAFKA_SECRET_CALLERS)
@pytest.mark.parametrize(
    "variable",
    (
        "KAFKA_SSL_CA_LOCATION",
        "KAFKA_SSL_CERTIFICATE_LOCATION",
        "KAFKA_SSL_KEY_LOCATION",
    ),
)
def test_kafka_tls_path_delivery_seams(service, variable):
    env = environment_of(service)
    assert env.get(variable) == f"${{{variable}:-}}"


def test_kafka_tls_paths_are_paths_not_secret_values():
    """Kafka TLS variables are FILE PATH configuration, not secret content."""
    for line in COMPOSE_TEXT.splitlines():
        if "KAFKA_SSL_CA_LOCATION" in line or "KAFKA_SSL_KEY_LOCATION" in line:
            # Delivery is an interpolation seam; no certificate/key CONTENT.
            assert "BEGIN CERTIFICATE" not in line
            assert "BEGIN PRIVATE KEY" not in line


# ---------------------------------------------------------------------------
# 4-5. Secret-provider seam delivery
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("service", DP_SEAM_SERVICES)
def test_secret_provider_callers_receive_dp_secret_source(service):
    env = environment_of(service)
    assert env.get("DP_SECRET_SOURCE") == "${DP_SECRET_SOURCE:-}"


@pytest.mark.parametrize("service", DP_SEAM_SERVICES)
def test_dp_secret_dir_supplyable_but_not_mandatory(service):
    env = environment_of(service)
    assert env.get("DP_SECRET_DIR") == "${DP_SECRET_DIR:-}"


def test_no_required_error_semantics_on_secret_delivery():
    """DP_/Kafka/SR delivery seams must never use ${VAR:?err} hard requirements."""
    seam_variables = (
        "DP_SECRET_SOURCE",
        "DP_SECRET_DIR",
        "KAFKA_SASL_PASSWORD",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_MECHANISM",
        "KAFKA_SSL_CA_LOCATION",
        "KAFKA_SSL_CERTIFICATE_LOCATION",
        "KAFKA_SSL_KEY_LOCATION",
        "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO",
    )
    for line in COMPOSE_TEXT.splitlines():
        stripped = line.strip()
        for name in seam_variables:
            if stripped.startswith(f"{name}:"):
                assert ":?" not in stripped, stripped


# ---------------------------------------------------------------------------
# 6-7. Schema Registry client delivery
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("service", SR_CLIENTS)
def test_schema_registry_clients_use_interpolated_url(service):
    env = environment_of(service)
    assert env.get("SCHEMA_REGISTRY_URL") == (
        "${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
    )


@pytest.mark.parametrize("service", SR_CLIENTS)
@pytest.mark.parametrize(
    "variable",
    ("SCHEMA_REGISTRY_AUTH_REQUIRED", "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"),
)
def test_schema_registry_auth_seams_delivered(service, variable):
    env = environment_of(service)
    assert variable in env


# ---------------------------------------------------------------------------
# 8-9. Local renderability without production-only material
# ---------------------------------------------------------------------------


def test_local_render_defaults_to_plaintext_and_http():
    render = yaml.safe_load(local_render())
    producer = render["services"]["payment-producer"]["environment"]
    consumer = render["services"]["payment-consumer-events"]["environment"]
    assert producer["KAFKA_SECURITY_PROTOCOL"] == "PLAINTEXT"
    assert consumer["KAFKA_SECURITY_PROTOCOL"] == "PLAINTEXT"
    assert producer["SCHEMA_REGISTRY_URL"] == "http://schema-registry:8081"
    assert consumer["SCHEMA_REGISTRY_URL"] == "http://schema-registry:8081"
    assert producer.get("KAFKA_SASL_PASSWORD") in (None, "")
    assert producer.get("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO") in (None, "")
    assert producer.get("DP_SECRET_SOURCE") in (None, "")
    assert producer.get("DP_SECRET_DIR") in (None, "")


def test_local_render_uses_default_bootstrap_and_internal_urls():
    render = yaml.safe_load(local_render())
    producer = render["services"]["payment-producer"]["environment"]
    consumer = render["services"]["payment-consumer-events"]["environment"]
    assert producer["KAFKA_BOOTSTRAP_SERVERS"] == "PLAINTEXT://kafka:9092"
    assert consumer["KAFKA_BOOTSTRAP_SERVERS"] == "PLAINTEXT://kafka:9092"


def test_local_render_never_requires_sasl_or_tls_material():
    """Rendering with no environment must not raise on missing TLS/SASL values."""
    # local_render() substitutes only :- defaults; any :? requirement without
    # a host value would raise when evaluated — prove the Kafka/SR seams
    # cannot trigger that.
    for line in local_render().splitlines():
        stripped = line.strip()
        for name in (
            "KAFKA_SASL_PASSWORD",
            "KAFKA_SSL_CA_LOCATION",
            "KAFKA_SSL_CERTIFICATE_LOCATION",
            "KAFKA_SSL_KEY_LOCATION",
            "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO",
            "DP_SECRET_SOURCE",
            "DP_SECRET_DIR",
        ):
            if stripped.startswith(f"{name}:"):
                assert ":?" not in stripped, stripped


# ---------------------------------------------------------------------------
# 10. Runtime validators still reject insecure production combinations
# ---------------------------------------------------------------------------


def test_runtime_production_validators_still_reject_insecure(monkeypatch):
    from services.shared.security.runtime_security import (
        validate_kafka_security,
        validate_schema_registry_security,
    )

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    with pytest.raises(ValueError, match="encrypted"):
        validate_kafka_security(security_protocol="PLAINTEXT")
    with pytest.raises(ValueError, match="HTTPS"):
        validate_schema_registry_security("http://schema-registry:8081")


# ---------------------------------------------------------------------------
# 11. No secret value is committed through the Compose contract
# ---------------------------------------------------------------------------


def test_no_secret_values_committed_in_compose():
    """Compose delivers seams only; no literal credential material."""
    assert "BEGIN CERTIFICATE" not in COMPOSE_TEXT
    assert "BEGIN PRIVATE KEY" not in COMPOSE_TEXT
    assert "BEGIN RSA PRIVATE KEY" not in COMPOSE_TEXT
    # No literal default password behind the Kafka SASL seam.
    assert not re.search(r"KAFKA_SASL_PASSWORD:\s*\$\{KAFKA_SASL_PASSWORD:-[^}]+\}", COMPOSE_TEXT)
    assert not re.search(
        r"SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO:\s*\$\{SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO:-[^}]+\}",
        COMPOSE_TEXT,
    )


# ---------------------------------------------------------------------------
# 12. Credentials are not broadened into unrelated services
# ---------------------------------------------------------------------------


def test_kafka_credentials_not_broadened_to_unrelated_services():
    for service, env in services().items():
        kafka_env = {k for k in env.get("environment", {}) if k.startswith("KAFKA_")}
        if service in KAFKA_SECRET_CALLERS or service == "kafka-ui":
            continue
        # Only topic topology is shared; no SASL/TLS credentials elsewhere.
        forbidden = {
            "KAFKA_SASL_PASSWORD",
            "KAFKA_SASL_USERNAME",
            "KAFKA_SASL_MECHANISM",
            "KAFKA_SSL_CA_LOCATION",
            "KAFKA_SSL_CERTIFICATE_LOCATION",
            "KAFKA_SSL_KEY_LOCATION",
        }
        assert not kafka_env & forbidden, (service, sorted(kafka_env & forbidden))


def test_schema_registry_credentials_only_on_sr_clients():
    for service, env in services().items():
        sr_env = {
            k
            for k in env.get("environment", {})
            if k in ("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "SCHEMA_REGISTRY_AUTH_REQUIRED")
        }
        if service in SR_CLIENTS:
            continue
        assert not sr_env, (service, sorted(sr_env))


def test_secret_provider_seam_limited_to_declared_services():
    declared_services = set(DP_SEAM_SERVICES) | set(
        DP_PINNED_SEAM_SERVICES
    )
    for service, env in services().items():
        dp_env = {k for k in env.get("environment", {}) if k.startswith("DP_SECRET")}
        if service in declared_services:
            continue
        assert not dp_env, (service, sorted(dp_env))
    # The pinned bootstrapper keeps its explicit environment-backed seam.
    assert environment_of("minio-init-iam").get("DP_SECRET_SOURCE") == "environment"


# ---------------------------------------------------------------------------
# 13-15. MDM publisher disabled, CDC inactive, execution gates false
# ---------------------------------------------------------------------------


def test_mdm_publisher_remains_absent_or_disabled_in_compose():
    # The transitional MDM publisher has no always-on Compose service.
    assert "mdm-publisher" not in services()


def test_cdc_connect_service_absent_or_gated():
    # Debezium/CDC has no active Compose service in this phase.
    cdc_services = [
        name
        for name in services()
        if "debezium" in name or name.startswith("cdc-")
    ]
    assert not cdc_services, cdc_services


def test_execution_gates_remain_false_by_default():
    for service in ("transform-runtime", "platform-job-runner"):
        env = environment_of(service)
        for gate in (
            "LAKEHOUSE_TRANSFORM_EXECUTION_ENABLED",
            "PLATFORM_RUNNER_EXECUTION_ENABLED",
            "PLATFORM_JOB_EXECUTION_ENABLED",
        ):
            if gate in env:
                assert env[gate].strip("'\"") == "false", (service, gate)


# ---------------------------------------------------------------------------
# 16. C8/C9 authority boundaries remain unchanged
# ---------------------------------------------------------------------------


def test_authority_boundary_variables_unchanged():
    # Transform authority separation: ordinary / restricted / ML credentials
    # remain distinct, per-authority variables on platform-job-runner.
    runner_env = environment_of("platform-job-runner")
    for authority in ("ORDINARY", "RESTRICTED", "ML"):
        assert f"{authority}_S3_ACCESS_KEY_ID" in runner_env
        assert f"{authority}_S3_SECRET_ACCESS_KEY" in runner_env
    # Nessie authority separation remains: transform vs publication tokens.
    assert "NESSIE_TRANSFORM_TOKEN" in runner_env
    runtime_env = environment_of("transform-runtime")
    assert "NESSIE_PUBLICATION_TOKEN" in runtime_env
    # No cross-pollination: publication token never on the runner, transform
    # token never on transform-runtime.
    assert "NESSIE_PUBLICATION_TOKEN" not in runner_env
    assert "NESSIE_TRANSFORM_TOKEN" not in runtime_env
