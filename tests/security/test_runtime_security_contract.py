import pytest

from services.shared.security.runtime_security import (
    is_production_security_mode,
    validate_kafka_security,
    validate_nessie_security,
    validate_schema_registry_security,
    validate_trino_security,
)


@pytest.fixture(autouse=True)
def clear_security_mode(monkeypatch):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)


def test_security_mode_defaults_to_non_production():
    assert is_production_security_mode() is False


@pytest.mark.parametrize(
    "value",
    ["production", "PRODUCTION", "Production", " production "],
)
def test_production_security_mode_is_case_insensitive(monkeypatch, value):
    monkeypatch.setenv("DP_SECURITY_MODE", value)
    assert is_production_security_mode() is True


def test_development_allows_nessie_http():
    validate_nessie_security(
        "http://nessie:19120",
        auth_mode=None,
        token=None,
    )


def test_development_allows_trino_http():
    validate_trino_security(scheme="http")


def test_development_allows_kafka_plaintext():
    validate_kafka_security(security_protocol="PLAINTEXT")


def test_development_allows_schema_registry_http():
    validate_schema_registry_security(
        "http://schema-registry:8081"
    )


def test_production_allows_secure_nessie(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    validate_nessie_security(
        "https://nessie.example.internal",
        auth_mode="bearer",
        token="test-token",
    )


def test_production_rejects_nessie_http(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(ValueError, match="HTTPS"):
        validate_nessie_security(
            "http://nessie:19120",
            auth_mode="bearer",
            token="test-token",
        )


def test_production_rejects_nessie_missing_bearer(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(ValueError, match="bearer"):
        validate_nessie_security(
            "https://nessie.example.internal",
            auth_mode="none",
            token="test-token",
        )


def test_production_rejects_nessie_missing_token(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(ValueError, match="token"):
        validate_nessie_security(
            "https://nessie.example.internal",
            auth_mode="bearer",
            token="",
        )


def test_production_allows_trino_https(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    validate_trino_security(scheme="https")


def test_production_rejects_trino_http(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(ValueError, match="HTTPS"):
        validate_trino_security(scheme="http")


def test_production_allows_kafka_ssl(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    validate_kafka_security(
        security_protocol="SSL",
        ssl_ca_location="/run/secrets/kafka-ca.pem",
    )


def test_production_allows_kafka_sasl_ssl(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    validate_kafka_security(
        security_protocol="SASL_SSL",
        ssl_ca_location="/run/secrets/kafka-ca.pem",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="service-user",
        sasl_password="test-password",
    )


@pytest.mark.parametrize(
    "protocol",
    ["PLAINTEXT", "SASL_PLAINTEXT"],
)
def test_production_rejects_unencrypted_kafka(monkeypatch, protocol):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(ValueError, match="encrypted"):
        validate_kafka_security(security_protocol=protocol)


def test_production_rejects_kafka_ssl_without_ca(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(ValueError, match="CA location"):
        validate_kafka_security(security_protocol="SSL")


@pytest.mark.parametrize(
    "kwargs",
    [
        {
            "sasl_mechanism": None,
            "sasl_username": "service-user",
            "sasl_password": "test-password",
        },
        {
            "sasl_mechanism": "SCRAM-SHA-512",
            "sasl_username": None,
            "sasl_password": "test-password",
        },
        {
            "sasl_mechanism": "SCRAM-SHA-512",
            "sasl_username": "service-user",
            "sasl_password": None,
        },
    ],
)
def test_production_rejects_incomplete_kafka_sasl_ssl(
    monkeypatch,
    kwargs,
):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(ValueError, match="requires"):
        validate_kafka_security(
            security_protocol="SASL_SSL",
            ssl_ca_location="/run/secrets/kafka-ca.pem",
            **kwargs,
        )


def test_production_allows_schema_registry_https(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    validate_schema_registry_security(
        "https://schema-registry.example.internal"
    )


def test_production_rejects_schema_registry_http(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(ValueError, match="HTTPS"):
        validate_schema_registry_security(
            "http://schema-registry:8081"
        )


def test_production_requires_schema_registry_auth_when_requested(
    monkeypatch,
):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    with pytest.raises(ValueError, match="authentication material"):
        validate_schema_registry_security(
            "https://schema-registry.example.internal",
            authentication_required=True,
            authentication_material=None,
        )


def test_production_accepts_schema_registry_auth_when_supplied(
    monkeypatch,
):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")

    validate_schema_registry_security(
        "https://schema-registry.example.internal",
        authentication_required=True,
        authentication_material="test-user:test-password",
    )
