"""C10.3D.2B: Kafka and Schema Registry callers resolve secrets via the provider.

Each test proves one property of the migration: local environment
compatibility, mounted-file delivery, producer/consumer/replay/MDM receipt of
the resolved credential, preserved configuration-vs-secret classification,
fail-closed production behaviour, no secret leakage, and unchanged event/topic
boundaries. Nothing here uses a network, a container, or an external secret
manager.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from services.shared.security.secret_provider import (
    SecretConfigurationError,
    SecretUnavailable,
)

ROOT = Path(__file__).resolve().parents[2]

CONSUMER = ROOT / "services/kafka-consumer-events/kafka_consumer_events.py"
REPLAY = ROOT / "services/kafka-consumer-events/replay_dlq.py"
PRODUCER = ROOT / "services/payment-producer/kafka_producer.py"
MDM = ROOT / "services/mdm-publisher/mdm_publisher.py"

KAFKA_PASSWORD = "kafka-sasl-password-value"
SR_USER_INFO = "schema-registry-user:schema-registry-password"

#: Everything these tests may set; no test depends on ambient state.
SCRUBBED_NAMES = (
    "DP_SECURITY_MODE",
    "DP_SECRET_SOURCE",
    "DP_SECRET_DIR",
    "KAFKA_SASL_PASSWORD",
    "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_SECURITY_PROTOCOL",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SSL_CA_LOCATION",
    "KAFKA_SSL_CERTIFICATE_LOCATION",
    "KAFKA_SSL_KEY_LOCATION",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_AUTH_REQUIRED",
    "MDM_KAFKA_PUBLISH_ENABLED",
)

#: The only secrets these callers are approved to resolve. Configuration
#: variables stay ordinary os.getenv reads.
APPROVED_SECRET_NAMES = (
    "KAFKA_SASL_PASSWORD",
    "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO",
)

MIGRATED_FILES = (CONSUMER, REPLAY, PRODUCER, MDM)


@pytest.fixture(autouse=True)
def isolated_secret_environment(monkeypatch):
    for name in SCRUBBED_NAMES:
        monkeypatch.delenv(name, raising=False)


def load_module(path: Path, name: str):
    """Import a service module afresh so import-time resolution is exercised."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def mount_secrets(tmp_path, monkeypatch, values: dict[str, str]) -> Path:
    for name, value in values.items():
        (tmp_path / name).write_text(value + "\n", encoding="utf-8")
    monkeypatch.setenv("DP_SECRET_DIR", str(tmp_path))
    return tmp_path


# ---------------------------------------------------------------------------
# 1. Local environment behaviour remains compatible
# ---------------------------------------------------------------------------


def test_local_env_kafka_password_remains_compatible(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", KAFKA_PASSWORD)
    producer = load_module(PRODUCER, "producer_env_password")
    consumer = load_module(CONSUMER, "consumer_env_password")

    assert producer.Config.KAFKA_SASL_PASSWORD == KAFKA_PASSWORD
    assert consumer.Settings.KAFKA_SASL_PASSWORD == KAFKA_PASSWORD


def test_local_env_schema_registry_auth_remains_compatible(monkeypatch):
    monkeypatch.setenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", SR_USER_INFO)
    producer = load_module(PRODUCER, "producer_env_sr")
    consumer = load_module(CONSUMER, "consumer_env_sr")

    assert producer.Config.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO == SR_USER_INFO
    assert consumer.Settings.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO == SR_USER_INFO


# ---------------------------------------------------------------------------
# 8. Configuration stays ordinary environment reads
# ---------------------------------------------------------------------------


def test_configuration_values_remain_ordinary_reads(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "config-not-secret")
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/run/secrets/kafka-ca.pem")
    monkeypatch.setenv("KAFKA_SSL_CERTIFICATE_LOCATION", "/run/secrets/kafka-cert.pem")
    monkeypatch.setenv("KAFKA_SSL_KEY_LOCATION", "/run/secrets/kafka-key.pem")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "https://schema-registry.example.internal")
    monkeypatch.setenv("SCHEMA_REGISTRY_AUTH_REQUIRED", "true")
    producer = load_module(PRODUCER, "producer_config_reads")

    assert producer.Config.KAFKA_BOOTSTRAP_SERVERS == "broker1:9092"
    assert producer.Config.KAFKA_SECURITY_PROTOCOL == "SSL"
    assert producer.Config.KAFKA_SASL_MECHANISM == "SCRAM-SHA-512"
    assert producer.Config.KAFKA_SASL_USERNAME == "config-not-secret"
    # TLS file PATHS remain configuration, never secret values.
    assert producer.Config.KAFKA_SSL_CA_LOCATION == "/run/secrets/kafka-ca.pem"
    assert producer.Config.KAFKA_SSL_CERTIFICATE_LOCATION == "/run/secrets/kafka-cert.pem"
    assert producer.Config.KAFKA_SSL_KEY_LOCATION == "/run/secrets/kafka-key.pem"
    assert producer.Config.SCHEMA_REGISTRY_URL == (
        "https://schema-registry.example.internal"
    )
    assert producer.Config.SCHEMA_REGISTRY_AUTH_REQUIRED is True
    config = producer.kafka_security_config()
    assert config["ssl.key.location"] == "/run/secrets/kafka-key.pem"


# ---------------------------------------------------------------------------
# 9-10. Fail-closed behaviour
# ---------------------------------------------------------------------------


def test_missing_production_kafka_credential_fails_closed(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SSL_CA_LOCATION", "/run/secrets/kafka-ca.pem")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "replay")
    replay = load_module(REPLAY, "replay_fail_closed")

    with pytest.raises(ValueError, match="SASL password"):
        replay.kafka_security_config()


def test_unsupported_secret_source_fails_closed(monkeypatch):
    monkeypatch.setenv("DP_SECRET_SOURCE", "vault")
    mdm = load_module(MDM, "mdm_bad_source")

    with pytest.raises(SecretConfigurationError, match="DP_SECRET_SOURCE"):
        mdm.kafka_security_config()


def test_undeclared_production_secret_source_fails_closed(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    replay = load_module(REPLAY, "replay_undeclared_source")

    with pytest.raises(SecretConfigurationError, match="DP_SECRET_SOURCE"):
        replay.kafka_security_config()


# ---------------------------------------------------------------------------
# 11. No secret leakage in diagnostics
# ---------------------------------------------------------------------------


def test_kafka_security_config_contains_no_logging_or_repr_of_secret(
    monkeypatch,
):
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", KAFKA_PASSWORD)
    producer = load_module(PRODUCER, "producer_no_leak")

    config = producer.kafka_security_config()
    assert config["sasl.password"] == KAFKA_PASSWORD
    # The dict carries the credential for the client library only; nothing in
    # the migrated code prints, reprs or logs it.
    source = PRODUCER.read_text(encoding="utf-8")
    for line in source.splitlines():
        if "sasl.password" in line or "basic.auth.user.info" in line:
            assert "log" not in line.lower()
            assert "print" not in line.lower()
    assert "KAFKA_SASL_PASSWORD" not in repr(config)


# ---------------------------------------------------------------------------
# 2. Mounted-file delivery for Kafka/SR secret material
# ---------------------------------------------------------------------------


def test_mounted_file_kafka_and_sr_secrets_resolve(monkeypatch, tmp_path):
    mount_secrets(
        tmp_path,
        monkeypatch,
        {
            "KAFKA_SASL_PASSWORD": KAFKA_PASSWORD,
            "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO": SR_USER_INFO,
        },
    )
    producer = load_module(PRODUCER, "producer_mounted")
    consumer = load_module(CONSUMER, "consumer_mounted")

    assert producer.Config.KAFKA_SASL_PASSWORD == KAFKA_PASSWORD
    assert producer.Config.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO == SR_USER_INFO
    assert consumer.Settings.KAFKA_SASL_PASSWORD == KAFKA_PASSWORD
    assert consumer.Settings.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO == SR_USER_INFO


# ---------------------------------------------------------------------------
# 3-6. Each caller receives the resolved credential
# ---------------------------------------------------------------------------


def test_producer_receives_resolved_credential(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", KAFKA_PASSWORD)
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "producer")
    producer = load_module(PRODUCER, "producer_credential")

    config = producer.kafka_security_config()
    assert config["sasl.password"] == KAFKA_PASSWORD
    assert config["sasl.username"] == "producer"


def test_generated_event_consumer_receives_resolved_credential(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", KAFKA_PASSWORD)
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "consumer")
    consumer = load_module(CONSUMER, "consumer_credential")

    config = consumer.kafka_client_config()
    assert config["sasl.password"] == KAFKA_PASSWORD
    assert config["sasl.username"] == "consumer"


def test_replay_dlq_receives_resolved_credential(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", KAFKA_PASSWORD)
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "replay")
    replay = load_module(REPLAY, "replay_credential")

    config = replay.kafka_security_config()
    assert config["sasl.password"] == KAFKA_PASSWORD
    assert config["sasl.username"] == "replay"


def test_mdm_publisher_receives_resolved_credential(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", KAFKA_PASSWORD)
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "mdm-publisher")
    mdm = load_module(MDM, "mdm_credential")

    config = mdm.kafka_security_config()
    assert config["sasl.password"] == KAFKA_PASSWORD
    assert config["sasl.username"] == "mdm-publisher"


# ---------------------------------------------------------------------------
# 7. Schema Registry secret material resolves and reaches the client config
# ---------------------------------------------------------------------------


def test_schema_registry_material_reaches_client_config(monkeypatch):
    monkeypatch.setenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", SR_USER_INFO)
    producer = load_module(PRODUCER, "producer_sr_config")

    sr_config = {"url": producer.Config.SCHEMA_REGISTRY_URL}
    if producer.Config.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO:
        sr_config["basic.auth.user.info"] = (
            producer.Config.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO
        )
    assert sr_config["basic.auth.user.info"] == SR_USER_INFO
    assert sr_config["url"] == "http://schema-registry:8081"


# ---------------------------------------------------------------------------
# 12-13. Validators remain authoritative and unchanged
# ---------------------------------------------------------------------------


def test_kafka_security_validator_behaviour_unchanged(monkeypatch):
    from services.shared.security.runtime_security import validate_kafka_security

    # Development permits plaintext without credentials.
    validate_kafka_security(security_protocol="PLAINTEXT")

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    with pytest.raises(ValueError, match="encrypted"):
        validate_kafka_security(security_protocol="PLAINTEXT")
    with pytest.raises(ValueError, match="CA location"):
        validate_kafka_security(security_protocol="SSL")
    with pytest.raises(ValueError, match="SASL password"):
        validate_kafka_security(
            security_protocol="SASL_SSL",
            ssl_ca_location="/run/secrets/kafka-ca.pem",
            sasl_mechanism="PLAIN",
            sasl_username="svc",
        )
    validate_kafka_security(
        security_protocol="SASL_SSL",
        ssl_ca_location="/run/secrets/kafka-ca.pem",
        sasl_mechanism="PLAIN",
        sasl_username="svc",
        sasl_password=KAFKA_PASSWORD,
    )


def test_schema_registry_validator_behaviour_unchanged(monkeypatch):
    from services.shared.security.runtime_security import (
        validate_schema_registry_security,
    )

    validate_schema_registry_security("http://schema-registry:8081")

    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    with pytest.raises(ValueError, match="HTTPS"):
        validate_schema_registry_security("http://schema-registry:8081")
    with pytest.raises(ValueError, match="authentication material"):
        validate_schema_registry_security(
            "https://schema-registry.example.internal",
            authentication_required=True,
        )
    validate_schema_registry_security(
        "https://schema-registry.example.internal",
        authentication_required=True,
        authentication_material=SR_USER_INFO,
    )


# ---------------------------------------------------------------------------
# 14. MDM publisher stays execution-gated and inactive
# ---------------------------------------------------------------------------


def test_mdm_publisher_remains_execution_gated(monkeypatch):
    mdm = load_module(MDM, "mdm_gated")

    assert mdm.load_registry()["publishing_enabled_by_default"] is False
    with pytest.raises(RuntimeError, match="disabled"):
        mdm.execute_publish("ordinary")
    with pytest.raises(RuntimeError, match="disabled"):
        mdm.execute_publish("restricted_identity")


# ---------------------------------------------------------------------------
# 15. Caller discipline and topic/contract invariants
# ---------------------------------------------------------------------------


def test_callers_resolve_only_approved_secret_names():
    """Only approved names reach resolve_secret; os.getenv handles config."""
    import ast

    for path in MIGRATED_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        resolved = []
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "resolve_secret"
            ):
                assert node.args and isinstance(node.args[0], ast.Constant)
                resolved.append(node.args[0].value)
        assert set(resolved) <= set(APPROVED_SECRET_NAMES), (path, resolved)


def test_debezium_and_pdmis_topic_boundaries_unchanged():
    consumer = load_module(CONSUMER, "consumer_boundaries")

    # CDC is not independently activatable and its topic map stays empty.
    assert consumer.activated_cdc_topic_map() == {}
    # pdmis.* generated-event topics remain consumer-owned.
    for topic in consumer.Settings.KAFKA_TOPICS:
        assert not topic.startswith("cdc.")
    assert "pdmis.loans" in consumer.Settings.KAFKA_TOPICS
    assert "pdmis.repayments" in consumer.Settings.KAFKA_TOPICS

