import importlib.util
from pathlib import Path

import pytest


import json

PATH = Path(
    "services/mdm-publisher/mdm_publisher.py"
)

SPEC = importlib.util.spec_from_file_location(
    "mdm_publisher",
    PATH,
)

publisher = importlib.util.module_from_spec(
    SPEC
)

assert SPEC.loader is not None
SPEC.loader.exec_module(publisher)


@pytest.fixture(autouse=True)
def isolated_mdm_publisher_data(tmp_path, monkeypatch):
    """Provide deterministic repository-independent MDM test inputs."""
    registry = publisher.load_registry()

    synthetic_root = tmp_path / "repository"

    expected_counts = {
        "mdm.beneficiary.golden.restricted": 244,
        "mdm.beneficiary.identity-alert.restricted": 6,
        "mdm.crosswalk.restricted": 364,
        "mdm.sacco.golden": 38,
        "mdm.agent.golden": 38,
        "mdm.geography.golden": 38,
    }

    assert {
        spec["topic"]
        for spec in registry["records"]
    } == set(expected_counts)

    assert sum(expected_counts.values()) == 728

    for spec in registry["records"]:
        topic = spec["topic"]
        count = expected_counts[topic]

        output_path = synthetic_root / spec["file"]
        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        rows = []

        for row_index in range(1, count + 1):
            row = {}

            if "key_field" in spec:
                key = spec["key_field"]
                row[key] = (
                    f"TEST-{topic}-{row_index:04d}"
                )

            if "key_fields" in spec:
                for key_index, key in enumerate(
                    spec["key_fields"],
                    start=1,
                ):
                    row[key] = (
                        f"TEST-{topic}-"
                        f"{row_index:04d}-"
                        f"{key_index:02d}"
                    )

            rows.append(row)

        output_path.write_text(
            json.dumps(rows),
            encoding="utf-8",
        )

    monkeypatch.setattr(
        publisher,
        "ROOT",
        synthetic_root,
    )


def test_registry_validates():
    counts = publisher.validate_registry()

    assert len(counts) == 6
    assert sum(counts.values()) == 728


def test_ordinary_plan_has_only_ordinary_topics():
    plan = publisher.build_publish_plan(
        "ordinary"
    )

    topics = {
        row["topic"]
        for row in plan
    }

    assert topics == {
        "mdm.sacco.golden",
        "mdm.agent.golden",
        "mdm.geography.golden",
    }

    assert len(plan) == 114


def test_restricted_plan_is_separate():
    plan = publisher.build_publish_plan(
        "restricted_identity"
    )

    topics = {
        row["topic"]
        for row in plan
    }

    assert topics == {
        "mdm.beneficiary.golden.restricted",
        "mdm.beneficiary.identity-alert.restricted",
        "mdm.crosswalk.restricted",
    }

    assert len(plan) == 614


def test_cross_authority_publish_fails():
    registry = publisher.load_registry()
    policies = publisher.topic_policy()

    spec = next(
        row
        for row in registry["records"]
        if row["topic"]
        == "mdm.beneficiary.golden.restricted"
    )

    with pytest.raises(PermissionError):
        publisher.validate_authority(
            spec,
            policies[spec["topic"]],
            "ordinary",
        )


def test_execute_fails_closed_by_default(
    monkeypatch,
):
    monkeypatch.delenv(
        "MDM_KAFKA_PUBLISH_ENABLED",
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match="publishing is disabled",
    ):
        publisher.execute_publish(
            "ordinary"
        )


def test_kafka_security_config_allows_local_plaintext(monkeypatch):
    monkeypatch.delenv("DP_SECURITY_MODE", raising=False)
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

    config = publisher.kafka_security_config()

    assert config["security.protocol"] == "PLAINTEXT"


def test_kafka_security_config_rejects_production_plaintext(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")

    with pytest.raises(
        ValueError,
        match="encrypted security protocol",
    ):
        publisher.kafka_security_config()


def test_kafka_security_config_accepts_production_ssl(monkeypatch):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SSL")
    monkeypatch.setenv(
        "KAFKA_SSL_CA_LOCATION",
        "/run/secrets/kafka-ca.pem",
    )

    config = publisher.kafka_security_config()

    assert config["security.protocol"] == "SSL"
    assert (
        config["ssl.ca.location"]
        == "/run/secrets/kafka-ca.pem"
    )


def test_kafka_security_config_requires_complete_production_sasl_ssl(
    monkeypatch,
):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv(
        "KAFKA_SSL_CA_LOCATION",
        "/run/secrets/kafka-ca.pem",
    )
    monkeypatch.delenv("KAFKA_SASL_MECHANISM", raising=False)
    monkeypatch.delenv("KAFKA_SASL_USERNAME", raising=False)
    monkeypatch.delenv("KAFKA_SASL_PASSWORD", raising=False)

    with pytest.raises(
        ValueError,
        match="SASL_SSL production configuration requires",
    ):
        publisher.kafka_security_config()


def test_kafka_security_config_accepts_complete_production_sasl_ssl(
    monkeypatch,
):
    monkeypatch.setenv("DP_SECURITY_MODE", "production")
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.setenv(
        "KAFKA_SSL_CA_LOCATION",
        "/run/secrets/kafka-ca.pem",
    )
    monkeypatch.setenv("KAFKA_SASL_MECHANISM", "PLAIN")
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "mdm-publisher")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "test-secret")

    config = publisher.kafka_security_config()

    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.mechanism"] == "PLAIN"
    assert config["sasl.username"] == "mdm-publisher"
    assert config["sasl.password"] == "test-secret"
