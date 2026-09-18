import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

TARGETS = (
    ROOT
    / "infra"
    / "terraform"
    / "contracts"
    / "deployment_targets.json"
)

CAPABILITIES = (
    ROOT
    / "infra"
    / "terraform"
    / "contracts"
    / "platform_capabilities.json"
)

TARGET_ROOT = (
    ROOT
    / "infra"
    / "terraform"
    / "targets"
)


def load(path):
    return json.loads(path.read_text())


def test_four_targets_are_declared():
    doc = load(TARGETS)

    assert set(doc["targets"]) == {
        "local",
        "aws",
        "gcp",
        "azure",
    }


def test_implementation_order_is_explicit():
    doc = load(TARGETS)

    assert doc["implementation_order"] == [
        "local",
        "aws",
        "gcp",
        "azure",
    ]


def test_local_is_first_reference_environment():
    doc = load(TARGETS)
    local = doc["targets"]["local"]

    assert local["enabled"] is True
    assert local["implementation_order"] == 1
    assert (
        local["classification"]
        == "REFERENCE_INTEGRATION_ENVIRONMENT"
    )
    assert local["production_environment"] is False
    assert local["cloud_credentials_required"] is False
    assert local["remote_state_required"] is False
    assert local["high_availability_claim_allowed"] is False
    assert local["production_dr_claim_allowed"] is False


def test_cloud_targets_are_production_capable():
    doc = load(TARGETS)

    expected = {
        "aws": "aws",
        "gcp": "google",
        "azure": "azurerm",
    }

    for target, provider in expected.items():
        cfg = doc["targets"][target]

        assert cfg["enabled"] is True
        assert cfg["production_capable"] is True
        assert cfg["provider"] == provider
        assert (
            cfg["remote_state_required_for_production"]
            is True
        )
        assert cfg["production_iam_required"] is True
        assert cfg["production_kms_required"] is True
        assert (
            cfg["production_network_encryption_required"]
            is True
        )
        assert (
            cfg["production_observability_required"]
            is True
        )
        assert (
            cfg["production_backup_dr_required"]
            is True
        )


def test_architecture_cannot_fork_by_provider():
    doc = load(TARGETS)
    policy = doc["architecture_policy"]

    assert policy["provider_neutral_core_required"] is True
    assert policy["single_logical_platform_architecture"] is True
    assert (
        policy["provider_specific_architecture_forks_allowed"]
        is False
    )
    assert policy["direct_kafka_to_bronze_allowed"] is False
    assert policy["raw_first_ingestion_required"] is True
    assert (
        policy["cdc_activation_by_deployment_default"]
        is False
    )
    assert (
        policy[
            "token_link_rematerialisation_by_deployment_allowed"
        ]
        is False
    )


def test_core_data_platform_capabilities_are_portable():
    doc = load(CAPABILITIES)
    capabilities = doc["capabilities"]

    for name in (
        "compute",
        "networking",
        "service_identity",
        "object_storage",
        "event_streaming",
        "cdc",
        "relational_database",
        "lakehouse",
        "catalogue",
        "query_engine",
        "transformation",
        "orchestration",
        "business_intelligence",
    ):
        assert capabilities[name]["required"] is True
        assert capabilities[name]["portable"] is True


def test_reference_semantics_are_preserved():
    doc = load(CAPABILITIES)
    c = doc["capabilities"]

    assert c["object_storage"]["local_reference"] == "minio"
    assert c["event_streaming"]["reference_semantics"] == "kafka"
    assert c["cdc"]["reference_adapter"] == "debezium"
    assert c["cdc"]["runtime_activation_default"] == "DENY"
    assert c["relational_database"]["reference_engine"] == "postgresql"
    assert c["lakehouse"]["table_format"] == "iceberg"
    assert c["catalogue"]["reference_implementation"] == "nessie"
    assert c["query_engine"]["reference_implementation"] == "trino"
    assert c["transformation"]["reference_implementation"] == "dbt"
    assert c["orchestration"]["reference_implementation"] == "airflow"


def test_all_target_directories_exist():
    for target in (
        "local",
        "aws",
        "gcp",
        "azure",
    ):
        assert (
            TARGET_ROOT
            / target
            / "README.md"
        ).is_file()


def test_cloud_substitution_is_not_implicit():
    text = (
        TARGET_ROOT
        / "gcp"
        / "README.md"
    ).read_text()

    assert "does not by itself replace Kafka with Pub/Sub" in text
    assert "Iceberg with" in text
    assert "BigQuery" in text


def test_local_documentation_rejects_production_claim():
    text = (
        TARGET_ROOT
        / "local"
        / "README.md"
    ).read_text().lower()

    assert "not a production environment" in text
    assert "production high availability" in text
    assert "production disaster recovery" in text
    assert "cloud kms" in text
