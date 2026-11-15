import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

TARGETS = (
    ROOT
    / "contracts"
    / "infrastructure"
    / "deployment_targets.json"
)

CAPABILITIES = (
    ROOT
    / "contracts"
    / "infrastructure"
    / "platform_capabilities.json"
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


def test_declared_targets_have_explicit_classification():
    doc = load(TARGETS)
    targets = doc["targets"]

    assert set(targets) == {"local", "aws", "gcp", "azure"}

    for name, target in targets.items():
        assert target["enabled"] is True
        assert target["terraform_target_required"] is True

        if name == "local":
            assert (
                target["classification"]
                == "REFERENCE_INTEGRATION_ENVIRONMENT"
            )
        else:
            assert (
                target["classification"]
                == "CLOUD_DEPLOYMENT_TARGET"
            )


def test_cloud_substitution_is_not_implicit():
    doc = load(TARGETS)
    policy = doc["architecture_policy"]

    assert policy["provider_neutral_core_required"] is True
    assert policy["single_logical_platform_architecture"] is True
    assert (
        policy["provider_specific_architecture_forks_allowed"]
        is False
    )

    assert policy["raw_first_ingestion_required"] is True
    assert policy["direct_kafka_to_bronze_allowed"] is False


def test_local_contract_rejects_production_claim():
    doc = load(TARGETS)
    local = doc["targets"]["local"]

    assert local["production_environment"] is False
    assert local["high_availability_claim_allowed"] is False
    assert local["production_dr_claim_allowed"] is False
    assert local["cloud_credentials_required"] is False
    assert local["remote_state_required"] is False
