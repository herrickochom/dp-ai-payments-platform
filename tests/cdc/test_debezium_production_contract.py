import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

TEMPLATE = (
    ROOT
    / "platform"
    / "cdc"
    / "debezium"
    / "templates"
    / "postgresql_connector.json"
)

CONTRACT = (
    ROOT
    / "platform"
    / "cdc"
    / "contracts"
    / "debezium_connector_contract.json"
)

REGISTRY = (
    ROOT
    / "platform"
    / "source_registry"
    / "contracts"
    / "source_systems.json"
)

CDC_CONTRACT = (
    ROOT
    / "platform"
    / "cdc"
    / "contracts"
    / "pdm_cdc_contract.json"
)


def load(path):
    return json.loads(path.read_text())


def test_connector_template_is_fail_closed_and_allowlisted():
    template = load(TEMPLATE)["config"]

    assert template["connector.class"] == (
        "io.debezium.connector.postgresql."
        "PostgresConnector"
    )

    assert template["plugin.name"] == "pgoutput"

    assert "schema.include.list" in template
    assert "table.include.list" in template

    assert template["publication.autocreate.mode"] == (
        "disabled"
    )

    assert "publication.name" in template
    assert "slot.name" in template


def test_connector_template_uses_configprovider_credentials():
    template = load(TEMPLATE)["config"]

    user = template["database.user"]
    password = template["database.password"]

    assert user.startswith("${file:")
    assert password.startswith("${file:")

    assert "CDC_DATABASE_USER" not in user
    assert "CDC_DATABASE_PASSWORD" not in password


def test_capture_controls_are_explicit():
    template = load(TEMPLATE)["config"]

    assert template["provide.transaction.metadata"] == "true"
    assert template["tombstones.on.delete"] == "true"

    assert template["snapshot.mode"] == (
        "${CDC_SNAPSHOT_MODE}"
    )

    assert template["heartbeat.interval.ms"] == (
        "${CDC_HEARTBEAT_INTERVAL_MS}"
    )

    assert template["errors.tolerance"] == "none"


def test_version_policy_requires_immutable_pin():
    contract = load(CONTRACT)

    policy = contract["version_policy"]

    assert policy["latest_allowed"] is False
    assert policy["floating_major_minor_allowed"] is False
    assert policy["immutable_release_pin_required"] is True
    assert policy["compatibility_evidence_required"] is True

    assert set(policy["compatibility_scope"]) == {
        "debezium",
        "kafka_connect",
        "kafka_broker",
        "postgresql",
    }


def test_postgresql_production_requirements():
    contract = load(CONTRACT)

    db = contract["database_requirements"]

    assert db["wal_level"] == "logical"
    assert db["plugin"] == "pgoutput"

    assert db["least_privilege_identity"] is True
    assert db["root_identity_allowed"] is False

    assert db["publication_required"] is True
    assert db["publication_autocreate_allowed"] is False

    assert db["replication_slot_required"] is True
    assert (
        db["replication_slot_drop_on_stop_allowed"]
        is False
    )

    assert db["replica_identity_policy_required"] is True

    assert (
        db["delete_before_image_requires_replica_identity"]
        is True
    )


def test_secret_policy_requires_configprovider():
    contract = load(CONTRACT)

    policy = contract["credential_policy"]

    assert policy[
        "plaintext_credentials_in_connector_template"
    ] is False

    assert policy[
        "plaintext_credentials_in_terraform"
    ] is False

    assert policy["secret_reference_required"] is True

    assert policy[
        "kafka_connect_config_provider_required"
    ] is True

    assert policy["config_provider"] == "FileConfigProvider"

    assert policy[
        "secret_material_committed_to_repository"
    ] is False


def test_operational_controls_are_required():
    contract = load(CONTRACT)

    ops = contract["operational_requirements"]

    required = (
        "connector_offset_persistence_required",
        "schema_history_durability_required",
        "replication_slot_monitoring_required",
        "wal_retention_monitoring_required",
        "connector_lag_monitoring_required",
        "connector_failure_monitoring_required",
        "heartbeat_monitoring_required",
        "dlq_or_quarantine_policy_required",
        "recovery_runbook_required",
    )

    for field in required:
        assert ops[field] is True


def test_activation_controls_remain_fail_closed():
    contract = load(CONTRACT)

    controls = contract["activation_controls"]

    assert controls["default"] == "DENY"

    assert (
        controls["connector_presence_does_not_activate_source"]
        is True
    )

    assert controls["source_registry_activation_required"] is True
    assert controls["cdc_contract_allowlist_required"] is True
    assert controls["database_preflight_required"] is True


def test_current_source_estate_remains_non_cdc():
    registry = load(REGISTRY)

    for source in registry["source_systems"]:
        if source["source_system"] == "pdm_mdm":
            assert source["adapter_type"] == "CDC_DATABASE"
            assert source["mutable_database_source"] is True
            assert source["cdc_configuration_status"] == "CONFIGURED_PENDING_VERSION_AND_GOVERNANCE_APPROVAL"
        else:
            assert source["adapter_type"] == "GENERATED_EVENT"
            assert source["mutable_database_source"] is False
        assert source["cdc_capable"] is False
        assert source["cdc_activated"] is False


def test_current_cdc_allowlist_remains_empty():
    contract = load(CDC_CONTRACT)

    assert contract["cdc_sources"] == []
    assert contract["status"] == "fail_closed_not_activated"
