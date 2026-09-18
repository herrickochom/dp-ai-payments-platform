import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

IAC = (
    ROOT
    / "infra"
    / "terraform"
    / "contracts"
    / "platform_iac_contract.json"
)

CICD = (
    ROOT
    / "infra"
    / "terraform"
    / "contracts"
    / "cicd_quality_gates.json"
)


def load(path):
    return json.loads(path.read_text())


def test_provider_is_not_invented():
    c = load(IAC)
    p = c["provider_policy"]

    assert p["provider_selected"] is False
    assert p["provider_assumption_allowed"] is False
    assert (
        p["provider_specific_resources_allowed_before_selection"]
        is False
    )


def test_production_state_is_governed():
    c = load(IAC)
    s = c["state_policy"]

    assert s["remote_state_required_for_production"] is True
    assert s["state_encryption_required"] is True
    assert s["state_locking_required"] is True
    assert s["state_access_least_privilege_required"] is True
    assert s["local_production_state_allowed"] is False


def test_secrets_fail_closed():
    c = load(IAC)
    s = c["security_policy"]

    assert s["plaintext_secrets_in_source"] is False
    assert s["plaintext_secrets_in_tfvars"] is False
    assert s["plaintext_secrets_in_ci_logs"] is False
    assert s["approved_secret_backend_required"] is True


def test_kafka_is_governed_as_code():
    c = load(IAC)
    k = c["kafka_policy"]

    assert k["topic_configuration_managed_as_code"] is True
    assert k["acl_managed_as_code"] is True
    assert k["retention_managed_as_code"] is True
    assert k["replication_managed_as_code"] is True
    assert k["tls_required_for_production"] is True
    assert k["anonymous_access_allowed"] is False


def test_cdc_cannot_be_implicitly_activated():
    c = load(IAC)
    d = c["cdc_policy"]

    assert d["source_registry_is_activation_authority"] is True
    assert d["explicit_source_allowlist_required"] is True
    assert d["terraform_presence_activates_cdc"] is False
    assert d["direct_kafka_to_bronze_allowed"] is False
    assert d["airflow_polling_for_cdc_allowed"] is False


def test_raw_first_architecture():
    c = load(IAC)
    d = c["data_platform_policy"]

    assert d["raw_first_ingestion_required"] is True
    assert d["raw_storage_persistent"] is True
    assert d["staging_ephemeral"] is True
    assert d["bronze_persistent"] is True


def test_mdm_privacy_boundary():
    c = load(IAC)
    m = c["mdm_policy"]

    assert m["restricted_identity_boundary_required"] is True
    assert m["golden_record_storage_restricted"] is True
    assert m["source_crosswalk_storage_restricted"] is True
    assert (
        m["ordinary_analytics_direct_identity_allowed"]
        is False
    )
    assert m["canonical_token_boundary_required"] is True
    assert (
        m["token_link_rematerialisation_by_deployment_allowed"]
        is False
    )


def test_production_apply_requires_controlled_path():
    c = load(IAC)
    d = c["deployment_policy"]

    assert d["plan_before_apply_required"] is True
    assert d["protected_environment_approval_required"] is True
    assert (
        d["direct_unreviewed_production_apply_allowed"]
        is False
    )
    assert d["post_deployment_acceptance_required"] is True


def test_ci_quality_gates():
    c = load(CICD)
    q = c["pull_request_ci"]

    required = [
        "python_tests_required",
        "cdc_tests_required",
        "mdm_tests_required",
        "privacy_security_tests_required",
        "dbt_contract_validation_required",
        "terraform_fmt_check_required",
        "terraform_validate_required",
        "terraform_static_security_analysis_required",
        "container_build_validation_required",
        "sbom_generation_required",
        "container_vulnerability_scan_required",
    ]

    assert all(q[name] is True for name in required)


def test_ci_cannot_apply_from_pull_request():
    c = load(CICD)
    p = c["prohibited_ci_behaviour"]

    assert p["terraform_apply_on_pull_request"] is True
    assert p["terraform_destroy_on_pull_request"] is True
    assert p["automatic_cdc_source_activation"] is True
    assert p["kafka_offset_reset"] is True
    assert p["raw_object_deletion"] is True
    assert p["token_link_rematerialisation"] is True
    assert p["production_secret_echo"] is True


def test_cd_requires_approval_and_acceptance():
    c = load(CICD)
    cd = c["protected_environment_cd"]

    assert cd["human_approval_required"] is True
    assert cd["approved_plan_required"] is True
    assert cd["immutable_artifact_required"] is True
    assert (
        cd["terraform_apply_allowed_only_after_approval"]
        is True
    )
    assert cd["post_deployment_acceptance_required"] is True
