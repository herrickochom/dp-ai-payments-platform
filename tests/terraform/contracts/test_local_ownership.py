import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TF = ROOT / "infra" / "terraform"

CONTRACT = (
    TF
    / "contracts"
    / "local_ownership.json"
)


def load():
    return json.loads(CONTRACT.read_text())


def terraform_text():
    return "\n".join(
        path.read_text()
        for path in TF.rglob("*.tf")
    )


def test_local_is_non_production():
    c = load()

    assert c["environment"]["target"] == "local"
    assert c["environment"]["production"] is False


def test_compose_is_topology_authority():
    c = load()
    compose = c["ownership"]["docker_compose"]

    assert (
        compose["role"]
        == "LOCAL_APPLICATION_TOPOLOGY_AUTHORITY"
    )
    assert (
        compose["authoritative_file"]
        == "docker-compose.yaml"
    )
    assert compose["owns_service_topology"] is True
    assert compose["owns_local_container_lifecycle"] is True
    assert compose["owns_local_network_topology"] is True
    assert compose["owns_local_volume_topology"] is True


def test_terraform_does_not_own_compose_resources():
    c = load()
    tf = c["ownership"]["terraform"]

    assert tf["owns_individual_docker_containers"] is False
    assert tf["owns_docker_networks"] is False
    assert tf["owns_docker_volumes"] is False
    assert tf["owns_compose_service_topology"] is False


def test_dual_ownership_is_prohibited():
    c = load()
    p = c["dual_ownership_policy"]

    assert p["allowed"] is False
    assert p["terraform_docker_provider_allowed"] is False
    assert (
        p["terraform_docker_container_resources_allowed"]
        is False
    )
    assert (
        p["terraform_docker_network_resources_allowed"]
        is False
    )
    assert (
        p["terraform_docker_volume_resources_allowed"]
        is False
    )


def test_no_docker_provider_in_terraform_source():
    text = terraform_text()

    assert not re.search(
        r'provider\s+"docker"',
        text,
    )


def test_no_docker_resources_in_terraform_source():
    text = terraform_text()

    prohibited = (
        "docker_container",
        "docker_image",
        "docker_network",
        "docker_volume",
    )

    for resource in prohibited:
        assert resource not in text


def test_local_state_is_not_production_state():
    c = load()
    state = c["local_state_policy"]

    assert (
        state["terraform_state_classification"]
        == "NON_PRODUCTION_DISPOSABLE"
    )
    assert state["remote_backend_required"] is False
    assert state["production_state_claim_allowed"] is False


def test_plan_cannot_mutate_compose():
    c = load()
    p = c["execution_policy"]

    assert (
        p["terraform_may_directly_start_compose_during_plan"]
        is False
    )
    assert (
        p["terraform_may_directly_stop_compose_during_plan"]
        is False
    )
    assert (
        p["terraform_may_directly_mutate_compose_during_plan"]
        is False
    )


def test_protected_operations_remain_prohibited():
    c = load()
    p = c["protected_operations"]

    assert p["kafka_offset_reset_allowed"] is False
    assert p["raw_object_deletion_allowed"] is False
    assert p["token_link_rematerialisation_allowed"] is False
    assert p["gate3_recovery_execution_allowed"] is False
    assert p["cdc_implicit_activation_allowed"] is False
    assert p["unbounded_dbt_execution_allowed"] is False


def test_local_readme_documents_ownership_boundary():
    text = (
        TF
        / "targets"
        / "local"
        / "README.md"
    ).read_text()

    assert (
        "Docker Compose remains the authoritative "
        "local application topology"
    ) in text

    assert (
        "Terraform Docker provider resources "
        "are intentionally prohibited"
    ) in text
