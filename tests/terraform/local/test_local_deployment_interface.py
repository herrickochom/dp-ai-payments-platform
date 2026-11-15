import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

CONTRACT = (
    ROOT
    / "contracts"
    / "infrastructure"
    / "local"
    / "deployment_interface.json"
)



def load():
    return json.loads(CONTRACT.read_text())


def test_local_interface_contract_exists():
    assert CONTRACT.is_file()


def test_compose_remains_authority():
    c = load()

    assert (
        c["authority"]["docker_compose"]
        == "LOCAL_APPLICATION_TOPOLOGY_AUTHORITY"
    )

    assert (
        c["compose"]["authoritative_file"]
        == "docker-compose.yaml"
    )

    assert (
        c["compose"][
            "individual_resource_ownership_by_terraform"
        ]
        is False
    )


def test_inspection_and_validation_are_read_only():
    c = load()

    assert c["modes"]["inspect"]["allowed"] is True
    assert c["modes"]["inspect"]["mutating"] is False

    assert c["modes"]["validate"]["allowed"] is True
    assert c["modes"]["validate"]["mutating"] is False


def test_deploy_is_not_yet_enabled():
    c = load()

    deploy = c["modes"]["deploy"]

    assert deploy["allowed"] is False
    assert deploy["mutating"] is True
    assert (
        deploy["requires_future_controlled_enablement"]
        is True
    )


def test_destroy_is_separately_protected():
    c = load()

    destroy = c["modes"]["destroy"]

    assert destroy["allowed"] is False
    assert destroy["mutating"] is True
    assert (
        destroy["requires_separate_explicit_action"]
        is True
    )


def test_terraform_execution_side_effects_prohibited():
    c = load()
    tf = c["terraform"]

    assert tf["plan_must_be_side_effect_free"] is True
    assert tf["local_exec_provisioner_allowed"] is False
    assert tf["remote_exec_provisioner_allowed"] is False
    assert (
        tf["null_resource_compose_execution_allowed"]
        is False
    )
    assert (
        tf["terraform_data_compose_execution_allowed"]
        is False
    )


def test_protected_operations_are_denied():
    c = load()

    for value in c["protected_operations"].values():
        assert value is False
