import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

CONTRACT = (
    ROOT
    / "infra"
    / "terraform"
    / "targets"
    / "local"
    / "contracts"
    / "deployment_interface.json"
)

SCRIPT = (
    ROOT
    / "infra"
    / "terraform"
    / "scripts"
    / "local-platform.sh"
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


def test_interface_script_exists():
    assert SCRIPT.is_file()


def test_script_does_not_contain_compose_mutation():
    text = SCRIPT.read_text()

    prohibited = (
        "docker compose up",
        "docker compose down",
        "docker compose start",
        "docker compose stop",
        "docker compose restart",
        "docker compose rm",
        "docker compose pull",
        "docker compose build",
    )

    for command in prohibited:
        assert command not in text


def test_script_governs_deploy_and_denies_destroy():
    text = SCRIPT.read_text()

    # Deploy is convergence-controlled. A runtime already
    # at desired state may return success without mutation.
    assert (
        "DEPLOYMENT_INTERFACE=CONVERGENCE_CONTROL"
        in text
    )
    assert "local-convergence.py" in text

    # Destroy remains an explicit interface mode, but the
    # script must not contain destructive implementations.
    assert "destroy)" in text

    prohibited = (
        "docker compose down",
        "docker compose rm",
        "docker volume rm",
        "terraform destroy",
        "rm -rf",
    )

    for token in prohibited:
        assert token not in text
