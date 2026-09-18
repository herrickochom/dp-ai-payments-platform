import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

BASE = (
    ROOT
    / "infra"
    / "terraform"
    / "targets"
    / "local"
    / "contracts"
)

CONTRACT = BASE / "deployment_command.json"
INVENTORY = BASE / "compose_inventory.json"

RENDERER = (
    ROOT
    / "infra"
    / "terraform"
    / "scripts"
    / "local-deployment-command.py"
)


def load(path):
    return json.loads(path.read_text())


def test_command_contract_exists():
    assert CONTRACT.is_file()


def test_renderer_exists():
    assert RENDERER.is_file()


def test_execution_remains_disabled():
    c = load(CONTRACT)

    assert c["execution"]["enabled"] is False
    assert c["execution"]["design_only"] is True
    assert (
        c["acceptance"]["command_may_be_executed"]
        is False
    )


def test_explicit_service_list_required():
    c = load(CONTRACT)

    assert (
        c["execution"][
            "explicit_service_list_required"
        ]
        is True
    )

    assert (
        c["execution"][
            "implicit_default_up_prohibited"
        ]
        is True
    )


def test_no_recreate_required():
    c = load(CONTRACT)

    assert (
        c["execution"]["no_recreate_required"]
        is True
    )

    assert "--no-recreate" in (
        c["command"]["arguments_prefix"]
    )


def test_core_inventory_is_nonempty_and_unique():
    inventory = load(INVENTORY)
    services = inventory["default_services"]

    assert services
    assert len(services) == len(set(services))


def test_prohibited_mutating_options():
    c = load(CONTRACT)

    required = {
        "--build",
        "--pull",
        "--force-recreate",
        "--remove-orphans",
        "--renew-anon-volumes",
        "--profile",
        "down",
        "stop",
        "restart",
        "rm",
    }

    assert required.issubset(
        set(c["prohibited_tokens"])
    )


def test_renderer_contains_no_execution_call():
    text = RENDERER.read_text()

    prohibited = (
        "subprocess.run",
        "subprocess.call",
        "subprocess.Popen",
        "os.system",
        "os.exec",
    )

    for token in prohibited:
        assert token not in text


def test_safety_controls_are_false():
    c = load(CONTRACT)

    for value in c["safety"].values():
        assert value is False


def test_separate_execution_acceptance_required():
    c = load(CONTRACT)

    assert (
        c["acceptance"][
            "separate_execution_acceptance_required"
        ]
        is True
    )
