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

POLICY = BASE / "deployment_enablement.json"
INVENTORY = BASE / "compose_inventory.json"


def load(path):
    return json.loads(path.read_text())


def test_policy_exists():
    assert POLICY.is_file()


def test_inventory_exists():
    assert INVENTORY.is_file()


def test_inventory_has_authoritative_compose_source():
    c = load(INVENTORY)

    assert c["source"] == "docker-compose.yaml"
    assert c["authoritative"] is True
    assert isinstance(c["default_services"], list)
    assert isinstance(c["profiles"], list)


def test_deployment_remains_disabled():
    c = load(POLICY)

    assert c["status"] == "DESIGN_ONLY"
    assert c["deployment"]["currently_enabled"] is False


def test_explicit_operator_intent_is_required():
    c = load(POLICY)
    d = c["deployment"]

    assert d["explicit_operator_intent_required"] is True
    assert d["preflight_required"] is True
    assert d["scope_must_be_explicit"] is True


def test_implicit_all_profiles_is_prohibited():
    c = load(POLICY)

    assert (
        c["deployment"]["implicit_all_profiles_allowed"]
        is False
    )

    assert (
        c["scope_policy"]["all_profiles_scope"]["allowed"]
        is False
    )


def test_default_scope_has_no_profiles():
    c = load(POLICY)

    scope = c["scope_policy"]["default_core_scope"]

    assert scope["defined"] is True
    assert scope["compose_profiles"] == []


def test_profile_names_must_be_governed():
    c = load(POLICY)

    scope = c["scope_policy"]["profile_scope"]

    assert scope["defined"] is True
    assert (
        scope["profile_names_must_exist_in_inventory"]
        is True
    )
    assert (
        scope["arbitrary_profile_names_allowed"]
        is False
    )


def test_mutating_compose_actions_remain_disabled():
    c = load(POLICY)
    actions = c["compose_actions"]

    assert actions["config"] is True
    assert actions["ps"] is True

    for action in (
        "up",
        "pull",
        "build",
        "restart",
        "stop",
        "down",
        "rm",
    ):
        assert actions[action] is False


def test_destructive_operations_are_denied():
    c = load(POLICY)

    for value in c["destructive_operations"].values():
        assert value is False


def test_data_safety_operations_are_denied():
    c = load(POLICY)

    for value in c["data_safety"].values():
        assert value is False


def test_future_enablement_requires_acceptance():
    c = load(POLICY)
    f = c["future_enablement"]

    assert f["requires_separate_acceptance_stage"] is True
    assert f["deploy_command_must_be_explicit"] is True
    assert f["approved_scope_required"] is True
    assert f["post_deployment_acceptance_required"] is True
