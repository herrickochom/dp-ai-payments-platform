import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

CONTRACT = (
    ROOT
    / "contracts"
    / "infrastructure"
    / "local"
    / "deployment_preflight.json"
)



def load():
    return json.loads(CONTRACT.read_text())


def test_preflight_is_read_only():
    c = load()

    assert c["target"] == "local"
    assert c["production"] is False
    assert c["mutating"] is False


def test_required_governance_checks_exist():
    c = load()

    checks = c["required_checks"]

    for value in checks.values():
        assert value is True


def test_scope_is_fail_closed():
    c = load()
    scope = c["scope_policy"]

    assert scope["default_scope_name"] == "core"
    assert scope["default_scope_uses_no_profiles"] is True
    assert (
        scope["profile_scope_requires_inventory_membership"]
        is True
    )
    assert scope["implicit_all_profiles_allowed"] is False


def test_secret_values_must_not_be_printed():
    c = load()
    secret = c["secret_policy"]

    assert secret["secret_values_must_not_be_printed"] is True
    assert (
        secret["environment_variable_names_may_be_inspected"]
        is True
    )
    assert (
        secret["resolved_compose_environment_must_not_be_dumped"]
        is True
    )


def test_mutable_image_is_local_warning_only():
    c = load()
    image = c["image_policy"]

    assert (
        image["mutable_image_tags_allowed_for_local_preflight"]
        is True
    )
    assert (
        image["mutable_image_tags_allowed_for_production_promotion"]
        is False
    )
    assert image["mutable_image_tags_must_be_reported"] is True


def test_runtime_mutation_is_prohibited():
    c = load()

    for value in c["runtime_policy"].values():
        assert value is False


def test_data_mutation_is_prohibited():
    c = load()

    for value in c["data_safety"].values():
        assert value is False


def test_preflight_does_not_enable_deployment():
    c = load()

    assert (
        c["acceptance"][
            "deployment_remains_disabled_after_preflight"
        ]
        is True
    )
