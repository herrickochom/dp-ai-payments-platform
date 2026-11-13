import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

CONTRACT_ROOT = (
    ROOT
    / "infra"
    / "terraform"
    / "targets"
    / "local"
    / "contracts"
)

POLICY = (
    CONTRACT_ROOT
    / "deployment_convergence.json"
)

CORE = (
    CONTRACT_ROOT
    / "operational_core.json"
)

CHECKER = (
    ROOT
    / "infra"
    / "terraform"
    / "scripts"
    / "local-convergence.py"
)

PLATFORM = (
    ROOT
    / "infra"
    / "terraform"
    / "scripts"
    / "local-platform.sh"
)


def load(path):
    return json.loads(path.read_text())


def test_convergence_policy_exists():
    assert POLICY.is_file()


def test_convergence_checker_exists():
    assert CHECKER.is_file()


def test_operational_core_has_seven_services():
    core = load(CORE)

    assert len(core["services"]) == 7


def test_converged_state_is_success_noop():
    policy = load(POLICY)

    semantics = policy[
        "deployment_semantics"
    ]

    assert (
        semantics["already_converged_is_success"]
        is True
    )

    assert (
        semantics["already_converged_is_noop"]
        is True
    )


def test_blind_compose_up_prohibited():
    policy = load(POLICY)

    assert (
        policy["deployment_semantics"]
        ["blind_compose_up_prohibited"]
        is True
    )


def test_mutation_on_drift_remains_disabled():
    policy = load(POLICY)

    assert (
        policy["current_enablement"]
        ["mutation_on_drift_enabled"]
        is False
    )


def test_drift_fails_closed():
    policy = load(POLICY)

    drift = policy["drift_policy"]

    assert drift["missing_service"] == "FAIL_CLOSED"
    assert drift["stopped_service"] == "FAIL_CLOSED"
    assert drift["unhealthy_service"] == "FAIL_CLOSED"


def test_future_mutation_is_explicit_and_no_recreate():
    policy = load(POLICY)

    future = policy[
        "future_mutation_command_policy"
    ]

    assert future["no_recreate"] is True
    assert future["no_dependencies"] is True
    assert future["explicit_services_only"] is True

    assert (
        future[
            "only_non_running_approved_services"
        ]
        is True
    )


def test_protected_operations_disabled():
    policy = load(POLICY)

    for value in (
        policy["protected_operations"].values()
    ):
        assert value is False


def test_checker_contains_no_compose_up():
    text = CHECKER.read_text()

    assert '"up"' not in text
    assert "'up'" not in text
    assert "compose up" not in text


def test_platform_deploy_uses_convergence_control():
    text = PLATFORM.read_text()

    assert (
        "DEPLOYMENT_INTERFACE=CONVERGENCE_CONTROL"
        in text
    )

    assert "local-convergence.py" in text
    assert "current_activation_summary" in text
