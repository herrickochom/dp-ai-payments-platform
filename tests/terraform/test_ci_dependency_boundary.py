import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

RUNTIME = ROOT / "requirements.txt"
CI = ROOT / "requirements-ci.txt"
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"

CONTRACT = (
    ROOT
    / "infra"
    / "terraform"
    / "contracts"
    / "github_actions_ci_contract.json"
)


def test_ci_requirements_exists():
    assert CI.is_file()


def test_pytest_is_ci_only():
    runtime = RUNTIME.read_text().lower()
    ci = CI.read_text().lower()

    assert "pytest" not in runtime
    assert "pytest==9.1.1" in ci


def test_ci_dependencies_are_pinned():
    lines = [
        line.strip()
        for line in CI.read_text().splitlines()
        if line.strip()
        and not line.strip().startswith("#")
    ]

    assert lines

    for line in lines:
        assert "==" in line


def test_workflow_installs_runtime_and_ci_requirements():
    text = WORKFLOW.read_text()

    assert (
        text.count(
            "python -m pip install -r requirements.txt"
        )
        == 2
    )

    assert (
        text.count(
            "python -m pip install -r requirements-ci.txt"
        )
        == 2
    )


def test_contract_records_dependency_boundary():
    doc = json.loads(CONTRACT.read_text())

    policy = doc["dependency_policy"]

    assert (
        policy["runtime_requirements"]
        == "requirements.txt"
    )

    assert (
        policy["ci_requirements"]
        == "requirements-ci.txt"
    )

    assert (
        policy["runtime_and_ci_separated"]
        is True
    )

    assert (
        policy["pytest_runtime_dependency"]
        is False
    )

    assert (
        policy["pytest_ci_dependency"]
        is True
    )

    assert (
        policy["ci_dependency_versions_pinned"]
        is True
    )
