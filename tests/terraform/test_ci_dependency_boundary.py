from pathlib import Path
import json


ROOT = Path(__file__).resolve().parents[2]

REQUIREMENTS = ROOT / "requirements.txt"
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
CONTRACT = (
    ROOT
    / "infra"
    / "terraform"
    / "contracts"
    / "github_actions_ci_contract.json"
)


def test_root_requirements_exists():
    assert REQUIREMENTS.is_file()


def test_single_root_requirements_contains_test_runner():
    text = REQUIREMENTS.read_text().lower()

    assert "pytest==9.1.1" in text


def test_required_clean_runner_dependencies_are_declared():
    text = REQUIREMENTS.read_text().lower()

    required = (
        "boto3",
        "confluent-kafka",
        "pytz",
        "pytest==9.1.1",
    )

    for dependency in required:
        assert dependency in text


def test_workflow_uses_single_root_requirements():
    workflow = WORKFLOW.read_text()

    assert (
        workflow.count(
            "python -m pip install -r requirements.txt"
        )
        == 2
    )

    assert "requirements-ci.txt" not in workflow


def test_contract_records_single_dependency_authority():
    doc = json.loads(CONTRACT.read_text())

    policy = doc["dependency_policy"]

    assert (
        policy["runtime_requirements"]
        == "requirements.txt"
    )

    assert "ci_requirements" not in policy
