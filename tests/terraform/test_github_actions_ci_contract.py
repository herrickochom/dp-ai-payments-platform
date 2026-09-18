import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

CONTRACT = (
    ROOT
    / "infra"
    / "terraform"
    / "contracts"
    / "github_actions_ci_contract.json"
)


def load():
    return json.loads(CONTRACT.read_text())


def test_contract_exists():
    assert CONTRACT.is_file()


def test_platform_is_github_actions():
    doc = load()
    assert doc["platform"] == "github_actions"


def test_repository_and_default_branch():
    doc = load()

    assert (
        doc["repository"]
        == "herrickochom/dp-pdm-ai-platform"
    )

    assert doc["default_branch"] == "main"


def test_pr_and_main_push_are_enabled():
    doc = load()

    assert doc["triggers"]["pull_request"] is True
    assert doc["triggers"]["push_main"] is True


def test_default_permissions_are_read_only():
    doc = load()

    assert doc["permissions"]["default"] == "read_only"
    assert doc["permissions"]["contents"] == "read"


def test_required_ci_jobs_exist():
    doc = load()

    required = {
        "source_quality",
        "terraform_governance",
        "cdc_mdm_regression",
        "compose_validation",
    }

    assert required == set(doc["jobs"])


def test_terraform_version_is_pinned():
    doc = load()

    assert (
        doc["jobs"]["terraform_governance"]
        ["terraform_version"]
        == "1.16.3"
    )


def test_mutating_operations_are_prohibited():
    doc = load()

    prohibited = doc["prohibited_actions"]

    required_prohibitions = (
        "terraform_init",
        "terraform_plan",
        "terraform_apply",
        "terraform_destroy",
        "docker_compose_up",
        "docker_compose_down",
        "docker_build",
        "cdc_activation",
        "mdm_execution",
        "dbt_execution",
        "kafka_execution",
        "raw_deletion",
        "token_link_rematerialisation",
        "gate3_recovery",
        "cloud_resource_mutation",
        "production_secrets",
    )

    for key in required_prohibitions:
        assert prohibited[key] is True


def test_production_deployment_disabled():
    doc = load()

    assert (
        doc["production_deployment"]["enabled"]
        is False
    )


def test_no_terraform_plan_artifact():
    doc = load()

    assert (
        doc["artifact_policy"]
        ["retain_terraform_plan"]
        is False
    )

    assert (
        doc["artifact_policy"]["retain_state"]
        is False
    )
