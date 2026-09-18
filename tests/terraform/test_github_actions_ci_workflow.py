from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def workflow():
    return WORKFLOW.read_text()


def test_workflow_exists():
    assert WORKFLOW.is_file()


def test_expected_jobs_exist():
    text = workflow()

    for job in (
        "source-quality:",
        "terraform-governance:",
        "cdc-mdm-regression:",
        "compose-validation:",
    ):
        assert job in text


def test_read_only_permissions():
    text = workflow()

    assert "contents: read" in text
    assert "contents: write" not in text
    assert "id-token: write" not in text


def test_terraform_version_pinned():
    assert 'terraform_version: "1.16.3"' in workflow()


def test_no_terraform_mutation():
    text = workflow().lower()

    for token in (
        "terraform init",
        "terraform plan",
        "terraform apply",
        "terraform destroy",
    ):
        assert token not in text


def test_no_compose_mutation():
    text = workflow().lower()

    for token in (
        "docker compose up",
        "docker compose down",
        "docker compose restart",
        "docker compose stop",
        "docker compose rm",
        "docker compose build",
    ):
        assert token not in text


def test_no_cloud_credentials():
    text = workflow()

    for token in (
        "secrets.",
        "AWS_ACCESS_KEY_ID",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AZURE_CLIENT_ID",
        "DP_TOKEN_KEY",
    ):
        assert token not in text


def test_required_test_commands():
    text = workflow()

    assert "python -m pytest -q tests/terraform" in text
    assert "python -m pytest -q tests/cdc tests/mdm" in text


def test_compose_is_validation_only():
    text = workflow()

    assert "docker compose config --quiet" in text
