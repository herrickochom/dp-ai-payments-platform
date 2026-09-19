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


def test_ci_actions_are_pinned_to_verified_full_commit_shas():
    import re

    text = workflow()

    expected = {
        "actions/checkout":
            "11d5960a326750d5838078e36cf38b85af677262",
        "actions/setup-python":
            "a26af69be951a213d495a4c3e4e4022e16d87065",
        "hashicorp/setup-terraform":
            "b9cd54a3c349d3f38e8881555d616ced269862dd",
    }

    refs = re.findall(
        r"^\s*uses:\s*([^\s#]+)",
        text,
        flags=re.MULTILINE,
    )

    assert len(refs) == 7

    for ref in refs:
        name, sha = ref.rsplit("@", 1)

        assert name in expected
        assert sha == expected[name]
        assert re.fullmatch(
            r"[0-9a-f]{40}",
            sha,
        )


def test_ci_runner_is_pinned_to_ubuntu_2404():
    text = workflow()

    assert text.count(
        "runs-on: ubuntu-24.04"
    ) == 4

    assert "ubuntu-latest" not in text


def test_source_quality_is_event_base_aware():
    text = workflow()

    assert "fetch-depth: 0" in text
    assert "github.event_name" in text

    assert (
        "github.event.pull_request.base.sha"
        in text
    )

    assert "github.event.before" in text

    assert (
        'git diff --check "$base" HEAD'
        in text
    )

    assert (
        "git diff-tree --check --root HEAD"
        in text
    )


def test_orchestration_regression_is_in_ci():
    text = workflow()

    assert (
        "python -m pytest -q "
        "orchestration/airflow/tests"
        in text
    )
