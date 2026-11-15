"""GCP Terraform foundation governance tests.

Static assertions only. No Terraform CLI or cloud operations.
"""

from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[3]
TF = ROOT / "infrastructure" / "terraform" / "gcp"
DEV = TF / "environments" / "dev"


def read(path):
    return path.read_text(encoding="utf-8")


def test_canonical_terraform_root_exists():
    assert TF.is_dir()
    assert DEV.is_dir()


def test_required_terraform_version_is_bounded():
    text = read(DEV / "versions.tf")
    assert re.search(
        r'required_version\s*=\s*"~> 1\.16\.0"',
        text,
    )


def test_google_provider_is_declared():
    versions = read(DEV / "versions.tf")
    providers = read(DEV / "providers.tf")
    assert 'source  = "hashicorp/google"' in versions
    assert 'provider "google"' in providers


def test_gcs_backend_is_explicit():
    backend = read(DEV / "backend.tf")
    assert 'backend "gcs"' in backend
    assert 'bucket = "pdm-dev-tfstate-91932345778"' in backend
    assert 'prefix = "terraform/environments/dev"' in backend


def test_real_gcp_resources_exist():
    files = list(DEV.glob("*.tf"))
    combined = "\\n".join(read(path) for path in files)
    assert re.search(r'resource\s+"google_', combined)


def test_gcp_module_boundaries_exist():
    modules = TF / "modules"
    for name in (
        "composer_environment",
        "dataplex_payments",
        "governed_gcs_bucket",
        "pubsub_payments",
    ):
        assert (modules / name / "main.tf").is_file()


def test_development_environment_files_exist():
    for name in (
        "main.tf",
        "versions.tf",
        "providers.tf",
        "backend.tf",
        "variables.tf",
        "outputs.tf",
    ):
        assert (DEV / name).is_file()


def test_terraform_runtime_artifacts_are_ignored():
    text = read(TF / ".gitignore")
    for pattern in (
        ".terraform/",
        "*.tfstate",
        "*.tfstate.*",
        "*.tfvars",
        "*.tfplan",
    ):
        assert pattern in text.splitlines()


def test_no_terraform_state_files_in_git_index():
    import subprocess

    result = subprocess.run(
        ["git", "ls-files", "--", "infrastructure/terraform/gcp"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    tracked = result.stdout.splitlines()
    assert not [
        path for path in tracked
        if path.endswith((".tfstate", ".tfplan", ".tfvars"))
    ]


def test_no_legacy_terraform_directory():
    assert not (ROOT / "infra" / "terraform").exists()
