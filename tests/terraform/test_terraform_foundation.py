from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TF = ROOT / "infra" / "terraform"


def read(relative):
    return (ROOT / relative).read_text()


def test_required_terraform_version_is_bounded():
    text = read("infra/terraform/versions.tf")

    assert 'required_version = "~> 1.16.0"' in text


def test_no_provider_block_exists():
    tf_files = list(TF.rglob("*.tf"))

    combined = "\n".join(
        path.read_text()
        for path in tf_files
    )

    assert 'provider "' not in combined


def test_no_resource_block_exists():
    tf_files = list(TF.rglob("*.tf"))

    combined = "\n".join(
        path.read_text()
        for path in tf_files
    )

    assert 'resource "' not in combined


def test_no_backend_block_exists():
    tf_files = list(TF.rglob("*.tf"))

    combined = "\n".join(
        path.read_text()
        for path in tf_files
    )

    assert 'backend "' not in combined


def test_environment_validation_is_present():
    text = read("infra/terraform/variables.tf")

    assert '"dev"' in text
    assert '"staging"' in text
    assert '"prod"' in text
    assert "validation {" in text


def test_provider_selected_remains_false():
    text = read("infra/terraform/outputs.tf")

    assert "provider_selected = false" in text


def test_environment_directories_exist():
    for environment in (
        "dev",
        "staging",
        "prod",
    ):
        assert (
            TF
            / "environments"
            / environment
            / "README.md"
        ).is_file()


def test_module_boundaries_exist():
    modules = (
        "networking",
        "identity",
        "storage",
        "kafka",
        "cdc",
        "database",
        "airflow",
        "observability",
        "backup",
    )

    for module in modules:
        assert (
            TF
            / "modules"
            / module
            / "README.md"
        ).is_file()


def test_backend_governance_is_explicit():
    text = read("infra/terraform/BACKEND.md").lower()

    assert "remote backend" in text
    assert "state locking" in text
    assert "least privilege" in text
    assert "local terraform state is not" in text


def test_terraform_runtime_artifacts_are_ignored():
    text = read(".gitignore").splitlines()

    required = {
        ".terraform/",
        "*.tfstate",
        "*.tfstate.*",
        "*.tfplan",
        "override.tf",
        "override.tf.json",
        "*_override.tf",
        "*_override.tf.json",
        ".terraform.tfstate.lock.info",
    }

    assert required.issubset(set(text))


def test_no_tfvars_files_are_committed():
    committed = list(TF.rglob("*.tfvars"))

    assert committed == []


def test_no_auto_tfvars_files_are_committed():
    committed = list(TF.rglob("*.auto.tfvars"))

    assert committed == []
