import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TF = ROOT / "infra" / "terraform"

CONTRACT = (
    TF
    / "targets"
    / "local"
    / "contracts"
    / "plan_contract.json"
)


def load():
    return json.loads(CONTRACT.read_text())


def terraform_text():
    return "\n".join(
        path.read_text()
        for path in TF.rglob("*.tf")
    )


def test_plan_contract_exists():
    assert CONTRACT.is_file()


def test_plan_is_non_mutating():
    c = load()
    r = c["requirements"]

    assert r["refresh_disabled"] is True
    assert r["managed_resource_count"] == 0
    assert r["resource_change_count"] == 0
    assert r["compose_mutation_allowed"] is False
    assert r["apply_allowed"] is False
    assert r["destroy_allowed"] is False
    assert r["cdc_activation_allowed"] is False


def test_plan_has_no_provider_or_backend():
    c = load()
    r = c["requirements"]

    assert r["provider_count"] == 0
    assert r["backend_count"] == 0


def test_plan_artifact_is_temporary():
    c = load()
    p = c["plan_artifact"]

    assert p["temporary"] is True
    assert p["repository_storage_allowed"] is False
    assert p["retention_after_validation"] is False


def test_acceptance_requires_zero_resource_changes():
    c = load()
    a = c["acceptance"]

    assert a["terraform_plan_exit_code"] == 0
    assert a["resource_changes_must_be_zero"] is True
    assert (
        a["runtime_infrastructure_mutation_must_be_zero"]
        is True
    )


def test_terraform_source_has_zero_resources():
    text = terraform_text()

    assert not re.search(
        r'resource\s+"',
        text,
    )


def test_terraform_source_has_zero_providers():
    text = terraform_text()

    assert not re.search(
        r'provider\s+"',
        text,
    )


def test_terraform_source_has_zero_backends():
    text = terraform_text()

    assert not re.search(
        r'backend\s+"',
        text,
    )


def test_no_execution_provisioners_exist():
    text = terraform_text()

    prohibited = (
        "local-exec",
        "remote-exec",
        "null_resource",
    )

    for value in prohibited:
        assert value not in text
