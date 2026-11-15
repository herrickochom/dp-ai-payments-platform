import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

CONTRACT = (
    ROOT
    / "contracts"
    / "infrastructure"
    / "local"
    / "plan_contract.json"
)


def load():
    return json.loads(CONTRACT.read_text())




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










def test_local_terraform_implementation_is_absent():
    assert not (
        ROOT / "infrastructure" / "terraform" / "local"
    ).exists()


def test_gcp_terraform_is_separately_owned():
    assert (
        ROOT / "infrastructure" / "terraform" / "gcp"
    ).is_dir()


def test_local_compose_does_not_claim_terraform_ownership():
    path = ROOT / "contracts/infrastructure/local/deployment_interface.json"
    interface = json.loads(path.read_text())

    assert interface["compose"]["individual_resource_ownership_by_terraform"] is False
    assert interface["terraform"]["local_exec_provisioner_allowed"] is False
    assert interface["terraform"]["remote_exec_provisioner_allowed"] is False
    assert interface["terraform"]["null_resource_compose_execution_allowed"] is False
    assert interface["terraform"]["terraform_data_compose_execution_allowed"] is False
