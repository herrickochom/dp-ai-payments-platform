"""Offline contract for explicit MinIO IAM provisioning."""

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "platform/minio/provision_object_store_identities.py"
SHELL_PATH = ROOT / "platform/minio/provision-object-store-identities.sh"
spec = importlib.util.spec_from_file_location("object_store_provisioner", MODULE_PATH)
provisioner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(provisioner)

EXPECTED = {
    "raw_ingest": "raw-ingest",
    "cdc_quarantine": "cdc-quarantine",
    "platform_raw_read": "platform-raw-read",
    "nessie_catalog": "nessie-catalog",
    "trino_iceberg": "trino-iceberg-read",
    "ordinary_transform": "ordinary-transform",
    "restricted_identity_transform": "restricted-transform",
    "ml_prediction_transform": "ml-transform",
}


def fake_environment():
    source = {
        "MINIO_ENDPOINT": "http://127.0.0.1:9000",
        "MINIO_ROOT_USER": "distinct-root-id",
        "MINIO_ROOT_PASSWORD": "distinct-root-secret-xyz",
    }
    for number, (_, _, access, secret) in enumerate(provisioner.IDENTITIES):
        source[access] = f"distinct-service-id-{number}"
        source[secret] = f"distinct-service-secret-{number}-xyz"
    return source


def test_exact_mapping_and_environment_contract():
    assert {identity: policy for identity, policy, _, _ in provisioner.IDENTITIES} == EXPECTED
    assert len(provisioner.IDENTITIES) == 8
    source = fake_environment()
    provisioner.require_apply_environment(source)
    for name in source:
        with pytest.raises(provisioner.ProvisionError, match="missing required environment"):
            provisioner.require_apply_environment({k: v for k, v in source.items() if k != name})
    script = MODULE_PATH.read_text() + SHELL_PATH.read_text()
    assert "minioadmin" not in script
    assert "eval " not in script and "set -x" not in script


def test_validate_is_offline_and_path_independent(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    result = subprocess.run(
        [str(SHELL_PATH), "--validate"], cwd=tmp_path,
        env={"PATH": os.environ["PATH"]}, text=True, capture_output=True, check=False,
    )
    assert result.returncode == 0
    assert "VALID" in result.stdout
    monkeypatch.setattr(provisioner.subprocess, "run", lambda *args, **kwargs: pytest.fail("mc invoked"))
    assert provisioner.main(["--validate"], {}) == 0
    assert "VALID" in capsys.readouterr().out
    assert provisioner.POLICY_DIR == ROOT / "platform/minio/policies"


def test_dry_run_is_offline_and_secret_free(monkeypatch, capsys):
    source = fake_environment()
    monkeypatch.setattr(provisioner.subprocess, "run", lambda *args, **kwargs: pytest.fail("mc invoked"))
    assert provisioner.main(["--dry-run"], source) == 0
    captured = capsys.readouterr()
    for identity, policy in EXPECTED.items():
        assert f"{identity} -> {policy}" in captured.out
    for secret in source.values():
        assert secret not in captured.out + captured.err


def test_missing_apply_environment_fails_before_mc(monkeypatch):
    monkeypatch.setattr(provisioner.subprocess, "run", lambda *args, **kwargs: pytest.fail("mc invoked"))
    assert provisioner.main(["--apply"], {}) == 1


def test_policy_and_attachment_commands_use_intended_mapping():
    source = fake_environment()
    commands = provisioner.policy_commands()
    assert len(commands) == 8
    for index, (_, policy, access, secret) in enumerate(provisioner.IDENTITIES):
        assert commands[index] == [
            "admin", "policy", "create", provisioner.ALIAS,
            policy, str(provisioner.POLICY_DIR / f"{policy}.json"),
        ]
        assert provisioner.attachment_command(policy, source[access]) == [
            "admin", "policy", "attach", provisioner.ALIAS, policy, "--user", source[access],
        ]
    assert source["MINIO_ROOT_USER"] not in str(commands)
    assert source["MINIO_ROOT_PASSWORD"] not in str(commands)
    assert all(source[secret] not in str(commands) for _, _, _, secret in provisioner.IDENTITIES)
    assert source["RAW_INGEST_S3_ACCESS_KEY_ID"] not in provisioner.admin_environment(source)[f"MC_HOST_{provisioner.ALIAS}"]
    assert source["MINIO_ROOT_USER"] in provisioner.admin_environment(source)[f"MC_HOST_{provisioner.ALIAS}"]


def simulated_apply(monkeypatch, source, initial_users, *, fail_on_add=None):
    users = {key: set(policies) for key, policies in initial_users.items()}
    seen = []

    def fake_mc(command, environment):
        seen.append(command)
        if command[:3] == ["admin", "user", "ls"]:
            return "".join(
                f"enabled {key} {','.join(sorted(policies))}\n" for key, policies in users.items()
            )
        if command[:3] == ["admin", "user", "add"]:
            if command[4] == fail_on_add:
                raise provisioner.ProvisionError("simulated creation failure")
            assert command[4] not in users
            users[command[4]] = set()
        if command[:3] == ["admin", "policy", "attach"]:
            users[command[6]].add(command[4])
        return ""

    monkeypatch.setattr(provisioner, "mc", fake_mc)
    return users, seen


def test_all_absent_users_are_created_and_attached(monkeypatch, capsys):
    source = fake_environment()
    users, seen = simulated_apply(monkeypatch, source, {})
    provisioner.apply(source)
    assert len([command for command in seen if command[:3] == ["admin", "user", "add"]]) == 8
    assert len([command for command in seen if command[:3] == ["admin", "policy", "attach"]]) == 8
    assert len([command for command in seen if command[:3] == ["admin", "policy", "create"]]) == 8
    for _, policy, access, secret in provisioner.IDENTITIES:
        assert users[source[access]] == {policy}
    output = capsys.readouterr()
    assert all(source[secret] not in output.out + output.err for _, _, _, secret in provisioner.IDENTITIES)


def test_all_desired_users_are_reused_without_rotation(monkeypatch, capsys):
    source = fake_environment()
    initial = {source[access]: {policy} for _, policy, access, _ in provisioner.IDENTITIES}
    _, seen = simulated_apply(monkeypatch, source, initial)
    provisioner.apply(source)
    assert not any(command[:3] == ["admin", "user", "add"] for command in seen)
    assert not any(command[:3] == ["admin", "policy", "attach"] for command in seen)
    assert len([command for command in seen if command[:3] == ["admin", "policy", "create"]]) == 8
    assert "8 existing user secret(s) were not verified or rotated" in capsys.readouterr().out


@pytest.mark.parametrize("existing_count", [1, 3, 7])
def test_mixed_and_partial_state_recovers(monkeypatch, existing_count):
    source = fake_environment()
    initial = {
        source[access]: {policy}
        for _, policy, access, _ in provisioner.IDENTITIES[:existing_count]
    }
    users, seen = simulated_apply(monkeypatch, source, initial)
    provisioner.apply(source)
    adds = [command for command in seen if command[:3] == ["admin", "user", "add"]]
    assert len(adds) == 8 - existing_count
    assert {command[4] for command in adds} == {
        source[access] for _, _, access, _ in provisioner.IDENTITIES[existing_count:]
    }
    assert all(users[source[access]] == {policy} for _, policy, access, _ in provisioner.IDENTITIES)


def test_old_key_with_service_policy_fails_closed(monkeypatch):
    source = fake_environment()
    _, seen = simulated_apply(monkeypatch, source, {"old-access-key": {"raw-ingest"}})
    with pytest.raises(provisioner.ProvisionError, match="unexpected managed policy"):
        provisioner.apply(source)
    assert seen == [["admin", "user", "ls", provisioner.ALIAS]]


def test_desired_key_with_another_managed_policy_fails_before_mutation(monkeypatch):
    source = fake_environment()
    key = source["RAW_INGEST_S3_ACCESS_KEY_ID"]
    _, seen = simulated_apply(monkeypatch, source, {key: {"nessie-catalog"}})
    with pytest.raises(provisioner.ProvisionError, match="unexpected managed policy"):
        provisioner.apply(source)
    assert seen == [["admin", "user", "ls", provisioner.ALIAS]]


def test_existing_desired_user_missing_attachment_gets_only_attachment(monkeypatch):
    source = fake_environment()
    key = source["RAW_INGEST_S3_ACCESS_KEY_ID"]
    _, seen = simulated_apply(monkeypatch, source, {key: set()})
    provisioner.apply(source)
    assert not any(command[:3] == ["admin", "user", "add"] and command[4] == key for command in seen)
    assert [command for command in seen if command[:3] == ["admin", "policy", "attach"] and command[6] == key] == [
        provisioner.attachment_command("raw-ingest", key)
    ]


def test_partial_failure_can_be_retried_without_recreating_users(monkeypatch):
    source = fake_environment()
    failing_key = source[provisioner.IDENTITIES[3][2]]
    users, seen = simulated_apply(monkeypatch, source, {}, fail_on_add=failing_key)
    with pytest.raises(provisioner.ProvisionError, match="simulated creation failure"):
        provisioner.apply(source)
    assert len(users) == 3
    users, retry_seen = simulated_apply(monkeypatch, source, users)
    provisioner.apply(source)
    retry_adds = [command for command in retry_seen if command[:3] == ["admin", "user", "add"]]
    assert len(retry_adds) == 5
    assert all(command[4] not in set(users) - {source[row[2]] for row in provisioner.IDENTITIES[3:]} for command in retry_adds)


def test_policy_validation_rejects_broader_scope(tmp_path, monkeypatch):
    policy_dir = tmp_path / "policies"
    policy_dir.mkdir()
    for path in provisioner.POLICY_DIR.glob("*.json"):
        (policy_dir / path.name).write_bytes(path.read_bytes())
    monkeypatch.setattr(provisioner, "POLICY_DIR", policy_dir)
    raw = policy_dir / "raw-ingest.json"
    raw.write_text(raw.read_text().replace(
        "arn:aws:s3:::dp-ai-payment/raw/v2/*", "*"
    ))
    with pytest.raises(provisioner.ProvisionError):
        provisioner.validate_all()


def test_no_destructive_or_public_provisioning_commands():
    script = MODULE_PATH.read_text() + SHELL_PATH.read_text()
    for forbidden in (
        "mc rb", "mc rm", "admin user remove", "admin user rm",
        "admin policy remove", "admin policy rm", "anonymous set",
        "policy set download", "public access", "download access",
    ):
        assert forbidden not in script.lower()
    verbs = {tuple(command[:3]) for command in provisioner.policy_commands()}
    verbs.add(tuple(provisioner.attachment_command("raw-ingest", "key")[:3]))
    assert '["admin", "user", "add"' in script
    assert verbs == {
        ("admin", "policy", "create"),
        ("admin", "policy", "attach"),
    }


def test_no_implicit_apply_or_compose_startup():
    compose = (ROOT / "docker-compose.yaml").read_text()
    assert SHELL_PATH.name not in compose
    assert MODULE_PATH.name not in compose
    assert provisioner.main([], {}) == 2
