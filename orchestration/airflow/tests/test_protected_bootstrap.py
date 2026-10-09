from __future__ import annotations

import inspect
import sys
import types

import pytest
from pydantic import ValidationError

from orchestration.job_runner.protected_bootstrap_execution import build_protected_bootstrap_command
from orchestration.job_runner import protected_prerequisites
from orchestration.transform_runtime.execution_plan import EXECUTION_BATCHES, TOKEN_LINK
from orchestration.transform_runtime.ledger import LedgerError, TransformLedger, model_fingerprint
from orchestration.transform_runtime.models import SubmitProtectedBootstrapRequest
from orchestration.transform_runtime.auth import require_bootstrap_operator_identity
from fastapi import HTTPException
from orchestration.transform_runtime.protected_bootstrap import (
    INITIAL_TOKEN_LINK_CREATION,
    get_protected_bootstrap,
)
from services.shared import materialise_token_link as materializer


RUN_ID = "tr_" + "1" * 32
EXECUTION_ID = "be_" + "2" * 32


def source(tmp_path):
    return {
        "S3_ENDPOINT": "https://minio.example:9000",
        "S3_USE_SSL": "true",
        "S3_CA_BUNDLE": "/certs/ca.pem",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "payments",
        "RAW_ROOT": "raw", "RAW_VERSION": "v2", "RAW_PREFIX": "raw/v2",
        "WAREHOUSE_PREFIX": "warehouse", "WAREHOUSE_URI": "s3://payments/warehouse",
        "NESSIE_WAREHOUSE": "s3://payments/warehouse",
        "DBT_S3_URL_STYLE": "path", "NESSIE_ENDPOINT": "https://nessie.example",
        "NESSIE_TRANSFORM_TOKEN": "authority-token",
        "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID": "restricted-access",
        "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY": "restricted-secret",
        "DBT_TRINO_PASSWORD": "trino-password",
        "DP_SECRET_SOURCE": "mounted-files", "DP_SECRET_DIR": str(tmp_path),
    }


def test_contract_is_fixed_unscheduled_and_initial_create_only():
    operation = get_protected_bootstrap(INITIAL_TOKEN_LINK_CREATION, 1)
    assert operation.authority == "restricted_identity_transform"
    assert operation.prerequisite_batch == "C4_RAW_02"
    assert operation.initial_create_only is True
    assert operation.scheduled is False
    assert operation.airflow_invocation_allowed is False
    assert all(operation.final_target not in batch.model_allowlist for batch in EXECUTION_BATCHES.values())


def test_request_rejects_arbitrary_surface():
    with pytest.raises(ValidationError):
        SubmitProtectedBootstrapRequest(operation_id=INITIAL_TOKEN_LINK_CREATION,
            operation_version=1,idempotency_key="k"*16,sql="DROP TABLE x")


def test_airflow_credential_cannot_admit_bootstrap(monkeypatch):
    monkeypatch.setenv("DP_SECRET_SOURCE", "environment")
    monkeypatch.setenv("TRANSFORM_RUNTIME_AIRFLOW_TOKEN", "airflow-token")
    monkeypatch.setenv("TRANSFORM_RUNTIME_BOOTSTRAP_OPERATOR_TOKEN", "operator-token")
    with pytest.raises(HTTPException):
        require_bootstrap_operator_identity("Bearer airflow-token")
    assert require_bootstrap_operator_identity("Bearer operator-token") == "protected-bootstrap-operator"


def _successful_raw(ledger):
    run = ledger.create_run("airflow", "r"*16, "snapshot", "contract", "plan")
    batch = EXECUTION_BATCHES["C4_RAW_02"]
    row = ledger.submit_batch(run["transform_run_id"], "airflow", "b"*16, batch,
                              model_fingerprint(batch.model_allowlist))
    for state in ("QUEUED", "RUNNING", "TESTING"):
        ledger.transition(row["batch_execution_id"], state)
    ledger.transition(row["batch_execution_id"], "SUCCEEDED", test_status="PASSED")
    return run


def test_durable_admission_requires_existing_run_and_raw_success():
    ledger = TransformLedger()
    operation = get_protected_bootstrap(INITIAL_TOKEN_LINK_CREATION, 1)
    with pytest.raises(LedgerError):
        ledger.submit_protected_bootstrap(RUN_ID, "protected-bootstrap-operator", "x"*16, operation)
    run = ledger.create_run("airflow", "r"*16, "snapshot", "contract", "plan")
    with pytest.raises(LedgerError):
        ledger.submit_protected_bootstrap(run["transform_run_id"], "protected-bootstrap-operator", "x"*16, operation)


def test_durable_admission_is_idempotent_but_semantic_conflicts_fail():
    ledger = TransformLedger(); run = _successful_raw(ledger)
    operation = get_protected_bootstrap(INITIAL_TOKEN_LINK_CREATION, 1)
    first = ledger.submit_protected_bootstrap(run["transform_run_id"], "protected-bootstrap-operator", "x"*16, operation)
    again = ledger.submit_protected_bootstrap(run["transform_run_id"], "protected-bootstrap-operator", "x"*16, operation)
    assert first["bootstrap_execution_id"] == again["bootstrap_execution_id"]
    assert first["bootstrap_execution_id"].startswith("be_")
    assert first["nessie_branch"] == f"transform_{run['transform_run_id']}"
    assert "DP_TOKEN_KEY" not in first
    assert "DP_TOKEN_KEY_VERSION" not in first


def test_fixed_command_has_no_secret_values_in_argv_or_token_secret_environment(tmp_path):
    command = build_protected_bootstrap_command(INITIAL_TOKEN_LINK_CREATION, 1,
                                                EXECUTION_ID, RUN_ID, source(tmp_path))
    assert command.argv == ("/usr/local/bin/python", "/app/job_runner/materialise_token_link.py")
    assert command.authority == "restricted_identity_transform"
    assert "DP_TOKEN_KEY" not in command.environment
    assert "DP_TOKEN_KEY_VERSION" not in command.environment
    assert command.environment["DP_SECRET_SOURCE"] == "mounted-files"
    assert command.environment["DBT_NESSIE_BRANCH"] == f"transform_{RUN_ID}"
    reconcile = build_protected_bootstrap_command(
        INITIAL_TOKEN_LINK_CREATION, 1, EXECUTION_ID, RUN_ID, source(tmp_path),
        reconciliation_only=True)
    assert reconcile.environment["PROTECTED_BOOTSTRAP_MODE"] == "reconcile"
    assert reconcile.argv == command.argv


def test_materializer_requires_child_side_key_and_version(monkeypatch, tmp_path):
    for name, value in {"DP_TOKEN_KEY": "key", "DP_TOKEN_KEY_VERSION": "v9"}.items():
        (tmp_path / name).write_text(value)
    values = {"TRANSFORM_EXECUTION_ID": EXECUTION_ID,
              "PROTECTED_BOOTSTRAP_OPERATION": INITIAL_TOKEN_LINK_CREATION,
              "PROTECTED_BOOTSTRAP_VERSION": "1", "TRANSFORM_AUTHORITY": "restricted_identity_transform",
              "DP_SECRET_SOURCE": "mounted-files", "DP_SECRET_DIR": str(tmp_path)}
    for key, value in values.items(): monkeypatch.setenv(key, value)
    context = materializer.operation_context()
    assert context.staging_table == "__bootstrap_" + "2"*32
    (tmp_path / "DP_TOKEN_KEY_VERSION").write_text("")
    with pytest.raises(Exception): materializer.operation_context()


@pytest.mark.parametrize("final,staging,expected", [(True,False,"PUBLISHED"),(False,True,"STAGED")])
def test_publication_reconciliation_single_relation_states(monkeypatch, final, staging, expected):
    context = materializer.OperationContext(EXECUTION_ID,"__bootstrap_"+"2"*32,
        "iceberg.silver_vault.__bootstrap_"+"2"*32,"v1")
    monkeypatch.setattr(materializer,"relation_exists",lambda cursor,name: final if name==materializer.TABLE else staging)
    monkeypatch.setattr(materializer,"_provenance_matches",lambda *args: True)
    monkeypatch.setattr(materializer,"validate_table",lambda *args,**kwargs: 1)
    assert materializer.reconcile_publication(object(),context,1) == expected


@pytest.mark.parametrize("final,staging", [(True,True),(False,False)])
def test_publication_reconciliation_ambiguous_states_fail_closed(monkeypatch, final, staging):
    context = materializer.OperationContext(EXECUTION_ID,"__bootstrap_"+"2"*32,
        "iceberg.silver_vault.__bootstrap_"+"2"*32,"v1")
    monkeypatch.setattr(materializer,"relation_exists",lambda cursor,name: final if name==materializer.TABLE else staging)
    with pytest.raises(materializer.PublicationIndeterminate):
        materializer.reconcile_publication(object(),context,1)


def test_materializer_never_drops_final_and_renames_staging():
    code = inspect.getsource(materializer)
    assert f"DROP TABLE IF EXISTS {{context.staging_target}}" in code
    assert "DROP TABLE IF EXISTS {TARGET}" not in code
    assert "RENAME TO {TABLE}" in code


def test_token_link_gate_requires_durable_success_before_connecting():
    command = type("Command",(),{"external_prerequisites":frozenset({TOKEN_LINK}),"environment":{}})()
    with pytest.raises(RuntimeError, match="bootstrap is incomplete"):
        protected_prerequisites.validate_protected_prerequisites(
            command, token_link_bootstrap_succeeded=False)


def test_token_link_gate_requires_current_physical_validity(monkeypatch):
    cursor = types.SimpleNamespace(close=lambda: None)
    connection = types.SimpleNamespace(cursor=lambda: cursor, close=lambda: None)
    trino_module = types.SimpleNamespace(dbapi=types.SimpleNamespace(connect=lambda **kwargs: connection))
    auth_module = types.ModuleType("trino.auth")
    auth_module.BasicAuthentication = lambda *args: object()
    monkeypatch.setitem(sys.modules, "trino", trino_module)
    monkeypatch.setitem(sys.modules, "trino.auth", auth_module)
    monkeypatch.setattr(protected_prerequisites, "relation_exists", lambda *args: True)
    monkeypatch.setattr(protected_prerequisites, "validate_table", lambda *args: 1)
    monkeypatch.setattr(protected_prerequisites, "_show_create",
                        lambda *args: "COMMENT 'protected-bootstrap:be_" + "a"*32 + "'")
    command = types.SimpleNamespace(external_prerequisites=frozenset({TOKEN_LINK}), environment={
        "DBT_TRINO_HOST":"127.0.0.1","DBT_TRINO_PORT":"18443",
        "DBT_TRINO_USER":"dbt","DBT_TRINO_PASSWORD":"password"})
    protected_prerequisites.validate_protected_prerequisites(
        command, token_link_bootstrap_succeeded=True)
    monkeypatch.setattr(protected_prerequisites, "relation_exists", lambda *args: False)
    with pytest.raises(protected_prerequisites.ProtectedPrerequisiteError):
        protected_prerequisites.validate_protected_prerequisites(
            command, token_link_bootstrap_succeeded=True)
