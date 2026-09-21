"""Independently validated transform planning and bounded child lifecycle."""
from __future__ import annotations
import hashlib
import os
import re
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

try:
    from orchestration.transform_runtime.execution_plan import EXECUTION_BATCHES, PROTECTED_EXTERNAL_PREREQUISITES
except ImportError:
    from transform_runtime.execution_plan import EXECUTION_BATCHES, PROTECTED_EXTERNAL_PREREQUISITES

REDACTIONS = (re.compile(r"(?i)(password|secret|token|access_key)(\s*[=:]\s*)\S+"),)
AUTHORITY_ENV = {
    "ordinary_transform": frozenset({"ORDINARY_S3_ACCESS_KEY_ID", "ORDINARY_S3_SECRET_ACCESS_KEY", "NESSIE_AUTH_TOKEN"}),
    "restricted_identity_transform": frozenset({"RESTRICTED_S3_ACCESS_KEY_ID", "RESTRICTED_S3_SECRET_ACCESS_KEY", "NESSIE_AUTH_TOKEN"}),
    "ml_prediction_transform": frozenset({"ML_S3_ACCESS_KEY_ID", "ML_S3_SECRET_ACCESS_KEY", "NESSIE_AUTH_TOKEN"}),
}
FORBIDDEN_ENV = frozenset({"MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"})
ML_INPUT = Path("/app/data/pdmis_ml/default_risk_current_predictions.jsonl")

@dataclass(frozen=True)
class TransformCommand:
    batch_id: str
    authority: str
    argv: tuple[str, ...]
    environment: Mapping[str, str]
    model_fingerprint: str
    duckdb_path: str

@dataclass(frozen=True)
class ProcessResult:
    status: str
    return_code: int | None
    output: str
    output_truncated: bool
    failure_class: str | None

def _redact(value: str) -> str:
    for pattern in REDACTIONS:
        value = pattern.sub(r"\1\2[REDACTED]", value)
    return value

def build_transform_command(batch_id: str, execution_id: str, source: Mapping[str, str] | None = None, *, transform_run_id: str | None = None) -> TransformCommand:
    batch = EXECUTION_BATCHES.get(batch_id)
    if batch is None:
        raise ValueError("batch is not allowlisted")
    if any(model in PROTECTED_EXTERNAL_PREREQUISITES for model in batch.model_allowlist):
        raise ValueError("protected prerequisite cannot be selected")
    safe_id = re.fullmatch(r"be_[a-f0-9]{32}", execution_id)
    if not safe_id:
        raise ValueError("invalid execution identity")
    work = Path(os.getenv("TRANSFORM_WORK_ROOT", "/var/lib/platform-job-runner/work")) / execution_id
    duckdb_path = str(work / "runtime.duckdb")
    names = tuple(model.rsplit(".", 1)[-1] for model in sorted(batch.model_allowlist))
    argv = (
        os.getenv("DBT_EXECUTABLE", "/opt/dbt/bin/dbt"),
        "build",
        "--project-dir",
        "/app/dbt",
        "--profiles-dir",
        "/app/dbt",
        "--target",
        "runtime",
        "--select",
        *names,
    )
    supplied = source or os.environ
    required_authority = AUTHORITY_ENV[batch.authority]
    missing = sorted(key for key in required_authority if not supplied.get(key))
    if missing:
        raise ValueError("required transform authority credentials are unavailable")

    access_key_name, secret_key_name = {
        "ordinary_transform": ("ORDINARY_S3_ACCESS_KEY_ID", "ORDINARY_S3_SECRET_ACCESS_KEY"),
        "restricted_identity_transform": ("RESTRICTED_S3_ACCESS_KEY_ID", "RESTRICTED_S3_SECRET_ACCESS_KEY"),
        "ml_prediction_transform": ("ML_S3_ACCESS_KEY_ID", "ML_S3_SECRET_ACCESS_KEY"),
    }[batch.authority]

    passthrough = (
        "S3_ENDPOINT", "DBT_S3_URL_STYLE", "DBT_DATABASE",
        "DBT_STAGING_DATABASE", "DBT_BRONZE_DATABASE", "DBT_SILVER_DATABASE",
        "DBT_SILVER_VAULT_DATABASE", "DBT_GOLD_DATABASE",
        "DBT_CONSUMPTION_DATABASE", "OBJECT_STORE_BUCKET", "WAREHOUSE_PREFIX",
        "WAREHOUSE_URI", "NESSIE_WAREHOUSE", "ICEBERG_CATALOG",
        "NESSIE_ENDPOINT",
    )
    environment = {key: supplied[key] for key in passthrough if supplied.get(key)}
    environment.update({
        "TRANSFORM_S3_ACCESS_KEY_ID": supplied[access_key_name],
        "TRANSFORM_S3_SECRET_ACCESS_KEY": supplied[secret_key_name],
        "NESSIE_AUTH_TOKEN": supplied["NESSIE_AUTH_TOKEN"],
        "PATH": "/opt/dbt/bin:/usr/local/bin:/usr/bin",
        "DBT_DUCKDB_PATH": duckdb_path,
        "DBT_NESSIE_BRANCH": (f"transform_{transform_run_id}" if transform_run_id else f"transform_{execution_id}"),
        "TRANSFORM_AUTHORITY": batch.authority,
    })
    if FORBIDDEN_ENV.intersection(environment):
        raise ValueError("root object-store credentials are prohibited")
    fingerprint = hashlib.sha256("\n".join(sorted(batch.model_allowlist)).encode()).hexdigest()
    return TransformCommand(batch_id, batch.authority, argv, environment, fingerprint, duckdb_path)

def validate_ml_input(path: Path = ML_INPUT) -> str:
    if path != ML_INPUT or path.is_symlink() or not path.is_file():
        raise ValueError("governed ML prediction snapshot is unavailable")
    before = path.stat()
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    after = path.stat()
    if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
        raise ValueError("ML prediction snapshot changed during validation")
    return digest

def execute(command: TransformCommand, *, timeout_seconds: float, termination_grace_seconds: float, output_cap_bytes: int, cancel_requested=lambda: False) -> ProcessResult:
    process = subprocess.Popen(list(command.argv), shell=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=dict(command.environment), start_new_session=True)
    deadline = time.monotonic() + timeout_seconds
    failure = None
    while process.poll() is None:
        if cancel_requested():
            failure = "OPERATOR_CANCELLED"
            break
        if time.monotonic() >= deadline:
            failure = "TIMEOUT_REQUIRES_RECONCILIATION"
            break
        time.sleep(0.05)
    if failure:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=termination_grace_seconds)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
    output = (process.stdout.read() if process.stdout else b"")
    truncated = len(output) > output_cap_bytes
    safe_output = _redact(output[:output_cap_bytes].decode("utf-8", errors="replace"))
    if failure:
        return ProcessResult("CANCELLED" if failure == "OPERATOR_CANCELLED" else "ORPHANED", process.returncode, safe_output, truncated, failure)
    return ProcessResult("SUCCEEDED" if process.returncode == 0 else "FAILED", process.returncode, safe_output, truncated, None if process.returncode == 0 else "DBT_BUILD_FAILED")
