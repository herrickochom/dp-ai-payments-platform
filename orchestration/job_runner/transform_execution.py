"""Independently validated transform planning and bounded child lifecycle."""

from __future__ import annotations

import hashlib
import os
import re
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

try:
    from orchestration.job_runner.transform_trino_runtime import transform_trino_runtime
except ImportError:
    from transform_trino_runtime import transform_trino_runtime

from services.shared.security.runtime_security import (
    validate_nessie_security,
    validate_object_store_security,
)
from services.shared.security.secret_provider import resolve_secret

try:
    from orchestration.transform_runtime.execution_plan import (
        EXECUTION_BATCHES,
        PROTECTED_EXTERNAL_PREREQUISITES,
    )
except ImportError:
    from transform_runtime.execution_plan import (
        EXECUTION_BATCHES,
        PROTECTED_EXTERNAL_PREREQUISITES,
    )


REDACTIONS = (
    re.compile(
        r"(?i)(password|secret|token|access_key)(\s*[=:]\s*)\S+"
    ),
)

AUTHORITY_ENV = {
    "ordinary_transform": frozenset(
        {
            "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID",
            "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY",
            "NESSIE_TRANSFORM_TOKEN",
        }
    ),
    "restricted_identity_transform": frozenset(
        {
            "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID",
            "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY",
            "NESSIE_TRANSFORM_TOKEN",
        }
    ),
    "ml_prediction_transform": frozenset(
        {
            "ML_TRANSFORM_S3_ACCESS_KEY_ID",
            "ML_TRANSFORM_S3_SECRET_ACCESS_KEY",
            "NESSIE_TRANSFORM_TOKEN",
        }
    ),
}

AUTHORITY_CREDENTIALS = {
    "ordinary_transform": (
        "ORDINARY_TRANSFORM_S3_ACCESS_KEY_ID",
        "ORDINARY_TRANSFORM_S3_SECRET_ACCESS_KEY",
    ),
    "restricted_identity_transform": (
        "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID",
        "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY",
    ),
    "ml_prediction_transform": (
        "ML_TRANSFORM_S3_ACCESS_KEY_ID",
        "ML_TRANSFORM_S3_SECRET_ACCESS_KEY",
    ),
}

FORBIDDEN_ENV = frozenset(
    {
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    }
)

AUTHORITY_SECRET_NAMES = frozenset().union(*AUTHORITY_ENV.values())

CHILD_SECRET_NAMES = AUTHORITY_SECRET_NAMES | frozenset(
    {
        "DBT_TRINO_PASSWORD",
        "TRINO_KEYSTORE_PASSWORD",
        "TRINO_INTERNAL_SHARED_SECRET",
    }
)

ML_INPUT = Path(
    "/app/data/pdmis_ml/default_risk_current_predictions.jsonl"
)

TRINO_JNA_TMPDIR = "/var/lib/platform-transform-worker/jna"
TRINO_JAVA_TOOL_OPTIONS = f"-Djna.tmpdir={TRINO_JNA_TMPDIR}"


@dataclass(frozen=True)
class TransformCommand:
    batch_id: str
    authority: str
    argv: tuple[str, ...]
    environment: Mapping[str, str]
    trino_environment: Mapping[str, str]
    model_fingerprint: str
    work_path: str


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


def _ambient_environment() -> dict[str, str]:
    """Return ambient configuration with authorised secrets resolved first.

    Only explicitly authorised secret names are resolved through the platform
    secret provider. Secret-provider configuration itself is never copied into
    the bounded dbt child environment.
    """

    environment = dict(os.environ)

    for name in sorted(CHILD_SECRET_NAMES):
        value = resolve_secret(name)
        if value is not None:
            environment[name] = value

    return environment


def build_transform_command(
    batch_id: str,
    execution_id: str,
    source: Mapping[str, str] | None = None,
    *,
    transform_run_id: str | None = None,
) -> TransformCommand:
    batch = EXECUTION_BATCHES.get(batch_id)

    if batch is None:
        raise ValueError("batch is not allowlisted")

    if batch.authority not in AUTHORITY_ENV:
        raise ValueError("batch transform authority is not allowlisted")

    if any(
        model in PROTECTED_EXTERNAL_PREREQUISITES
        for model in batch.model_allowlist
    ):
        raise ValueError("protected prerequisite cannot be selected")

    if not re.fullmatch(r"be_[a-f0-9]{32}", execution_id):
        raise ValueError("invalid execution identity")

    if transform_run_id is not None and not re.fullmatch(
        r"tr_[a-f0-9]{32}",
        transform_run_id,
    ):
        raise ValueError("invalid transform run identity")

    work = (
        Path(
            os.getenv(
                "TRANSFORM_WORK_ROOT",
                "/var/lib/platform-job-runner/work",
            )
        )
        / execution_id
    )

    work_path = str(work)
    dbt_log_path = str(work / "logs")
    dbt_target_path = str(work / "target")

    names = tuple(
        model.rsplit(".", 1)[-1]
        for model in sorted(batch.model_allowlist)
    )

    bounded_ingestion = batch.ownership_unit in {
        "raw_to_bronze", "ml_derived_bronze",
    }
    if bounded_ingestion:
        argv = (
            os.getenv("PYTHON_EXECUTABLE", "/usr/local/bin/python"),
            "/app/job_runner/raw_to_bronze_executor.py",
            *names,
        )
    else:
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

    supplied = dict(source) if source is not None else _ambient_environment()

    required_authority = AUTHORITY_ENV[batch.authority]

    missing_authority = sorted(
        key
        for key in required_authority
        if not supplied.get(key)
    )

    if missing_authority:
        raise ValueError(
            "required transform authority credentials are unavailable"
        )

    object_store_required = (
        "S3_ENDPOINT",
        "S3_USE_SSL",
        "OBJECT_STORE_REGION",
        "OBJECT_STORE_BUCKET",
        "RAW_ROOT",
        "RAW_VERSION",
        "RAW_PREFIX",
        "WAREHOUSE_PREFIX",
        "WAREHOUSE_URI",
        "DBT_S3_URL_STYLE",
    )

    missing_storage = sorted(
        key
        for key in object_store_required
        if not supplied.get(key)
    )

    if missing_storage:
        raise ValueError(
            "required object-store configuration is unavailable: "
            + ", ".join(missing_storage)
        )

    trino_required = (
        "DBT_TRINO_PASSWORD",
    )

    missing_trino = sorted(
        key
        for key in trino_required
        if not supplied.get(key)
    )

    if missing_trino:
        raise ValueError(
            "required dbt Trino credentials are unavailable"
        )

    validate_object_store_security(
        supplied["S3_ENDPOINT"],
        use_ssl=supplied["S3_USE_SSL"],
        ca_bundle=supplied.get("S3_CA_BUNDLE"),
    )

    if supplied["RAW_PREFIX"] != (
        f'{supplied["RAW_ROOT"]}/{supplied["RAW_VERSION"]}'
    ):
        raise ValueError(
            "RAW_PREFIX must equal RAW_ROOT + '/' + RAW_VERSION"
        )

    expected_warehouse_uri = (
        f's3://{supplied["OBJECT_STORE_BUCKET"]}/'
        f'{supplied["WAREHOUSE_PREFIX"]}'
    )

    if supplied["WAREHOUSE_URI"] != expected_warehouse_uri:
        raise ValueError(
            "WAREHOUSE_URI must use the configured bucket and prefix"
        )

    nessie_warehouse = supplied.get("NESSIE_WAREHOUSE", "").strip()

    if not nessie_warehouse:
        raise ValueError("NESSIE_WAREHOUSE is required")

    if nessie_warehouse != supplied["WAREHOUSE_URI"]:
        raise ValueError(
            "NESSIE_WAREHOUSE must equal WAREHOUSE_URI"
        )

    validate_nessie_security(
        supplied.get("NESSIE_ENDPOINT", ""),
        auth_mode=os.getenv("NESSIE_AUTH_MODE", "bearer"),
        token=supplied["NESSIE_TRANSFORM_TOKEN"],
    )

    access_key_name, secret_key_name = AUTHORITY_CREDENTIALS[
        batch.authority
    ]

    passthrough = (
        "S3_ENDPOINT",
        "S3_USE_SSL",
        "S3_CA_BUNDLE",
        "OBJECT_STORE_REGION",
        "S3_PATH_STYLE_ACCESS",
        "DBT_S3_URL_STYLE",
        "DBT_DATABASE",
        "DBT_STAGING_DATABASE",
        "DBT_BRONZE_DATABASE",
        "DBT_SILVER_DATABASE",
        "DBT_SILVER_VAULT_DATABASE",
        "DBT_GOLD_DATABASE",
        "DBT_CONSUMPTION_DATABASE",
        "OBJECT_STORE_BUCKET",
        "RAW_ROOT",
        "RAW_VERSION",
        "RAW_PREFIX",
        "WAREHOUSE_PREFIX",
        "WAREHOUSE_URI",
        "NESSIE_WAREHOUSE",
        "ICEBERG_CATALOG",
        "NESSIE_ENDPOINT",
    )

    environment = {
        key: supplied[key]
        for key in passthrough
        if supplied.get(key)
    }

    effective_transform_run_id = (
        transform_run_id
        if transform_run_id is not None
        else execution_id
    )

    environment.update(
        {
            "PATH": "/opt/dbt/bin:/usr/local/bin:/usr/bin",
            "DBT_LOG_PATH": dbt_log_path,
            "DBT_TARGET_PATH": dbt_target_path,

            "DBT_NESSIE_BRANCH": (
                f"transform_{effective_transform_run_id}"
            ),

            "DBT_TRINO_HOST": supplied.get(
                "DBT_TRINO_HOST",
                "127.0.0.1",
            ),
            "DBT_TRINO_PORT": supplied.get(
                "DBT_TRINO_PORT",
                "18443",
            ),
            "DBT_TRINO_USER": supplied.get(
                "DBT_TRINO_USER",
                "dbt",
            ),
            "DBT_TRINO_PASSWORD": supplied[
                "DBT_TRINO_PASSWORD"
            ],

            "TRANSFORM_AUTHORITY": batch.authority,
            "TRANSFORM_EXECUTION_ID": execution_id,
            "TRANSFORM_EXECUTION_WORK_PATH": work_path,
        }
    )

    if bounded_ingestion:
        environment.update(
            {
                access_key_name: supplied[access_key_name],
                secret_key_name: supplied[secret_key_name],
            }
        )

    trino_environment = {
        "DBT_NESSIE_BRANCH": environment["DBT_NESSIE_BRANCH"],
        "TRINO_S3_ACCESS_KEY_ID": supplied[access_key_name],
        "TRINO_S3_SECRET_ACCESS_KEY": supplied[secret_key_name],
        "NESSIE_WAREHOUSE": nessie_warehouse,
        "S3_ENDPOINT": supplied["S3_ENDPOINT"],
        "S3_PATH_STYLE_ACCESS": supplied.get("S3_PATH_STYLE_ACCESS", "true"),
        "OBJECT_STORE_REGION": supplied["OBJECT_STORE_REGION"],
        "TRINO_KEYSTORE_PASSWORD": supplied.get("TRINO_KEYSTORE_PASSWORD", ""),
        "TRINO_INTERNAL_SHARED_SECRET": supplied.get("TRINO_INTERNAL_SHARED_SECRET", ""),
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "JAVA_HOME": os.environ.get("JAVA_HOME", "/usr/lib/jvm/temurin/jdk-24.0.2+12"),
        "JAVA_TOOL_OPTIONS": TRINO_JAVA_TOOL_OPTIONS,
    }

    if FORBIDDEN_ENV.intersection(environment):
        raise ValueError(
            "root object-store credentials are prohibited"
        )

    fingerprint = hashlib.sha256(
        "\n".join(
            sorted(batch.model_allowlist)
        ).encode()
    ).hexdigest()

    return TransformCommand(
        batch_id,
        batch.authority,
        argv,
        environment,
        trino_environment,
        fingerprint,
        work_path,
    )


def validate_ml_input(path: Path = ML_INPUT) -> str:
    if (
        path != ML_INPUT
        or path.is_symlink()
        or not path.is_file()
    ):
        raise ValueError(
            "governed ML prediction snapshot is unavailable"
        )

    before = path.stat()

    digest = hashlib.sha256(
        path.read_bytes()
    ).hexdigest()

    after = path.stat()

    if (
        before.st_size,
        before.st_mtime_ns,
        before.st_ino,
    ) != (
        after.st_size,
        after.st_mtime_ns,
        after.st_ino,
    ):
        raise ValueError(
            "ML prediction snapshot changed during validation"
        )

    return digest


def execute(
    command: TransformCommand,
    *,
    timeout_seconds: float,
    termination_grace_seconds: float,
    output_cap_bytes: int,
    cancel_requested=lambda: False,
    manage_runtime: bool = False,
) -> ProcessResult:
    if manage_runtime:
        with transform_trino_runtime(
            command.work_path,
            command.trino_environment,
            readiness_user=command.environment["DBT_TRINO_USER"],
            readiness_password=command.environment["DBT_TRINO_PASSWORD"],
            startup_timeout_seconds=float(
                os.getenv("TRANSFORM_TRINO_STARTUP_TIMEOUT_SECONDS", "90")
            ),
        ):
            return execute(command, timeout_seconds=timeout_seconds, termination_grace_seconds=termination_grace_seconds, output_cap_bytes=output_cap_bytes, cancel_requested=cancel_requested)
    process = subprocess.Popen(
        list(command.argv),
        shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=dict(command.environment),
        start_new_session=True,
    )

    deadline = time.monotonic() + timeout_seconds
    failure = None
    retained_output = bytearray()
    total_output_bytes = 0
    output_read_errors: list[Exception] = []

    def drain_output() -> None:
        nonlocal total_output_bytes

        if process.stdout is None:
            return

        try:
            while True:
                chunk = process.stdout.read(64 * 1024)

                if not chunk:
                    break

                total_output_bytes += len(chunk)
                remaining = output_cap_bytes - len(retained_output)

                if remaining > 0:
                    retained_output.extend(chunk[:remaining])
        except Exception as exc:
            output_read_errors.append(exc)

    output_reader = threading.Thread(
        target=drain_output,
        name=f"transform-output-{process.pid}",
    )
    output_reader.start()

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
            process.wait(
                timeout=termination_grace_seconds
            )
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()

    output_reader.join()

    if output_read_errors:
        raise RuntimeError("failed to drain transform child output")

    truncated = total_output_bytes > output_cap_bytes

    redacted_output = _redact(
        bytes(retained_output).decode(
            "utf-8",
            errors="replace",
        )
    )
    redacted_bytes = redacted_output.encode("utf-8")
    if len(redacted_bytes) > output_cap_bytes:
        truncated = True
        redacted_bytes = redacted_bytes[:output_cap_bytes]
    safe_output = redacted_bytes.decode("utf-8", errors="ignore")

    if failure:
        return ProcessResult(
            (
                "CANCELLED"
                if failure == "OPERATOR_CANCELLED"
                else "ORPHANED"
            ),
            process.returncode,
            safe_output,
            truncated,
            failure,
        )

    return ProcessResult(
        (
            "SUCCEEDED"
            if process.returncode == 0
            else "FAILED"
        ),
        process.returncode,
        safe_output,
        truncated,
        (
            None
            if process.returncode == 0
            else "DBT_BUILD_FAILED"
        ),
    )
