"""Fixed command construction for governed protected bootstrap operations."""
from __future__ import annotations

import dataclasses
import re
from typing import Mapping

from orchestration.transform_runtime.protected_bootstrap import (
    INITIAL_TOKEN_LINK_CREATION,
    get_protected_bootstrap,
)
# from orchestration.job_runner.transform_execution import _ambient_environment, build_governed_runtime_command
try:
    from orchestration.job_runner.transform_execution import _ambient_environment, build_governed_runtime_command
except ImportError:
    from transform_execution import _ambient_environment, build_governed_runtime_command


def build_protected_bootstrap_command(
    operation_id: str,
    operation_version: int,
    execution_id: str,
    transform_run_id: str,
    source: Mapping[str, str] | None = None,
    *,
    reconciliation_only: bool = False,
):
    operation = get_protected_bootstrap(operation_id, operation_version)
    if re.fullmatch(r"be_[a-f0-9]{32}", execution_id) is None:
        raise ValueError("invalid protected bootstrap execution identity")
    if re.fullmatch(r"tr_[a-f0-9]{32}", transform_run_id) is None:
        raise ValueError("invalid transform run identity")
    supplied = dict(_ambient_environment() if source is None else source)
    # Reuse the restricted authority, storage and private-Trino validation.
    base = build_governed_runtime_command(
        operation.authority,
        operation.operation_id,
        execution_id,
        supplied,
        transform_run_id=transform_run_id,
    )
    access = supplied.get("RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID", "")
    secret = supplied.get("RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY", "")
    if not access or not secret:
        raise ValueError("restricted bootstrap credentials are unavailable")
    secret_source = supplied.get("DP_SECRET_SOURCE", "").strip()
    secret_dir = supplied.get("DP_SECRET_DIR", "").strip()
    if secret_source != "mounted-files" or not secret_dir.startswith("/"):
        raise ValueError("bootstrap requires the mounted secret provider")
    environment = {
        **dict(base.environment),
        "PROTECTED_BOOTSTRAP_OPERATION": INITIAL_TOKEN_LINK_CREATION,
        "PROTECTED_BOOTSTRAP_VERSION": str(operation.version),
        "TRANSFORM_AUTHORITY": operation.authority,
        "RESTRICTED_TRANSFORM_S3_ACCESS_KEY_ID": access,
        "RESTRICTED_TRANSFORM_S3_SECRET_ACCESS_KEY": secret,
        "DP_SECRET_SOURCE": secret_source,
        "DP_SECRET_DIR": secret_dir,
        "PROTECTED_BOOTSTRAP_MODE": "reconcile" if reconciliation_only else "create",
    }
    if "DP_TOKEN_KEY" in environment or "DP_TOKEN_KEY_VERSION" in environment:
        raise ValueError("token secrets must be resolved inside the bootstrap child")
    return dataclasses.replace(
        base,
        batch_id=operation.operation_id,
        authority=operation.authority,
        argv=(
            supplied.get("PYTHON_EXECUTABLE", "/usr/local/bin/python"),
            "/app/job_runner/materialise_token_link.py",
        ),
        environment=environment,
    )
