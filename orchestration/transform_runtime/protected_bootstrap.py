"""Closed, versioned contracts for governed protected bootstrap operations."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping


INITIAL_TOKEN_LINK_CREATION = "initial_token_link_creation"
TOKEN_LINK_FINAL_TARGET = (
    "iceberg.silver_vault.vlt_pdm_beneficiary_token_link"
)


@dataclass(frozen=True)
class ProtectedBootstrapOperation:
    operation_id: str
    version: int
    authority: str
    prerequisite_batch: str
    final_target: str
    initial_create_only: bool
    scheduled: bool
    airflow_invocation_allowed: bool


PROTECTED_BOOTSTRAPS: Mapping[str, ProtectedBootstrapOperation] = MappingProxyType(
    {
        INITIAL_TOKEN_LINK_CREATION: ProtectedBootstrapOperation(
            operation_id=INITIAL_TOKEN_LINK_CREATION,
            version=1,
            authority="restricted_identity_transform",
            prerequisite_batch="C4_RAW_02",
            final_target=TOKEN_LINK_FINAL_TARGET,
            initial_create_only=True,
            scheduled=False,
            airflow_invocation_allowed=False,
        )
    }
)


def get_protected_bootstrap(operation_id: str, version: int) -> ProtectedBootstrapOperation:
    operation = PROTECTED_BOOTSTRAPS.get(operation_id)
    if operation is None or operation.version != version:
        raise ValueError("protected bootstrap operation is not allowlisted")
    if (
        operation.authority != "restricted_identity_transform"
        or not operation.initial_create_only
        or operation.scheduled
        or operation.airflow_invocation_allowed
    ):
        raise ValueError("protected bootstrap contract is unsafe")
    return operation
