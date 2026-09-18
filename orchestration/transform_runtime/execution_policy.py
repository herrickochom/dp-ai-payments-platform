from dataclasses import dataclass
from typing import FrozenSet


class ExecutionPolicyError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExecutionUnit:
    name: str
    authority: str
    modes: FrozenSet[str]
    allows_runtime_graph_expansion: bool = False
    allows_plus_selector: bool = False
    allows_tag_selector: bool = False
    allows_arbitrary_dbt_args: bool = False


# Deliberately small initial registry.
#
# These are policy identities, not yet executable dbt workloads.
# Model/test allowlists are added only after lifecycle classification
# and dependency/security-boundary validation.
EXECUTION_UNITS = {
    "raw_to_bronze": ExecutionUnit(
        name="raw_to_bronze",
        authority="ordinary_transform",
        modes=frozenset(
            {"snapshot", "incremental", "cdc_incremental"}
        ),
    ),
    "ordinary_analytics": ExecutionUnit(
        name="ordinary_analytics",
        authority="ordinary_transform",
        modes=frozenset(
            {"snapshot", "incremental", "cdc_incremental"}
        ),
    ),
    "restricted_identity": ExecutionUnit(
        name="restricted_identity",
        authority="restricted_identity_transform",
        modes=frozenset(
            {"snapshot", "incremental", "cdc_incremental"}
        ),
    ),
    "ml_derived_bronze": ExecutionUnit(
        name="ml_derived_bronze",
        authority="ml_prediction_transform",
        modes=frozenset(
            {"snapshot", "incremental"}
        ),
    ),
}


def validate_execution_unit(
    name: str,
    mode: str,
) -> ExecutionUnit:

    unit = EXECUTION_UNITS.get(name)

    if unit is None:
        raise ExecutionPolicyError(
            "execution unit is not allowlisted"
        )

    if mode not in unit.modes:
        raise ExecutionPolicyError(
            "execution mode is not allowed for unit"
        )

    if unit.allows_runtime_graph_expansion:
        raise ExecutionPolicyError(
            "runtime graph expansion must remain disabled"
        )

    if unit.allows_plus_selector:
        raise ExecutionPolicyError(
            "plus selectors must remain disabled"
        )

    if unit.allows_tag_selector:
        raise ExecutionPolicyError(
            "tag selectors must remain disabled"
        )

    if unit.allows_arbitrary_dbt_args:
        raise ExecutionPolicyError(
            "arbitrary dbt arguments must remain disabled"
        )

    return unit
