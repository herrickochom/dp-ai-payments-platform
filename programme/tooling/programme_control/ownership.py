"""Field ownership enforcement.

Automation may only write fields classified MACHINE (or SHARED, which
seeds once and then behaves as human-authored). Any attempt by the
automation layer to write a HUMAN field raises
:class:`HumanFieldViolation`.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from .errors import HumanFieldViolation
from .model import HUMAN, MACHINE, SHARED

# Injected by loader.load_programme via setattr.
_OWNERSHIP_ATTR = "ownership"


def get_policy(programme: Any) -> Mapping[str, frozenset[str]]:
    policy = getattr(programme, _OWNERSHIP_ATTR, None)
    if not policy:
        raise HumanFieldViolation("Ownership policy is not attached to the programme")
    return policy


def classify_field(programme: Any, field_name: str) -> str:
    """Return HUMAN, MACHINE or SHARED for a field name."""
    policy = get_policy(programme)
    if field_name in policy["human"]:
        return HUMAN
    if field_name in policy["shared"]:
        return SHARED
    if field_name in policy["machine"]:
        return MACHINE
    unknown = next(iter(policy.get("unknown_policy", frozenset({HUMAN}))))
    return unknown


def is_machine_writable(programme: Any, field_name: str) -> bool:
    """Automation may write only MACHINE fields (never HUMAN/SHARED)."""
    return classify_field(programme, field_name) == MACHINE


def partition_fields(
    programme: Any, field_names: Iterable[str]
) -> dict[str, list[str]]:
    """Split field names into machine-writable and human-protected lists."""
    machine: list[str] = []
    human: list[str] = []
    for name in field_names:
        (machine if is_machine_writable(programme, name) else human).append(name)
    return {"machine": machine, "human": human}


def guard_machine_write(
    programme: Any,
    register: str,
    record_id: str,
    updates: Mapping[str, Any],
) -> None:
    """Raise if ``updates`` contains any human-controlled field.

    This is the hard guard invoked before any machine write is applied,
    whether to a register file or to a workbook row.
    """
    violations = sorted(
        name
        for name in updates
        if not is_machine_writable(programme, name)
    )
    if violations:
        raise HumanFieldViolation(
            f"Automation may not modify human-controlled field(s) "
            f"{', '.join(violations)} on {register}/{record_id}. "
            "Human approval fields are protected by the ownership policy."
        )
