"""Evidence collectors.

Collectors read repository evidence only. They never execute platform
workloads, never start containers, never connect to live services and
never read secret material. An artefact that exists but is empty is
reported as ``PRESENT_EMPTY``; existence is never reported as a pass.
"""

from __future__ import annotations

# Importing each module registers its collector with the base registry.
from . import (  # noqa: F401  (imported for registration side effects)
    base as _base,
)
from .base import (
    CollectorResult,
    EvidenceItem,
    available_collectors,
    collect_all,
    run_collectors,
)

__all__ = [
    "CollectorResult",
    "EvidenceItem",
    "available_collectors",
    "collect_all",
    "run_collectors",
]
