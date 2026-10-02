"""Shared collector plumbing."""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

PRESENT = "PRESENT"
PRESENT_EMPTY = "PRESENT_EMPTY"
MISSING = "MISSING"

# Evidence kinds produced by collectors.
KIND_GIT = "git"
KIND_TEST = "test"
KIND_DBT = "dbt"
KIND_TERRAFORM = "terraform"
KIND_DOCUMENT = "document"
KIND_COMPOSE = "compose"
KIND_CONTRACT = "contract"
KIND_INVENTORY = "inventory"
KIND_RELEASE = "release"
KIND_SCHEMA = "schema_registry"


@dataclass
class EvidenceItem:
    """One piece of collected, machine-readable evidence."""

    evidence_id: str
    kind: str
    title: str
    location: str
    status: str
    detail: Mapping[str, Any] = field(default_factory=dict)
    source_path: str = ""
    observed_at: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "evidence_id": self.evidence_id,
            "kind": self.kind,
            "title": self.title,
            "location": self.location,
            "status": self.status,
            "detail": dict(self.detail),
            "source_path": self.source_path,
            "observed_at": self.observed_at,
        }


@dataclass
class CollectorResult:
    """The output of one collector run."""

    name: str
    items: list[EvidenceItem] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    facts: dict[str, Any] = field(default_factory=dict)

    def add(self, item: EvidenceItem) -> None:
        self.items.append(item)

    def as_dict(self) -> dict[str, Any]:
        return {
            "collector": self.name,
            "item_count": len(self.items),
            "errors": list(self.errors),
            "facts": dict(self.facts),
            "items": [item.as_dict() for item in self.items],
        }


def utc_now_iso() -> str:
    """Timestamp used for evidence observation."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def today_iso() -> str:
    return date.today().isoformat()


def file_status(path: Path) -> tuple[str, int, str]:
    """Return (status, size_bytes, sha256) for a repository file.

    EXISTS != PASS: an empty file is reported as PRESENT_EMPTY.
    """
    if not path.is_file():
        return MISSING, 0, ""
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if not payload.strip():
        return PRESENT_EMPTY, 0, digest
    return PRESENT, len(payload), digest


def run_git(root: Path, *args: str) -> tuple[int, str, str]:
    """Run a read-only git command. Never mutating arguments are used."""
    completed = subprocess.run(
        ["git", *args],
        cwd=str(root),
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    return completed.returncode, completed.stdout, completed.stderr


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def sorted_items(items: Iterable[EvidenceItem]) -> list[EvidenceItem]:
    """Deterministic ordering so evidence generation is reproducible."""
    return sorted(items, key=lambda item: item.evidence_id)


CollectorFn = Callable[[Path], CollectorResult]

_COLLECTORS: dict[str, CollectorFn] = {}

#: Collector modules imported for their registration side effects. Import
#: order is fixed so collection is deterministic.
COLLECTOR_MODULES = (
    "git_evidence",
    "test_evidence",
    "dbt_evidence",
    "terraform_evidence",
    "documentation",
    "compose_inventory",
    "contract_inventory",
)


def _load_collectors() -> None:
    """Import every collector module exactly once."""
    if _COLLECTORS:
        return
    from importlib import import_module

    for module_name in COLLECTOR_MODULES:
        import_module(f".{module_name}", __package__)


def register(name: str) -> Callable[[CollectorFn], CollectorFn]:
    def decorator(func: CollectorFn) -> CollectorFn:
        _COLLECTORS[name] = func
        return func

    return decorator


def available_collectors() -> list[str]:
    """Names of every registered collector."""
    _load_collectors()
    return sorted(_COLLECTORS)


def run_collectors(root: Path, only: Iterable[str] | None = None) -> list[CollectorResult]:
    """Run every registered collector, or only the named ones."""
    _load_collectors()
    wanted = set(only) if only else set(_COLLECTORS)
    results: list[CollectorResult] = []
    for name in sorted(wanted):
        func = _COLLECTORS.get(name)
        if func is None:
            raise KeyError(f"Unknown evidence collector: {name}")
        results.append(func(root))
    return results


def collect_all(root: Path, only: Iterable[str] | None = None) -> dict[str, Any]:
    """Run collectors and return a single serialisable evidence document."""
    results = run_collectors(root, only=only)
    return {
        "observed_at": utc_now_iso(),
        "collectors": [result.as_dict() for result in results],
        "totals": {
            "items": sum(len(result.items) for result in results),
            "errors": sum(len(result.errors) for result in results),
        },
    }
