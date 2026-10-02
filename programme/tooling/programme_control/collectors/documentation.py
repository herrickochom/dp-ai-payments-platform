"""Documentation, ADR and evidence-document inventory.

Authoritative documentation stays under ``docs/``. This collector only
reads it to build the Documentation Register, the ADR Register input and
the historical evidence inventory. It never moves, renames or rewrites
an existing document.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .base import (
    KIND_DOCUMENT,
    MISSING,
    PRESENT,
    PRESENT_EMPTY,
    CollectorResult,
    EvidenceItem,
    file_status,
    register,
    utc_now_iso,
)

DOCS_ROOT = "docs"
ADR_ROOT = "docs/adr"

# ADR status vocabulary recognised from ADR front matter.
ADR_STATUSES = ("Proposed", "Accepted", "Superseded", "Rejected", "Deprecated")

_ADR_NUMBER = re.compile(r"(?:ADR[- ]?)(\d{3})", re.IGNORECASE)
_STATUS_LINE = re.compile(
    r"^\s*\*\*(?:Status|Version|Date|Deciders)\*\*:?\s*(.+)$",
    re.MULTILINE | re.IGNORECASE,
)
_TITLE = re.compile(r"^#\s+(.+)$", re.MULTILINE)
_STATUS_VALUE = re.compile(r"(?:Status:?)\**\s*([A-Za-z][A-Za-z \-]*)")


def _parse_front_matter(text: str) -> dict[str, str]:
    head = text[:2000]
    facts: dict[str, str] = {}
    for line in head.splitlines():
        stripped = line.strip().lstrip("*").strip()
        if ":" in stripped:
            key, _, value = stripped.partition(":")
            key = key.strip().lower().rstrip("*").strip()
            value = value.strip().strip("*").strip()
            if key and value and key not in facts:
                facts[key] = value
    return facts


def _extract_status(text: str) -> str:
    match = _STATUS_VALUE.search(text[:3000])
    if not match:
        return ""
    raw = match.group(1).strip()
    for status in ADR_STATUSES:
        if raw.lower().startswith(status.lower()):
            return status.upper()
    return raw.upper()[:24]


def _extract_date(text: str) -> str:
    facts = _parse_front_matter(text)
    for key in ("date", "last updated", "evidence date"):
        value = facts.get(key, "")
        match = re.search(r"(\d{4}-\d{2}-\d{2})", value)
        if match:
            return match.group(1)
    match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", text[:3000])
    return match.group(1) if match else ""


def _version(text: str) -> str:
    facts = _parse_front_matter(text)
    value = facts.get("version", "")
    match = re.search(r"\d+(?:\.\d+)+", value)
    return match.group(0) if match else ""


@register("documentation")
def collect_documentation(root: Path) -> CollectorResult:
    result = CollectorResult(name="documentation")
    observed = utc_now_iso()

    docs_root = root / DOCS_ROOT
    if not docs_root.is_dir():
        result.errors.append(f"{DOCS_ROOT} not found")
        result.facts = {"document_count": 0, "observed_at": observed}
        return result

    documents: list[dict[str, Any]] = []
    adrs: list[dict[str, Any]] = []
    for path in sorted(docs_root.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts:
            continue
        relative = str(path.relative_to(root))
        status, size, digest = file_status(path)
        suffix = path.suffix.lower()
        is_adr = relative.startswith(ADR_ROOT) and suffix == ".md"

        entry: dict[str, Any] = {
            "path": relative,
            "name": path.name,
            "suffix": suffix,
            "status": status,
            "bytes": size,
            "sha256": digest,
            "is_adr": is_adr,
            "top_level": relative.split("/")[1] if "/" in relative else "",
        }

        if suffix == ".md":
            text = path.read_text(encoding="utf-8", errors="replace")
            title_match = _TITLE.search(text)
            entry["title"] = (
                title_match.group(1).strip() if title_match else path.stem
            )
            entry["document_date"] = _extract_date(text)
            entry["heading_count"] = len(re.findall(r"^#{1,6}\s", text, re.MULTILINE))
            entry["word_count"] = len(text.split())

        if is_adr:
            text = path.read_text(encoding="utf-8", errors="replace")
            number_match = _ADR_NUMBER.search(path.name) or _ADR_NUMBER.search(text)
            adrs.append(
                {
                    **entry,
                    "adr_number": (
                        f"ADR-{number_match.group(1)}" if number_match else ""
                    ),
                    "adr_status": _extract_status(text),
                    "adr_version": _version(text),
                    "adr_date": entry.get("document_date", ""),
                }
            )

        documents.append(entry)

    by_area: dict[str, int] = {}
    for entry in documents:
        area = entry["top_level"] or "root"
        by_area[area] = by_area.get(area, 0) + 1

    result.facts = {
        "document_count": len(documents),
        "documents_by_area": by_area,
        "adr_count": len(adrs),
        "adrs": adrs,
        "documents": documents,
        "observed_at": observed,
    }

    for index, entry in enumerate(documents, start=1):
        result.add(
            EvidenceItem(
                evidence_id=f"EV-DOC-{index:03d}",
                kind=KIND_DOCUMENT,
                title=entry.get("title", entry["name"])[:160],
                location=entry["path"],
                status=entry["status"],
                source_path=entry["path"],
                observed_at=observed,
                detail={
                    "sha256": entry["sha256"],
                    "bytes": entry["bytes"],
                    "document_date": entry.get("document_date", ""),
                    "top_level": entry["top_level"],
                },
            )
        )

    for index, entry in enumerate(adrs, start=1):
        result.add(
            EvidenceItem(
                evidence_id=f"EV-ADR-{index:03d}",
                kind=KIND_DOCUMENT,
                title=entry.get("title", entry["name"])[:160],
                location=entry["path"],
                status=entry["status"],
                source_path=entry["path"],
                observed_at=observed,
                detail={
                    "adr_number": entry["adr_number"],
                    "adr_status": entry["adr_status"],
                    "adr_version": entry["adr_version"],
                    "adr_date": entry["adr_date"],
                },
            )
        )
    return result
