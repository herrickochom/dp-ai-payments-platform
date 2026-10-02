"""Consistent enterprise workbook styling.

One place defines typography, palette, column sizing and conditional
formatting so every sheet looks like part of the same PMO artefact.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# --- Typography -----------------------------------------------------
FONT_FAMILY = "Calibri"
BODY_SIZE = 10
HEADER_SIZE = 10
TITLE_SIZE = 18
SUBTITLE_SIZE = 11

# --- Palette --------------------------------------------------------
NAVY = "1F3864"
MID_BLUE = "2E5C8A"
LIGHT_BLUE = "D9E2F3"
GREY_BAND = "F2F2F2"
BORDER_GREY = "BFBFBF"
WHITE = "FFFFFF"

RAG_GREEN = "C6EFCE"
RAG_AMBER = "FFEB9C"
RAG_RED = "FFC7CE"
RAG_GREY = "E7E6E6"

RAG_GREEN_TEXT = "006100"
RAG_AMBER_TEXT = "9C6500"
RAG_RED_TEXT = "9C0006"
RAG_GREY_TEXT = "595959"

# --- Ownership markers ---------------------------------------------
OWNERSHIP_MACHINE = "M"
OWNERSHIP_HUMAN = "H"
OWNERSHIP_SHARED = "S"
OWNERSHIP_LABELS = {
    OWNERSHIP_MACHINE: "Machine-managed (evidence/calculated)",
    OWNERSHIP_HUMAN: "Human-controlled (approval)",
    OWNERSHIP_SHARED: "Shared (human-seeded, machine-maintained)",
}

# --- Column behaviour ----------------------------------------------
COLUMN_ROLE = {
    "id": ("ID", 14, False),
    "title": ("Title", 52, True),
    "name": ("Name", 42, True),
    "description": ("Description", 60, True),
    "status": ("Status", 20, True),
    "owner": ("Owner", 22, True),
    "rag": ("RAG", 10, False),
    "progress": ("% Complete", 12, False),
    "dates": ("Date", 13, False),
    "count": ("Count", 10, False),
    "path": ("Path / Location", 46, True),
    "short": ("Value", 30, True),
}

RAG_PALETTE = {
    "GREEN": (RAG_GREEN, RAG_GREEN_TEXT),
    "AMBER": (RAG_AMBER, RAG_AMBER_TEXT),
    "RED": (RAG_RED, RAG_RED_TEXT),
    "GREY": (RAG_GREY, RAG_GREY_TEXT),
}


class RagPalette:
    """Named RAG colours for fills and fonts."""

    FILL = dict(RAG_PALETTE)
    FONT = {key: value[1] for key, value in RAG_PALETTE.items()}

    @classmethod
    def fill_for(cls, rag: str) -> str:
        return cls.FILL.get(str(rag).upper(), RAG_GREY)

    @classmethod
    def font_for(cls, rag: str) -> str:
        return cls.FONT.get(str(rag).upper(), RAG_GREY_TEXT)


@dataclass(frozen=True)
class Style:
    """Resolved style for one workbook element."""

    font_name: str = FONT_FAMILY
    body_size: int = BODY_SIZE
    header_fill: str = NAVY
    band_fill: str = GREY_BAND


def header_label(field_name: str) -> str:
    """Human label for a machine field name."""
    special = {
        "id": "ID",
        "rag": "RAG",
        "slo": "SLO",
        "cns": "CNS",
        "dbt": "dbt",
        "ui": "UI",
        "url": "URL",
        "sha": "SHA",
        "sso": "SSO",
    }
    parts = field_name.split("_")
    out: list[str] = []
    for part in parts:
        out.append(special.get(part.lower(), part.capitalize()))
    return " ".join(out)


def column_width(field_name: str) -> int:
    """Sensible default width for a column."""
    name = field_name.lower()
    if name == "id":
        return 14
    if name in {"title", "name", "description", "rationale", "notes", "comments"}:
        return 54 if name in {"description", "rationale", "notes", "comments"} else 44
    if name in {"path", "location", "source_path", "evidence_path", "evidence_url"}:
        return 46
    if name in {"status", "implementation", "verification_status", "technical_status"}:
        return 24
    if name in {"owner", "assigned_team", "decider_role"}:
        return 24
    if name in {
        "planned_start", "planned_finish", "actual_start", "actual_finish",
        "forecast_finish", "planned_date", "actual_date", "forecast_date",
        "decision_date", "raised_date", "due_date", "target_date",
        "author_date", "last_updated", "last_evidence_timestamp",
        "last_refresh_utc", "discovery_date",
    }:
        return 14
    if name.endswith("_pct") or name in {
        "percent_complete", "progress_pct", "allocation_pct",
        "calculated_percent_complete", "retention_days",
    }:
        return 13
    if name.endswith("_ids") or name.endswith("_id") or name in {
        "dependencies", "depends_on", "upstream", "downstream",
        "mapped_control_ids",
    }:
        return 34
    if name in {"rag", "calculated_rag", "manual_rag"}:
        return 10
    if name.startswith("calculated_"):
        return 18
    if name.endswith("_count") or name in {
        "test_count", "tests_passed", "tests_failed", "topic_count",
        "service_count", "model_count",
    }:
        return 12
    if name in {"severity", "type", "category", "level", "direction", "basis"}:
        return 20
    return 24
