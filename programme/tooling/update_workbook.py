#!/usr/bin/env python3
"""Update the enterprise programme governance and assurance workbook.

Usage:
    python programme/tooling/update_workbook.py [--check] [--dry-run]

The updater:

1. loads the controlled registries and configuration;
2. runs the read-only evidence collectors;
3. reads the existing workbook, if present, and captures every
   human-controlled value keyed by its stable ID;
4. renders a complete new workbook in a temporary file;
5. validates the rendered workbook and the registries;
6. replaces the workbook atomically only after validation succeeds;
7. writes a change summary.

No record is ever silently deleted: an ID present in the previous
workbook but absent from its register is retained and flagged.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

sys.path.insert(0, str(Path(__file__).resolve().parent))

from programme_control import __version__ as TOOL_VERSION
from programme_control.collectors import collect_all, run_collectors
from programme_control.config import find_repository_root
from programme_control.errors import ProgrammeControlError
from programme_control.loader import load_programme
from programme_control.model import HUMAN
from programme_control.ownership import classify_field
from programme_control.validation import validate_programme, validate_workbook_file, ValidationReport
from programme_control.workbook.sheets import SHEET_PLAN, SheetContext, build_workbook

#: Registers whose records carry machine-calculated schedule columns.
SCHEDULE_REGISTERS = frozenset({"tasks", "phases", "deliverables", "milestones"})


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _human_columns(programme: Any) -> set[str]:
    """Every column name in the workbook that automation may not write."""
    columns: set[str] = set()
    for definition in SHEET_PLAN:
        for column in definition.columns:
            if classify_field(programme, column) == HUMAN:
                columns.add(column)
    return columns


def read_human_values(
    workbook_path: Path, human_columns: set[str]
) -> tuple[dict[str, dict[str, dict[str, Any]]], set[str]]:
    """Capture human-controlled cell values from an existing workbook.

    Returns the captured values keyed by register, then by record ID, plus
    the set of IDs that existed in the previous refresh.
    """
    if not workbook_path.is_file():
        return {}, set()

    import openpyxl

    workbook = openpyxl.load_workbook(workbook_path, data_only=False)
    captured: dict[str, dict[str, dict[str, Any]]] = {}
    seen_ids: set[str] = set()

    for definition in SHEET_PLAN:
        if not definition.register or definition.name not in workbook.sheetnames:
            continue
        sheet = workbook[definition.name]
        header_row = 5
        headers = [
            (sheet.cell(row=header_row, column=index).value or "")
            for index in range(1, sheet.max_column + 1)
        ]
        mapping: dict[str, int] = {}
        for index, label in enumerate(headers, start=1):
            key = str(label).strip()
            for column in definition.columns:
                if key and (
                    key == column.replace("_", " ").title()
                    or key.lower() == column.lower()
                    or key.lower().replace(" ", "_") == column.lower()
                ):
                    mapping[column] = index
                    break
        protected = {
            column: index
            for column, index in mapping.items()
            if column in human_columns
        }
        if not protected:
            continue

        id_index = mapping.get("id") or 1
        for row in range(header_row + 1, sheet.max_row + 1):
            record_id = sheet.cell(row=row, column=id_index).value
            if not record_id:
                continue
            record_id = str(record_id).strip()
            if record_id == "AWAITING POPULATION":
                continue
            seen_ids.add(record_id)
            values: dict[str, Any] = {}
            for column, index in protected.items():
                value = sheet.cell(row=row, column=index).value
                if value not in (None, ""):
                    values[column] = value
            if values:
                captured.setdefault(definition.register, {})[record_id] = values

    return captured, seen_ids


def _diff(
    programme: Any, previous_ids: set[str], refreshed_at: str
) -> list[dict[str, str]]:
    """Build the change summary. Never reports a silent deletion."""
    changes: list[dict[str, str]] = []
    current_ids: set[str] = set()
    for name, register in programme.registers.items():
        for record in register.records:
            current_ids.add(record.id)
        if register.records:
            changes.append(
                {
                    "id": f"CHG-{len(changes) + 1:03d}",
                    "target": name,
                    "type": "REFRESHED",
                    "detail": f"{len(register.records)} record(s) rendered",
                }
            )

    for missing in sorted(previous_ids - current_ids):
        changes.append(
            {
                "id": f"CHG-{len(changes) + 1:03d}",
                "target": missing,
                "type": "RETAINED_NOT_DELETED",
                "detail": (
                    "This ID existed in the previous workbook but not in the "
                    "current register. It was NOT silently deleted; review "
                    "the register entry."
                ),
            }
        )

    changes.append(
        {
            "id": f"CHG-{len(changes) + 1:03d}",
            "target": "workbook",
            "type": "REFRESH",
            "detail": f"Generated {refreshed_at} by tooling v{TOOL_VERSION}",
        }
    )
    return changes


def _write_atomically(workbook: Any, destination: Path) -> Path:
    """Save to a temporary file then replace the destination atomically."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, temp_name = tempfile.mkstemp(
        prefix=f".{destination.stem}.", suffix=".tmp.xlsx", dir=str(destination.parent)
    )
    os.close(handle)
    temp_path = Path(temp_name)
    try:
        workbook.save(temp_path)
        # mkstemp creates the file 0600. The workbook is a shared programme
        # artefact and must remain readable after the atomic move.
        os.chmod(temp_path, 0o644)
        return temp_path
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def build_context(
    programme: Any,
    as_of: date,
    refreshed_at: str,
    evidence: Mapping[str, Any],
    human_values: Mapping[str, Mapping[str, Mapping[str, Any]]],
    changes: list[dict[str, str]],
) -> SheetContext:
    return SheetContext(
        programme=programme,
        as_of=as_of,
        refreshed_at=refreshed_at,
        evidence=evidence,
        human_values=human_values,
        changes=changes,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="update_workbook",
        description="Update the enterprise programme governance workbook.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="validate only; do not write the workbook",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="build and validate in a temporary file without replacing the workbook",
    )
    parser.add_argument(
        "--skip-evidence",
        action="store_true",
        help="reuse the last collected evidence instead of re-collecting",
    )
    parser.add_argument("--as-of", default="", help="override the refresh date (YYYY-MM-DD)")
    arguments = parser.parse_args(argv)

    root = find_repository_root(Path(__file__))
    programme = load_programme(root)
    config = programme.config or {}
    workbook_cfg = config.get("workbook", {}) or {}
    schedule_cfg = config.get("schedule", {}) or {}
    baseline_start = str(schedule_cfg.get("baseline_start", "2026-08-01"))

    refreshed_at = _now_iso()
    as_of = date.today()
    if arguments.as_of:
        from programme_control.validation import parse_date

        parsed = parse_date(arguments.as_of)
        if parsed is None:
            print(f"ERROR: --as-of is not a valid date: {arguments.as_of}", file=sys.stderr)
            return 2
        as_of = parsed

    workbook_path = root / str(
        workbook_cfg.get("path", "programme/output/workbook.xlsx")
    )

    print(f"Programme control updater v{TOOL_VERSION}")
    print(f"  repository      : {root}")
    print(f"  workbook        : {workbook_path}")
    print(f"  baseline start  : {baseline_start}")
    print(f"  refresh date    : {as_of.isoformat()}")

    # 1. Validate the registries before doing any work.
    report = validate_programme(programme, expected_baseline=baseline_start)
    if not report.ok:
        print("\nRegistry validation FAILED. Workbook not written.\n", file=sys.stderr)
        print(report.render(), file=sys.stderr)
        return 1
    counts = report.counts()
    print(
        f"  registries      : {len(programme.registers)} declared, "
        f"{counts['ERROR']} error(s), {counts['WARNING']} warning(s)"
    )

    if arguments.check:
        print("\n--check: no workbook written.")
        return 0

    # 2. Collect evidence.
    evidence_path = root / "programme" / "evidence" / "collected_evidence.json"
    if arguments.skip_evidence and evidence_path.is_file():
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        print("  evidence        : reused cached collection")
    else:
        evidence = collect_all(root)
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_path.write_text(
            json.dumps(evidence, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        print(
            f"  evidence        : {evidence['totals']['items']} item(s), "
            f"{evidence['totals']['errors']} collector error(s)"
        )

    # 3. Preserve human-controlled values from the existing workbook.
    human_columns = _human_columns(programme)
    human_values, previous_ids = read_human_values(workbook_path, human_columns)
    preserved = sum(len(v) for v in human_values.values())
    print(
        f"  human fields    : {preserved} record(s) carry preserved "
        f"human-controlled values across {len(human_columns)} protected column(s)"
    )

    # 4. Render and validate into a temporary file.
    changes = _diff(programme, previous_ids, refreshed_at)
    context = build_context(
        programme, as_of, refreshed_at, evidence, human_values, changes
    )
    workbook = build_workbook(context)

    temp_path = _write_atomically(workbook, workbook_path)
    workbook_report = validate_workbook_file(temp_path, ValidationReport())
    if not workbook_report.ok:
        temp_path.unlink(missing_ok=True)
        print(
            "\nWorkbook integrity validation FAILED. The existing workbook "
            "was NOT modified.\n",
            file=sys.stderr,
        )
        print(workbook_report.render(), file=sys.stderr)
        return 1
    print(f"  workbook check  : {len(workbook.worksheets)} sheets validated")

    # 5. Atomic replacement, skipped on --dry-run.
    if arguments.dry_run:
        temp_path.unlink(missing_ok=True)
        print("\n--dry-run: temporary workbook discarded, nothing replaced.")
        return 0

    shutil.move(str(temp_path), str(workbook_path))
    print(f"  saved           : {workbook_path}")

    # 6. Change summary.
    summary_path = root / "programme" / "reports" / "last_update.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "generated_at": refreshed_at,
        "tool_version": TOOL_VERSION,
        "as_of": as_of.isoformat(),
        "baseline_start": baseline_start,
        "workbook": str(workbook_path.relative_to(root)),
        "sheets": len(workbook.worksheets),
        "registers": {
            name: len(register.records)
            for name, register in programme.registers.items()
        },
        "record_total": sum(len(r.records) for r in programme.registers.values()),
        "human_values_preserved": preserved,
        "protected_columns": sorted(human_columns),
        "evidence_items": evidence.get("totals", {}).get("items", 0),
        "changes": changes,
    }
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str), encoding="utf-8"
    )
    print(f"  change summary  : {summary_path}")
    print("\nWorkbook updated.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProgrammeControlError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
