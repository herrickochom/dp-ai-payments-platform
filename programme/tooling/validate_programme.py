#!/usr/bin/env python3
"""Validate the programme registries, evidence and workbook.

Usage:
    python programme/tooling/validate_programme.py [--strict] [--json]

Exit codes:
    0  validation passed
    1  validation failed (one or more ERROR findings)
    2  the command could not run

This command never writes to the workbook. It is safe to run in CI.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from programme_control import __version__ as TOOL_VERSION
from programme_control.config import find_repository_root
from programme_control.errors import ProgrammeControlError
from programme_control.loader import load_programme
from programme_control.validation import (
    ERROR,
    WARNING,
    ValidationReport,
    validate_programme,
    validate_workbook_file,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="programme-validate",
        description="Validate the enterprise programme control system.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="treat warnings as failures",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit findings as JSON instead of text",
    )
    parser.add_argument(
        "--skip-workbook",
        action="store_true",
        help="validate registries only, not the generated workbook",
    )
    parser.add_argument(
        "--no-secret-scan",
        action="store_true",
        help="skip the secret-leak scan (not recommended)",
    )
    arguments = parser.parse_args(argv)

    root = find_repository_root(Path(__file__))
    programme = load_programme(root)
    config = programme.config or {}
    schedule_cfg = config.get("schedule", {}) or {}
    baseline = str(schedule_cfg.get("baseline_start", "2026-08-01"))

    report: ValidationReport = validate_programme(
        programme,
        expected_baseline=baseline,
        check_secrets=not arguments.no_secret_scan,
    )

    if not arguments.skip_workbook:
        workbook_cfg = config.get("workbook", {}) or {}
        workbook_path = root / str(workbook_cfg.get("path", ""))
        if workbook_path.name:
            validate_workbook_file(workbook_path, report)

    counts = report.counts()

    if arguments.json:
        payload = {
            "tool_version": TOOL_VERSION,
            "baseline_start": baseline,
            "ok": report.ok and not (arguments.strict and report.warnings),
            "counts": counts,
            "findings": [
                {
                    "severity": finding.severity,
                    "rule": finding.rule,
                    "location": finding.location,
                    "message": finding.message,
                }
                for finding in report.findings
                if finding.severity != "INFO" or counts[ERROR] or counts[WARNING]
            ],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"programme-validate v{TOOL_VERSION}")
        print(f"  repository      : {root}")
        print(f"  baseline start  : {baseline}")
        print(f"  registers       : {len(programme.registers)}")
        print(
            f"  records         : "
            f"{sum(len(r.records) for r in programme.registers.values())}"
        )
        print()
        for finding in report.findings:
            if finding.severity == ERROR or (arguments.strict and finding.severity == WARNING):
                print(f"  {finding}")
        print()
        print(f"  {counts[ERROR]} error(s), {counts[WARNING]} warning(s)")
        if report.ok:
            print("  RESULT: PASS")
        else:
            print("  RESULT: FAIL")
        if arguments.strict and report.warnings:
            print("  RESULT (strict): FAIL - warnings present")

    failed = not report.ok or (arguments.strict and bool(report.warnings))
    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProgrammeControlError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
