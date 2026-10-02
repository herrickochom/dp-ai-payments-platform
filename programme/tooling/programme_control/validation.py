"""Validation of programme registries.

``programme-validate`` runs these rules and fails on any ERROR. WARNINGS
are reported but do not fail the build unless ``--strict`` is used.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable

from . import secrets as secret_scanner
from .loader import load_programme
from .model import Programme, Record, Register

ERROR = "ERROR"
WARNING = "WARNING"
INFO = "INFO"

_ID_PATTERN = re.compile(r"^[A-Z][A-Z0-9]*(-[A-Za-z0-9]+)*-\d{2,4}(-[A-Za-z0-9]+)*$")
_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")


@dataclass(frozen=True)
class Finding:
    """One validation finding."""

    severity: str
    rule: str
    location: str
    message: str

    def __str__(self) -> str:
        return f"[{self.severity}] {self.rule} {self.location}: {self.message}"


@dataclass
class ValidationReport:
    """The outcome of a validation run."""

    findings: list[Finding] = field(default_factory=list)

    def add(self, severity: str, rule: str, location: str, message: str) -> None:
        self.findings.append(Finding(severity, rule, location, message))

    def error(self, rule: str, location: str, message: str) -> None:
        self.add(ERROR, rule, location, message)

    def warning(self, rule: str, location: str, message: str) -> None:
        self.add(WARNING, rule, location, message)

    def info(self, rule: str, location: str, message: str) -> None:
        self.add(INFO, rule, location, message)

    @property
    def errors(self) -> list[Finding]:
        return [item for item in self.findings if item.severity == ERROR]

    @property
    def warnings(self) -> list[Finding]:
        return [item for item in self.findings if item.severity == WARNING]

    @property
    def ok(self) -> bool:
        return not self.errors

    def counts(self) -> dict[str, int]:
        return {
            ERROR: len(self.errors),
            WARNING: len(self.warnings),
            INFO: len([f for f in self.findings if f.severity == INFO]),
        }

    def render(self) -> str:
        lines = [str(item) for item in self.findings]
        counts = self.counts()
        lines.append(
            f"Summary: {counts[ERROR]} error(s), {counts[WARNING]} warning(s), "
            f"{counts[INFO]} info"
        )
        return "\n".join(lines)


def parse_date(value: Any) -> date | None:
    """Parse a date from the accepted programme formats."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, dict)):
        return not value
    return False


def validate_identifier(
    report: ValidationReport, register: Register, record: Record
) -> None:
    spec = register.spec
    location = f"{register.name}/{record.id}"
    if not _ID_PATTERN.match(record.id):
        report.error(
            "invalid_id",
            location,
            f"Identifier {record.id!r} does not match the stable ID pattern "
            f"(expected e.g. {spec.id_prefix}-001)",
        )
    if not record.id.startswith(f"{spec.id_prefix}-"):
        report.error(
            "wrong_id_prefix",
            location,
            f"Identifier must start with {spec.id_prefix}-, got {record.id!r}",
        )


def validate_required_fields(
    report: ValidationReport, register: Register, record: Record
) -> None:
    for required in register.spec.required:
        if required == "id":
            continue
        if _is_blank(record.data.get(required)):
            report.error(
                "missing_required_field",
                f"{register.name}/{record.id}",
                f"Required field {required!r} is missing or empty",
            )


def validate_vocabularies(
    report: ValidationReport, register: Register, record: Record
) -> None:
    for field_name, vocabulary in register.spec.vocabularies.items():
        if not vocabulary:
            continue
        value = record.data.get(field_name)
        if _is_blank(value):
            continue
        text = str(value)
        if text not in vocabulary:
            report.error(
                "invalid_status",
                f"{register.name}/{record.id}",
                f"{field_name}={text!r} is not in the allowed vocabulary "
                f"{list(vocabulary)}",
            )


def validate_dates(
    report: ValidationReport, register: Register, record: Record
) -> None:
    location = f"{register.name}/{record.id}"
    dates: dict[str, date] = {}
    for field_name in register.spec.date_fields:
        if field_name not in record.data:
            continue
        raw = record.data.get(field_name)
        if _is_blank(raw):
            continue
        parsed = parse_date(raw)
        if parsed is None:
            report.error(
                "invalid_date",
                location,
                f"{field_name}={raw!r} is not a valid date "
                f"(expected ISO YYYY-MM-DD or DD/MM/YYYY)",
            )
        else:
            dates[field_name] = parsed

    pairs = (
        ("planned_start", "planned_finish"),
        ("actual_start", "actual_finish"),
        ("planned_start", "actual_start"),
        ("planned_finish", "actual_finish"),
    )
    for start_field, finish_field in pairs:
        start = dates.get(start_field)
        finish = dates.get(finish_field)
        if start and finish and finish < start:
            report.error(
                "finish_before_start",
                location,
                f"{finish_field} ({finish.isoformat()}) is before "
                f"{start_field} ({start.isoformat()})",
            )


def validate_actual_dates_are_evidence_backed(
    programme: Programme, report: ValidationReport, register: Register, record: Record
) -> None:
    """Actual dates must carry evidence; tooling never invents them."""
    spec = register.spec
    actual_fields = [
        name
        for name in spec.date_fields
        if name.startswith("actual_") and not _is_blank(record.data.get(name))
    ]
    if not actual_fields:
        return
    evidence = record.text("evidence_path") or record.text("evidence_url")
    if not evidence:
        report.error(
            "unsubstantiated_actual_date",
            f"{register.name}/{record.id}",
            f"Actual date(s) {actual_fields} recorded without evidence_path "
            "or evidence_url; historical actuals must be evidenced",
        )


def validate_references(
    programme: Programme, report: ValidationReport, register: Register, record: Record
) -> None:
    global_ids = programme.global_ids()
    location = f"{register.name}/{record.id}"
    for field_name, target_register in register.spec.reference_fields.items():
        value = record.data.get(field_name)
        if _is_blank(value):
            continue
        for ref in record.list_of(field_name):
            if ref not in global_ids:
                report.error(
                    "broken_reference",
                    location,
                    f"{field_name}={ref!r} does not resolve to any register record",
                )
            elif global_ids[ref] != target_register:
                report.error(
                    "invalid_reference_target",
                    location,
                    f"{field_name}={ref!r} resolves to register "
                    f"{global_ids[ref]!r} but must resolve to {target_register!r}",
                )
    for field_name, target_register in register.spec.reference_list_fields.items():
        for ref in record.list_of(field_name):
            if ref not in global_ids:
                report.error(
                    "broken_reference",
                    location,
                    f"{field_name}={ref!r} does not resolve to any register record",
                )
            elif global_ids[ref] != target_register:
                report.error(
                    "invalid_reference_target",
                    location,
                    f"{field_name}={ref!r} resolves to register "
                    f"{global_ids[ref]!r} but must resolve to {target_register!r}",
                )


def validate_duplicate_ids(programme: Programme, report: ValidationReport) -> None:
    """IDs must be globally unique: one identity, one register."""
    seen: dict[str, str] = {}
    for name, register in programme.registers.items():
        for record_id in sorted(register.ids()):
            if record_id in seen:
                report.error(
                    "duplicate_id",
                    f"{name}/{record_id}",
                    f"Identifier {record_id!r} is already used by register "
                    f"{seen[record_id]!r}",
                )
            else:
                seen[record_id] = name


def validate_no_human_approval_from_machine(
    programme: Programme, report: ValidationReport, register: Register, record: Record
) -> None:
    """Machine-populated registers may not assert human approval."""
    machine_approval_fields = {
        field_name
        for field_name in record.data
        if field_name.endswith(("_approval", "_approved", "_acceptance", "_sign_off"))
    }
    for field_name in sorted(machine_approval_fields):
        value = record.data.get(field_name)
        if _is_blank(value) or str(value).strip().lower() in {
            "", "pending", "not_approved", "unapproved", "none",
        }:
            continue
        evidence = record.text("evidence_path")
        if not evidence:
            report.error(
                "machine_approval_assertion",
                f"{register.name}/{record.id}",
                f"{field_name}={value!r} asserts an approval but the record "
                "carries no evidence_path; machine evidence must not become "
                "human approval",
            )


def validate_secrets(
    programme: Programme, report: ValidationReport, register: Register
) -> None:
    findings = secret_scanner.scan_records(
        register.name, [record.data for record in register.records]
    )
    for item in findings:
        report.error(
            "secret_leak",
            item.location,
            f"Secret-shaped content detected ({item.detector}): {item.preview}",
        )


def validate_empty_population(
    programme: Programme, report: ValidationReport
) -> None:
    """An empty register is a gap to report, not a validation failure."""
    for name, register in programme.registers.items():
        if not register.records:
            report.info(
                "awaiting_population",
                name,
                "Register is defined but empty: capability is AWAITING "
                "EVIDENCE / POPULATION",
            )


def validate_baseline(
    programme: Programme, report: ValidationReport, expected_baseline: str
) -> None:
    schedule = (programme.config or {}).get("schedule", {}) or {}
    baseline = str(schedule.get("baseline_start", ""))
    if baseline != expected_baseline:
        report.error(
            "baseline_changed",
            "config/programme.yaml",
            f"Programme baseline start is {baseline!r}; the formal baseline is "
            f"{expected_baseline!r}. A rebaseline requires a version, reason, "
            "decision and approval recorded in programme/decisions.yaml",
        )
    parsed = parse_date(baseline)
    if parsed is None:
        report.error(
            "invalid_baseline_date",
            "config/programme.yaml",
            f"baseline_start={baseline!r} is not a valid date",
        )


def validate_traceability_coverage(
    programme: Programme, report: ValidationReport
) -> None:
    """Report broken or missing traceability links without failing."""
    global_ids = programme.global_ids()
    trace_register = programme.get("traceability")
    if trace_register is None:
        return
    for record in trace_register.records:
        chain = [
            "business_requirement_ids",
            "functional_requirement_ids",
            "non_functional_requirement_ids",
            "component_ids",
            "control_ids",
            "test_ids",
            "evidence_ids",
            "acceptance_ids",
        ]
        populated = [name for name in chain if record.list_of(name)]
        if not populated:
            report.warning(
                "empty_traceability_chain",
                f"traceability/{record.id}",
                "Traceability record links no downstream stage; chain is empty",
            )


def validate_programme(
    programme: Programme,
    expected_baseline: str = "2026-08-01",
    check_secrets: bool = True,
) -> ValidationReport:
    """Run every validation rule and return a report."""
    report = ValidationReport()
    validate_baseline(programme, report, expected_baseline)
    validate_duplicate_ids(programme, report)

    for name, register in programme.registers.items():
        for record in register.records:
            validate_identifier(report, register, record)
            validate_required_fields(report, register, record)
            validate_vocabularies(report, register, record)
            validate_dates(report, register, record)
            validate_references(programme, report, register, record)
            validate_actual_dates_are_evidence_backed(
                programme, report, register, record
            )
            validate_no_human_approval_from_machine(
                programme, report, register, record
            )
        if check_secrets:
            validate_secrets(programme, report, register)

    validate_empty_population(programme, report)
    validate_traceability_coverage(programme, report)
    return report


def validate_workbook_file(path: Any, report: ValidationReport) -> ValidationReport:
    """Validate the generated workbook can be opened and is well formed."""
    from pathlib import Path

    workbook_path = Path(path)
    if not workbook_path.is_file():
        report.error(
            "workbook_missing",
            str(workbook_path),
            "Programme workbook not found; run the updater to generate it",
        )
        return report
    try:
        import openpyxl

        workbook = openpyxl.load_workbook(workbook_path, data_only=False)
    except Exception as exc:  # noqa: BLE001 - any failure is corruption
        report.error(
            "workbook_corrupt",
            str(workbook_path),
            f"Workbook could not be opened: {exc}",
        )
        return report

    if not workbook.sheetnames:
        report.error(
            "workbook_corrupt",
            str(workbook_path),
            "Workbook contains no sheets",
        )
    formula_errors = ("#REF!", "#VALUE!", "#DIV/0!", "#NAME?", "#N/A", "#NULL!")
    for sheet in workbook.worksheets:
        if sheet.max_row < 1 or sheet.max_column < 1:
            report.error(
                "workbook_corrupt",
                f"{workbook_path.name}:{sheet.title}",
                "Sheet is empty",
            )
        for row in sheet.iter_rows():
            for cell in row:
                value = cell.value
                if isinstance(value, str) and any(
                    value.startswith(token) for token in formula_errors
                ):
                    report.error(
                        "formula_error",
                        f"{workbook_path.name}:{sheet.title}!{cell.coordinate}",
                        f"Cell contains an Excel error value: {value}",
                    )
    return report
