"""Sheet construction for the programme workbook.

Sheets are declarative. Each register maps to one sheet with a stable
column set; dashboards and the Gantt are built from the same record set
so the workbook can never disagree with the registries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Iterable, Mapping, Sequence

from ..model import Programme, Record
from ..calculations import (
    calculate_task_fields,
    effective_rag,
    rag_status,
    rag_sort_key,
    rollup,
)
from ..validation import parse_date
from .styling import (
    BODY_SIZE,
    BORDER_GREY,
    FONT_FAMILY,
    GREY_BAND,
    HEADER_SIZE,
    LIGHT_BLUE,
    MID_BLUE,
    NAVY,
    RAG_GREY,
    RAG_PALETTE,
    SUBTITLE_SIZE,
    TITLE_SIZE,
    WHITE,
    column_width,
    header_label,
)

#: Columns promoted to the front of every register sheet.
ID = "id"


@dataclass(frozen=True)
class SheetDefinition:
    """Declarative description of one register sheet."""

    name: str
    register: str
    columns: tuple[str, ...]
    group: str = ""
    description: str = ""
    sort_by: Callable[[Mapping[str, Any]], Any] | None = None


@dataclass
class SheetContext:
    """Everything a sheet builder needs to render."""

    programme: Programme
    as_of: date
    refreshed_at: str
    evidence: Mapping[str, Any] = field(default_factory=dict)
    preservation: Mapping[str, Mapping[str, Mapping[str, Any]]] = field(
        default_factory=dict
    )
    changes: list[str] = field(default_factory=list)
    human_values: Mapping[str, Mapping[str, Mapping[str, Any]]] = field(
        default_factory=dict
    )

    def human_value(self, register: str, record_id: str, column: str) -> Any:
        return (
            self.human_values.get(register, {})
            .get(record_id, {})
            .get(column)
        )


def _sort_by_id(record: Mapping[str, Any]) -> Any:
    return str(record.get("id", ""))


def _sort_by_severity(record: Mapping[str, Any]) -> Any:
    order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    return (
        order.get(str(record.get("severity", "")).upper(), 4),
        str(record.get("id", "")),
    )


def _sort_by_status(record: Mapping[str, Any]) -> Any:
    return (str(record.get("status", "")), str(record.get("id", "")))


# Columns shown per register. Order is the column order in the sheet.
TASK_COLUMNS = (
    "id", "title", "phase_id", "owner", "status",
    "planned_start", "planned_finish", "actual_start", "actual_finish",
    "calculated_percent_complete", "calculated_schedule_variance_days",
    "calculated_rag", "calculated_days_to_finish",
    "evidence_path", "requirement_ids", "component_ids", "control_ids",
    "test_ids", "dependencies", "notes",
)

#: The full sheet plan. Order here is the tab order in the workbook.
SHEET_PLAN: tuple[SheetDefinition, ...] = (
    SheetDefinition("Index", "", (), "Management", "Workbook navigation and baseline"),
    SheetDefinition("Executive Dashboard", "", (), "Management", "Executive summary"),
    SheetDefinition("Programme Dashboard", "", (), "Management", "Delivery control"),
    SheetDefinition("Master Tracker", "tasks", TASK_COLUMNS, "Delivery", "All WBS tasks"),
    SheetDefinition("Gantt", "tasks", (), "Delivery", "Planned timeline"),
    SheetDefinition("Milestones", "milestones", (
        "id", "name", "phase_id", "criticality", "status",
        "planned_date", "forecast_date", "actual_date",
        "calculated_rag", "owner", "evidence_path", "notes",
    ), "Delivery", "Programme milestones", _sort_by_status),
    SheetDefinition("Phases", "phases", (
        "id", "name", "status", "planned_start", "planned_finish",
        "actual_start", "actual_finish", "owner",
        "calculated_percent_complete", "calculated_rag",
        "gate_criteria", "evidence_path", "notes",
    ), "Delivery", "Phases and gates"),
    SheetDefinition("Deliverables", "deliverables", (
        "id", "name", "phase_id", "owner", "status",
        "planned_start", "planned_finish",
        "calculated_percent_complete", "calculated_rag",
        "task_ids", "evidence_path", "acceptance_state", "notes",
    ), "Delivery", "Deliverables"),
    SheetDefinition("Dependencies", "dependencies", (
        "id", "name", "depends_on", "type", "owner", "status",
        "predecessor_id", "successor_id", "target_date", "notes",
    ), "Delivery", "Dependencies"),
    SheetDefinition("RAID", "raid", (
        "id", "type", "title", "severity", "status", "owner",
        "mitigation_task_ids", "control_ids", "probability", "impact",
        "raised_date", "due_date", "risk_acceptance",
        "residual_risk_acceptance", "evidence_path", "notes",
    ), "Governance", "Risks, issues, assumptions, dependencies", _sort_by_severity),
    SheetDefinition("Decision Log", "decisions", (
        "id", "title", "decision_date", "status", "decider_role",
        "decision_rationale", "affected_scope", "evidence_path", "notes",
    ), "Governance", "Programme and technical decisions"),
    SheetDefinition("Change Requests", "change_requests", (
        "id", "title", "raised_date", "status", "requester",
        "description", "impact", "decision_date", "evidence_path", "notes",
    ), "Governance", "Change control"),
    SheetDefinition("RACI", "raci", (
        "id", "activity", "responsible", "accountable", "consulted", "informed",
    ), "Governance", "Role assignments"),
    SheetDefinition("Resources", "resources", (
        "id", "name", "role", "allocation_pct", "start_date", "end_date", "notes",
    ), "Governance", "Resources"),
    SheetDefinition("Stakeholders", "stakeholders", (
        "id", "name", "role", "engagement", "organisation", "notes",
    ), "Governance", "Stakeholders"),
    SheetDefinition("Communications", "communications", (
        "id", "channel", "audience", "cadence", "owner", "notes",
    ), "Governance", "Communications"),
    SheetDefinition("Budget", "budget", (
        "id", "category", "basis", "currency", "approved_amount",
        "forecast_amount", "budget_approved", "budget_approver", "notes",
    ), "Governance", "Budget and forecast"),
    SheetDefinition("Benefits", "benefits", (
        "id", "name", "owner", "measurement", "status",
        "benefit_acceptance", "benefit_accepted_by", "benefit_acceptance_date", "notes",
    ), "Governance", "Benefits realisation"),
    SheetDefinition("Acceptance", "acceptance", (
        "id", "subject", "decision", "decision_date", "conditions",
        "approver_role", "evidence_path", "notes",
    ), "Governance", "Acceptance and sign-off"),
    SheetDefinition("Business Requirements", "business_requirements", (
        "id", "title", "status", "description", "owner",
        "acceptance_criteria_ids", "evidence_path", "notes",
    ), "Requirements", "Business requirements"),
    SheetDefinition("Functional Requirements", "functional_requirements", (
        "id", "title", "status", "business_requirement_ids",
        "description", "component_ids", "control_ids", "test_ids",
        "evidence_path", "notes",
    ), "Requirements", "Functional requirements"),
    SheetDefinition("Non-Functional Requirements", "non_functional_requirements", (
        "id", "title", "category", "status", "business_requirement_ids",
        "target_metric", "control_ids", "slo_ids", "verified_by",
        "evidence_path", "notes",
    ), "Requirements", "Non-functional requirements"),
    SheetDefinition("Acceptance Criteria", "acceptance_criteria", (
        "id", "requirement_id", "criterion", "evidence_required",
        "verification_method", "status", "evidence_path", "notes",
    ), "Requirements", "Acceptance criteria"),
    SheetDefinition("Traceability", "traceability", (
        "id", "business_requirement_id", "functional_requirement_ids",
        "non_functional_requirement_ids", "component_ids", "adr_ids",
        "data_product_ids", "control_ids", "test_ids", "evidence_ids",
        "release_ids", "slo_ids", "runbook_ids", "acceptance_ids",
        "notes",
    ), "Requirements", "End-to-end traceability"),
    SheetDefinition("ADR Register", "adrs", (
        "id", "title", "status", "decision_date", "version",
        "location", "evidence_path", "decision_rationale", "notes",
    ), "Architecture", "Architecture decision records"),
    SheetDefinition("Documentation", "documentation", (
        "id", "title", "location", "top_level", "document_date",
        "status", "bytes", "sha256", "owner", "notes",
    ), "Architecture", "Documentation inventory"),
    SheetDefinition("Architecture Components", "components", (
        "id", "name", "category", "status", "technology_ids",
        "requirement_ids", "control_ids", "source_path", "notes",
    ), "Architecture", "Components"),
    SheetDefinition("Technologies", "technologies", (
        "id", "name", "category", "status", "version", "source_path", "notes",
    ), "Architecture", "Technology register"),
    SheetDefinition("Integrations", "integrations", (
        "id", "name", "source_component", "target_component", "direction",
        "status", "protocol", "source_path", "notes",
    ), "Architecture", "Interfaces and integrations"),
    SheetDefinition("APIs", "apis", (
        "id", "path", "method", "owner_component", "status",
        "authentication", "source_path", "notes",
    ), "Architecture", "API register"),
    SheetDefinition("Environments", "environments", (
        "id", "name", "classification", "status", "source_path",
        "notes",
    ), "Architecture", "Environment register"),
    SheetDefinition("Configuration", "configuration", (
        "id", "name", "location", "classification", "status",
        "owner", "notes",
    ), "Architecture", "Configuration register (metadata only)"),
    SheetDefinition("Data Assets", "data_assets", (
        "id", "name", "layer", "status", "product_ids", "contract_ids",
        "control_ids", "source_path", "evidence_path", "notes",
    ), "Data", "Medallion data assets"),
    SheetDefinition("Data Products", "data_products", (
        "id", "name", "audience", "status", "asset_ids", "contract_ids",
        "classification", "source_path", "evidence_path", "notes",
    ), "Data", "Data product catalogue"),
    SheetDefinition("Data Contracts", "data_contracts", (
        "id", "name", "kind", "location", "compatibility", "status",
        "contract_version", "sha256", "notes",
    ), "Data", "Schema and data contracts"),
    SheetDefinition("Data Quality", "data_quality", (
        "id", "name", "type", "dimension", "status", "asset_id",
        "rule_logic", "evidence_path", "notes",
    ), "Data", "Data quality controls"),
    SheetDefinition("Classification", "data_classification", (
        "id", "name", "level", "control", "retention_days",
        "masking_required", "evidence_path", "notes",
    ), "Data", "Classification levels"),
    SheetDefinition("Lineage", "lineage", (
        "id", "name", "upstream", "downstream", "evidence_path", "notes",
    ), "Data", "Dataset lineage"),
    SheetDefinition("Retention", "retention", (
        "id", "name", "asset_class", "retention_days",
        "enforcement_status", "evidence_path", "notes",
    ), "Data", "Retention and lifecycle"),
    SheetDefinition("AI Agents", "agents", (
        "id", "name", "capability", "status", "owner_component",
        "source_path", "test_ids", "evidence_path", "notes",
    ), "AI", "Agent register"),
    SheetDefinition("AI Models", "models", (
        "id", "name", "task", "status", "version", "metrics",
        "source_path", "evidence_path", "notes",
    ), "AI", "Model register"),
    SheetDefinition("AI RAG", "rag", (
        "id", "name", "status", "backend", "source_path", "notes",
    ), "AI", "RAG and retrieval"),
    SheetDefinition("AI Evaluations", "ai_evaluations", (
        "id", "subject", "metric", "outcome", "observed_on",
        "evidence_path", "notes",
    ), "AI", "AI evaluations"),
    SheetDefinition("AI Provenance", "ai_provenance", (
        "id", "subject", "method", "status", "evidence_path", "notes",
    ), "AI", "AI provenance"),
    SheetDefinition("AI Authority", "ai_authority", (
        "id", "name", "control_type", "enforcement", "source_path",
        "evidence_path", "notes",
    ), "AI", "Delegated authority controls"),
    SheetDefinition("AI Risks", "ai_risks", (
        "id", "title", "category", "severity", "status", "owner",
        "control_ids", "notes",
    ), "AI", "AI risks", _sort_by_severity),
    SheetDefinition("Security Controls", "security_controls", (
        "id", "title", "control_type", "implementation",
        "verification_status", "component_ids", "threat_ids", "test_ids",
        "source_path", "evidence_path", "notes",
    ), "Security", "Security control register"),
    SheetDefinition("Threat Register", "threats", (
        "id", "title", "category", "severity", "status", "control_ids",
        "evidence_path", "notes",
    ), "Security", "Threat register", _sort_by_severity),
    SheetDefinition("Vulnerabilities", "vulnerabilities", (
        "id", "title", "severity", "status", "source", "remediation",
        "target_date", "evidence_path", "notes",
    ), "Security", "Vulnerability register", _sort_by_severity),
    SheetDefinition("Secrets Metadata", "secrets_metadata", (
        "id", "name", "owner", "provider", "status", "rotation_due",
        "last_rotated", "evidence_path", "notes",
    ), "Security", "Secret metadata (never values)"),
    SheetDefinition("Certificates", "certificates", (
        "id", "name", "subject", "purpose", "status", "valid_from",
        "expires_on", "notes",
    ), "Security", "Certificate metadata"),
    SheetDefinition("Access Reviews", "access_reviews", (
        "id", "scope", "review_type", "status", "reviewer_role",
        "last_reviewed", "next_due", "notes",
    ), "Security", "Access reviews"),
    SheetDefinition("Compliance", "compliance", (
        "id", "framework", "control_reference", "mapped_control_ids",
        "assessment_status", "evidence_path", "notes",
    ), "Security", "Compliance mapping"),
    SheetDefinition("Test Register", "tests", (
        "id", "name", "level", "scope", "status", "technical_status",
        "test_result", "test_count", "last_test_run", "requirement_ids",
        "source_path", "evidence_path", "notes",
    ), "Delivery", "Test register"),
    SheetDefinition("Defects", "defects", (
        "id", "title", "severity", "status", "raised_date", "owner",
        "component_id", "evidence_path", "notes",
    ), "Delivery", "Defects", _sort_by_severity),
    SheetDefinition("Evidence Register", "evidence", (
        "id", "title", "evidence_type", "location", "status", "sha256",
        "bytes", "observed_at", "notes",
    ), "Delivery", "Evidence"),
    SheetDefinition("Releases", "releases", (
        "id", "name", "version", "status", "released_on", "git_sha",
        "component_ids", "evidence_path", "notes",
    ), "Delivery", "Releases"),
    SheetDefinition("Deployments", "deployments", (
        "id", "environment_id", "component_id", "status", "deployed_on",
        "git_sha", "evidence_path", "notes",
    ), "Delivery", "Deployments"),
    SheetDefinition("CI and CD Controls", "cicd_controls", (
        "id", "name", "pipeline", "control_type", "status",
        "source_path", "evidence_path", "notes",
    ), "Delivery", "CI/CD controls"),
    SheetDefinition("Dependency Inventory", "dependencies_sbom", (
        "id", "name", "scope", "version_constraint", "status",
        "source_path", "notes",
    ), "Delivery", "Declared dependencies and SBOM inputs"),
    SheetDefinition("Git Evidence", "git_evidence", (
        "id", "sha", "summary", "author_date", "author", "evidence_path",
    ), "Delivery", "Commit evidence"),
    SheetDefinition("SLA and SLO", "slos", (
        "id", "name", "sli_definition", "target", "status",
        "component_ids", "evidence_path", "notes",
    ), "Operations", "SLA/SLO/SLI"),
    SheetDefinition("Capacity", "capacity", (
        "id", "subject", "metric", "design_target", "measured_value",
        "status", "evidence_path", "notes",
    ), "Operations", "Capacity and performance"),
    SheetDefinition("Monitoring", "monitoring", (
        "id", "name", "signal_type", "status", "source_path",
        "evidence_path", "notes",
    ), "Operations", "Monitoring and alerting"),
    SheetDefinition("Incidents", "incidents", (
        "id", "title", "severity", "status", "raised_date", "resolved_on",
        "impact", "evidence_path", "notes",
    ), "Operations", "Incidents", _sort_by_severity),
    SheetDefinition("Problems and RCA", "problems", (
        "id", "title", "status", "root_cause", "corrective_action",
        "evidence_path", "notes",
    ), "Operations", "Problem and RCA"),
    SheetDefinition("Runbooks", "runbooks", (
        "id", "name", "scenario", "location", "owner", "last_tested",
        "notes",
    ), "Operations", "Runbooks"),
    SheetDefinition("Backup and Restore", "backup_restore", (
        "id", "name", "scope", "status", "last_tested", "rto", "rpo",
        "evidence_path", "notes",
    ), "Operations", "Backup and restore"),
    SheetDefinition("Disaster Recovery", "dr", (
        "id", "name", "rto", "rpo", "status", "last_tested",
        "evidence_path", "notes",
    ), "Operations", "Disaster recovery"),
    SheetDefinition("Training", "training", (
        "id", "topic", "audience", "status", "delivered_on", "owner",
        "notes",
    ), "Operations", "Training and KT"),
    SheetDefinition("Change Log", "change_log", (
        "id", "sheet", "change_type", "record_id", "detail",
    ), "Management", "What this refresh changed"),
    SheetDefinition("Data Dictionary", "data_dictionary", (
        "id", "sheet", "column", "definition", "ownership",
    ), "Management", "Field definitions and ownership"),
)


# --------------------------------------------------------------------------
# Rendering helpers
# --------------------------------------------------------------------------
def _openpyxl():
    try:
        import openpyxl  # noqa: F401
    except ImportError as exc:  # pragma: no cover - dependency is declared
        raise RuntimeError(
            "openpyxl is required to build the programme workbook. "
            "Install it with: pip install -r requirements.txt"
        ) from exc
    return openpyxl


def _value_for(
    context: SheetContext, definition: SheetDefinition, record: Mapping[str, Any], column: str
) -> Any:
    """Resolve one cell value, preserving human-controlled entries."""
    register = definition.register
    record_id = str(record.get("id", ""))
    raw = record.get(column)

    if column.startswith("calculated_"):
        return calculate_task_fields(record, context.as_of).get(column, raw)
    if column in {"rag", "calculated_rag"} and column != "calculated_rag":
        return effective_rag(record, context.as_of)

    # Human-controlled cells keep whatever the workbook already holds.
    human = context.human_value(register, record_id, column)
    if human not in (None, ""):
        return human
    return raw


def _is_percentage(column: str) -> bool:
    return column.endswith("_pct") or column in {
        "percent_complete",
        "calculated_percent_complete",
    }


def _is_date_column(column: str) -> bool:
    return column.endswith("_date") or column.endswith("_on") or column in {
        "planned_start",
        "planned_finish",
        "actual_start",
        "actual_finish",
        "forecast_finish",
        "planned_date",
        "actual_date",
        "forecast_date",
        "decision_date",
        "last_test_run",
        "last_rotated",
        "rotation_due",
        "last_reviewed",
        "next_due",
        "last_tested",
        "observed_at",
        "last_evidence_timestamp",
        "last_refresh_utc",
        "delivered_on",
        "deployed_on",
        "released_on",
        "resolved_on",
        "discovered_at",
    }


def _is_count_column(column: str) -> bool:
    return column.endswith("_count") or column in {"bytes", "allocation_pct", "retention_days"}


def _cell_value(raw: Any, column: str) -> Any:
    """Coerce a registry value into a typed Excel cell value."""
    if raw is None:
        return ""
    if isinstance(raw, bool):
        return "Yes" if raw else "No"
    if isinstance(raw, (list, tuple)):
        return ", ".join(str(item) for item in raw)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return raw
    text = str(raw)
    if _is_date_column(column) and text:
        parsed = parse_date(text)
        if parsed is not None and len(text) <= 10:
            return parsed
    if _is_count_column(column):
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return text
    return text


def _apply_header(sheet, row: int, labels: Sequence[str], widths: Sequence[int]) -> None:
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

    fill = PatternFill("solid", fgColor=NAVY)
    font = Font(name=FONT_FAMILY, size=HEADER_SIZE, bold=True, color=WHITE)
    border = Border(bottom=Side(style="thin", color=BORDER_GREY))
    alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    for index, label in enumerate(labels, start=1):
        cell = sheet.cell(row=row, column=index, value=label)
        cell.fill = fill
        cell.font = font
        cell.alignment = alignment
        cell.border = border
    for index, width in enumerate(widths, start=1):
        letter = sheet.cell(row=row, column=index).column_letter
        sheet.column_dimensions[letter].width = width
    sheet.row_dimensions[row].height = 30


def _apply_body(sheet, row: int, values: Sequence[Any], columns: Sequence[str], band: bool) -> None:
    from openpyxl.styles import Alignment, Font, PatternFill

    font = Font(name=FONT_FAMILY, size=BODY_SIZE)
    fill = PatternFill("solid", fgColor=GREY_BAND) if band else None
    for index, (value, column) in enumerate(zip(values, columns), start=1):
        cell = sheet.cell(row=row, column=index, value=value)
        cell.font = font
        if fill is not None:
            cell.fill = fill
        if _is_date_column(column):
            cell.number_format = "DD/MM/YYYY"
            cell.alignment = Alignment(horizontal="center")
        elif _is_percentage(column):
            cell.number_format = "0.0"
            cell.alignment = Alignment(horizontal="center")
        elif _is_count_column(column):
            cell.number_format = "#,##0"
            cell.alignment = Alignment(horizontal="right")
        else:
            cell.alignment = Alignment(vertical="top", wrap_text=True)
        text = str(value)
        if text in RAG_PALETTE:
            from openpyxl.styles import PatternFill as _PF

            fill_colour, font_colour = RAG_PALETTE[text]
            cell.fill = _PF("solid", fgColor=fill_colour)
            cell.font = Font(name=FONT_FAMILY, size=BODY_SIZE, bold=True, color=font_colour)
            cell.alignment = Alignment(horizontal="center", vertical="center")
        if text == "RED" or text == "CRITICAL":
            cell.font = Font(name=FONT_FAMILY, size=BODY_SIZE, bold=True, color=RAG_PALETTE["RED"][1])


def _add_title_block(sheet, title: str, subtitle: str, context: SheetContext, span: int) -> None:
    from openpyxl.styles import Alignment, Font, PatternFill

    config = context.programme.config or {}
    workbook_cfg = config.get("workbook", {}) or {}
    schedule = config.get("schedule", {}) or {}

    title_cell = sheet.cell(row=1, column=1, value=title)
    title_cell.font = Font(name=FONT_FAMILY, size=TITLE_SIZE, bold=True, color=NAVY)
    sheet.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max(span, 1))
    sheet.row_dimensions[1].height = 26

    meta = (
        f"{workbook_cfg.get('organisation', '')} | "
        f"Baseline start {schedule.get('baseline_start', '')} | "
        f"Baseline v{schedule.get('baseline_version', '')} | "
        f"Refreshed {context.refreshed_at} | Workbook v{workbook_cfg.get('version', '')}"
    )
    meta_cell = sheet.cell(row=2, column=1, value=meta)
    meta_cell.font = Font(name=FONT_FAMILY, size=SUBTITLE_SIZE, color=MID_BLUE)
    sheet.merge_cells(start_row=2, start_column=1, end_row=2, end_column=max(span, 1))

    note_cell = sheet.cell(row=3, column=1, value=subtitle)
    note_cell.font = Font(name=FONT_FAMILY, size=BODY_SIZE, italic=True, color="595959")
    note_cell.alignment = Alignment(wrap_text=True, vertical="top")
    sheet.merge_cells(start_row=3, start_column=1, end_row=3, end_column=max(span, 1))
    sheet.row_dimensions[3].height = 30


def render_register_sheet(sheet, definition: SheetDefinition, context: SheetContext) -> int:
    """Render one register sheet. Returns the number of data rows."""
    register = context.programme.get(definition.register)
    records: list[Mapping[str, Any]] = (
        [record.data for record in register.records] if register else []
    )

    if definition.sort_by is not None:
        records = sorted(records, key=definition.sort_by)
    else:
        records = sorted(records, key=_sort_by_id)

    columns = definition.columns
    labels = [header_label(column) for column in columns]
    widths = [column_width(column) for column in columns]

    subtitle = definition.description
    if register is None:
        subtitle = (
            f"{subtitle} | REGISTER '{definition.register}' IS NOT DEFINED"
        )
    elif not records:
        subtitle = (
            f"{subtitle} | AWAITING EVIDENCE / POPULATION - "
            f"{len(register.records)} record(s); capability structure is "
            "defined but no evidence-backed records exist yet"
        )
    else:
        subtitle = f"{subtitle} | {len(records)} record(s)"

    _add_title_block(sheet, definition.name, subtitle, context, len(columns))

    header_row = 5
    _apply_header(sheet, header_row, labels, widths)

    if not records:
        empty_cell = sheet.cell(row=header_row + 1, column=1, value="AWAITING POPULATION")
        from openpyxl.styles import Font

        empty_cell.font = Font(name=FONT_FAMILY, size=BODY_SIZE, italic=True, color="595959")
        sheet.freeze_panes = sheet.cell(row=header_row + 1, column=1)
        return 0

    for offset, record in enumerate(records):
        row = header_row + 1 + offset
        values = [
            _cell_value(_value_for(context, definition, record, column), column)
            for column in columns
        ]
        _apply_body(sheet, row, values, columns, band=offset % 2 == 1)

    last_row = header_row + len(records)
    sheet.freeze_panes = sheet.cell(row=header_row + 1, column=2)
    last_column = sheet.cell(row=header_row, column=len(columns)).column_letter
    sheet.auto_filter.ref = f"A{header_row}:{last_column}{last_row}"

    _add_conditional_formatting(sheet, columns, header_row, last_row)
    return len(records)


def _add_conditional_formatting(
    sheet, columns: Sequence[str], header_row: int, last_row: int
) -> None:
    """Apply RAG colours, progress bars and overdue shading."""
    from openpyxl.formatting.rule import CellIsRule, FormulaRule
    from openpyxl.styles import PatternFill

    def col_letter(name: str) -> str | None:
        return sheet.cell(row=header_row, column=columns.index(name) + 1).column_letter if name in columns else None

    rag_columns = [c for c in columns if c in {"calculated_rag", "rag", "status"}]
    for name in rag_columns:
        letter = col_letter(name)
        if not letter:
            continue
        for key, (fill_colour, font_colour) in RAG_PALETTE.items():
            sheet.conditional_formatting.add(
                f"{letter}{header_row + 1}:{letter}{last_row}",
                FormulaRule(
                    formula=[f'EXACT(${letter}{header_row + 1},"{key}")'],
                    fill=PatternFill("solid", bgColor=fill_colour),
                    stopIfTrue=False,
                ),
            )

    progress = col_letter("calculated_percent_complete")
    if progress:
        sheet.conditional_formatting.add(
            f"{progress}{header_row + 1}:{progress}{last_row}",
            CellIsRule(
                operator="lessThan",
                formula=["100"],
                fill=PatternFill("solid", bgColor=RAG_AMBER),
            ),
        )

    variance = col_letter("calculated_schedule_variance_days")
    if variance:
        sheet.conditional_formatting.add(
            f"{variance}{header_row + 1}:{variance}{last_row}",
            CellIsRule(
                operator="greaterThan",
                formula=["14"],
                fill=PatternFill("solid", bgColor=RAG_RED),
            ),
        )


# --------------------------------------------------------------------------
# Gantt
# --------------------------------------------------------------------------
GANTT_ANCHOR_COLUMNS = (
    "id", "title", "phase_id", "owner", "status",
    "planned_start", "planned_finish", "calculated_percent_complete",
    "calculated_rag",
)

#: The task sheet definition, reused by the Gantt for value resolution.
SHEET_TASKS = SheetDefinition(
    "Master Tracker", "tasks", TASK_COLUMNS, "Delivery", "All WBS tasks"
)

MONTH_ABBR = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)


def _month_sequence(start: date, end: date) -> list[date]:
    months: list[date] = []
    cursor = date(start.year, start.month, 1)
    guard = date(end.year, end.month, 1)
    while cursor <= guard and len(months) <= 240:
        months.append(cursor)
        cursor = date(cursor.year + (cursor.month // 12), (cursor.month % 12) + 1, 1)
    return months


def _month_overlaps(start: date, finish: date, month: date) -> bool:
    month_end = date(
        month.year + (month.month // 12), (month.month % 12) + 1, 1
    )
    last_day = date(month_end.year, month_end.month, 1) - _one_day()
    return start <= last_day and finish >= month


def _one_day():
    from datetime import timedelta

    return timedelta(days=1)


def render_gantt(sheet, context: SheetContext) -> int:
    """Month-granularity planned timeline across every WBS task."""
    from openpyxl.styles import Alignment, Font, PatternFill

    register = context.programme.get("tasks")
    records = sorted(
        [record.data for record in register.records] if register else [],
        key=_sort_by_id,
    )
    config = context.programme.config or {}
    schedule_cfg = config.get("schedule", {}) or {}
    baseline_start = parse_date(schedule_cfg.get("baseline_start")) or context.as_of
    baseline_finish = (
        parse_date(schedule_cfg.get("baseline_planned_finish")) or context.as_of
    )

    _add_title_block(
        sheet,
        "Programme Timeline (Gantt)",
        "Planned baseline timeline at month granularity. Bars show PLANNED "
        "dates only. Cells are shaded green when evidenced progress has "
        "started and amber where the planned window has passed without "
        "completion. No actual date is inferred from this view.",
        context,
        len(GANTT_ANCHOR_COLUMNS),
    )

    header_row = 5
    months = _month_sequence(baseline_start, baseline_finish)
    labels = [header_label(col) for col in GANTT_ANCHOR_COLUMNS] + [
        f"{MONTH_ABBR[m.month - 1]} {str(m.year)[2:]}" for m in months
    ]
    widths = [column_width(col) for col in GANTT_ANCHOR_COLUMNS] + [4.5] * len(months)
    _apply_header(sheet, header_row, labels, widths)

    if not records:
        cell = sheet.cell(row=header_row + 1, column=1, value="AWAITING POPULATION")
        cell.font = Font(name=FONT_FAMILY, size=BODY_SIZE, italic=True, color="595959")
        return 0

    green_fill = PatternFill("solid", fgColor=RAG_PALETTE["GREEN"][0])
    amber_fill = PatternFill("solid", fgColor=RAG_PALETTE["AMBER"][0])
    red_fill = PatternFill("solid", fgColor=RAG_PALETTE["RED"][0])
    planned_fill = PatternFill("solid", fgColor=LIGHT_BLUE)
    today_fill = PatternFill("solid", fgColor="FFD966")

    offset = len(GANTT_ANCHOR_COLUMNS)
    for index, record in enumerate(records):
        row = header_row + 1 + index
        values = [
            _cell_value(_value_for(context, SHEET_TASKS, record, col), col)
            for col in GANTT_ANCHOR_COLUMNS
        ]
        _apply_body(sheet, row, values, GANTT_ANCHOR_COLUMNS, band=index % 2 == 1)

        start = parse_date(record.get("planned_start"))
        finish = parse_date(record.get("planned_finish"))
        rag = str(record.get("calculated_rag") or rag_status(record, context.as_of))
        pct = float(record.get("calculated_percent_complete") or 0)

        for month_index, month in enumerate(months):
            cell = sheet.cell(row=row, column=offset + month_index + 1)
            cell.alignment = Alignment(horizontal="center")
            if start and finish and _month_overlaps(start, finish, month):
                if pct >= 100:
                    cell.fill = green_fill
                elif month < date(context.as_of.year, context.as_of.month, 1):
                    cell.fill = amber_fill if rag != "RED" else red_fill
                else:
                    cell.fill = planned_fill
        sheet.row_dimensions[row].height = 15

    # Mark today's month in the header so the reader can orient the view.
    today_column = offset + months.index(
        date(context.as_of.year, context.as_of.month, 1)
    ) + 1 if date(context.as_of.year, context.as_of.month, 1) in months else None
    if today_column:
        sheet.cell(row=header_row, column=today_column).fill = today_fill

    last_column = sheet.cell(
        row=header_row, column=offset + len(months)
    ).column_letter
    sheet.freeze_panes = sheet.cell(row=header_row + 1, column=len(GANTT_ANCHOR_COLUMNS) + 1)
    sheet.auto_filter.ref = f"A{header_row}:{last_column}{header_row + len(records)}"
    return len(records)



# --------------------------------------------------------------------------
# Dashboards
# --------------------------------------------------------------------------
def _kpi(sheet, row: int, label: str, value: Any, note: str = "", rag: str = "") -> None:
    from openpyxl.styles import Alignment, Font, PatternFill

    label_cell = sheet.cell(row=row, column=1, value=label)
    label_cell.font = Font(name=FONT_FAMILY, size=BODY_SIZE, bold=True, color=NAVY)

    value_cell = sheet.cell(row=row, column=2, value=value)
    value_cell.font = Font(name=FONT_FAMILY, size=12, bold=True)
    value_cell.alignment = Alignment(horizontal="center")
    if rag:
        fill_colour, font_colour = RAG_PALETTE.get(rag, (RAG_GREY, "595959"))
        value_cell.fill = PatternFill("solid", fgColor=fill_colour)
        value_cell.font = Font(name=FONT_FAMILY, size=12, bold=True, color=font_colour)

    note_cell = sheet.cell(row=row, column=3, value=note)
    note_cell.font = Font(name=FONT_FAMILY, size=BODY_SIZE, color="595959")
    note_cell.alignment = Alignment(wrap_text=True, vertical="top")


def _register_counts(context: SheetContext) -> dict[str, int]:
    return {
        name: len(register.records)
        for name, register in context.programme.registers.items()
    }


def _task_records(context: SheetContext) -> list[Mapping[str, Any]]:
    register = context.programme.get("tasks")
    return [record.data for record in register.records] if register else []


def _evidence_facts(context: SheetContext) -> Mapping[str, Any]:
    """Facts from the collected evidence document, if available."""
    if not context.evidence:
        return {}
    collectors = context.evidence.get("collectors", [])
    facts: dict[str, Any] = {}
    for entry in collectors:
        facts[entry.get("collector", "")] = entry.get("facts", {}) or {}
    return facts


def render_executive_dashboard(sheet, context: SheetContext) -> None:
    from openpyxl.styles import Font

    config = context.programme.config or {}
    programme_cfg = config.get("programme", {}) or {}
    schedule = config.get("schedule", {}) or {}

    _add_title_block(
        sheet,
        "Executive Dashboard",
        "Management summary generated from repository evidence. A number "
        "here is only as strong as the evidence behind it; gaps are stated, "
        "not smoothed over.",
        context,
        6,
    )

    tasks = _task_records(context)
    summary = rollup(tasks, context.as_of)
    counts = _register_counts(context)
    evidence_facts = _evidence_facts(context)

    row = 5
    _kpi(
        sheet, row, "Overall RAG", summary["overall_rag"],
        "Derived from task status, schedule variance and evidence state.",
        summary["overall_rag"],
    )
    _kpi(
        sheet, row + 1, "Programme progress",
        f"{summary['percent_complete']}%",
        "Weighted by planned duration. Evidence-weighted, not asserted.",
    )
    _kpi(
        sheet, row + 2, "Baseline start",
        schedule.get("baseline_start", ""),
        f"Baseline v{schedule.get('baseline_version', '')} - "
        f"{schedule.get('baseline_status', '')}. Planned dates are baseline "
        "dates, not actuals.",
    )
    _kpi(
        sheet, row + 3, "Days to baseline finish",
        _days_between(context.as_of, schedule.get("baseline_planned_finish")),
        "Planning baseline finish. Not a forecast actual.",
    )

    git_facts = evidence_facts.get("git", {})
    test_facts = evidence_facts.get("tests", {})
    dbt_facts = evidence_facts.get("dbt", {})
    terraform_facts = evidence_facts.get("terraform", {})

    _kpi(sheet, row + 4, "Repository HEAD", git_facts.get("short_sha", "not collected"),
         f"Branch {git_facts.get('branch', '?')}, "
         f"{git_facts.get('tracked_file_count', 0)} tracked files.")
    _kpi(
        sheet, row + 5, "Last recorded test result",
        test_facts.get("test_result", "NOT_RUN"),
        f"{test_facts.get('suite_count', 0)} suites discovered. Discovery is "
        "not a passing result.",
        "RED" if test_facts.get("test_result") != "PASS" else "GREEN",
    )
    _kpi(
        sheet, row + 6, "dbt result", dbt_facts.get("dbt_result", "NOT_COLLECTED"),
        "manifest.json is PRESENT_EMPTY: the file exists but carries no "
        "result. EXISTS != PASS.",
        "RED" if dbt_facts.get("dbt_result") in {"EMPTY_OUTPUT", "MISSING"} else "GREEN",
    )
    _kpi(
        sheet, row + 7, "Terraform provisioning",
        "CONTRACTS_ONLY" if not terraform_facts.get("has_resource_block") else "RESOURCES",
        "No resource or provider block is declared: Terraform defines "
        "governance contracts and provisions nothing yet.",
        "AMBER",
    )

    populated = sum(1 for value in counts.values() if value > 0)
    _kpi(
        sheet, row + 9, "Registers populated",
        f"{populated} / {len(counts)}",
        "Capability structure is defined for every register; a register with "
        "no records is AWAITING EVIDENCE / POPULATION.",
    )

    row += 11
    sheet.cell(row=row, column=1, value="Critical path and exceptions").font = Font(
        name=FONT_FAMILY, size=12, bold=True, color=NAVY
    )
    row += 1
    at_risk = sorted(
        (t for t in tasks if rag_status(t, context.as_of) in {"RED", "AMBER"}),
        key=lambda t: (rag_sort_key(rag_status(t, context.as_of)), str(t.get("id", ""))),
    )
    labels = ["Task ID", "Title", "Status", "Planned finish", "Variance (days)", "RAG"]
    widths = [14, 50, 18, 16, 14, 8]
    _apply_header(sheet, row, labels, widths)
    if not at_risk:
        sheet.cell(row=row + 1, column=1, value="No RED or AMBER tasks recorded").font = Font(
            name=FONT_FAMILY, size=BODY_SIZE, italic=True, color="595959"
        )
    for index, task in enumerate(at_risk[:40], start=1):
        excel_row = row + index
        values = [
            str(task.get("id", "")),
            str(task.get("title", "")),
            str(task.get("status", "")),
            task.get("planned_finish", ""),
            calculate_task_fields(task, context.as_of)["calculated_schedule_variance_days"],
            rag_status(task, context.as_of),
        ]
        _apply_body(
            sheet, excel_row, values,
            ("id", "title", "status", "planned_finish",
             "calculated_schedule_variance_days", "calculated_rag"),
            band=index % 2 == 0,
        )


def _days_between(as_of: date, finish: Any) -> Any:
    parsed = parse_date(finish)
    if parsed is None:
        return ""
    return (parsed - as_of).days


def render_programme_dashboard(sheet, context: SheetContext) -> None:
    from openpyxl.styles import Font

    _add_title_block(
        sheet,
        "Programme Dashboard",
        "Delivery control view: phase progress, milestone position, register "
        "population and assurance state.",
        context,
        8,
    )

    row = 5
    sheet.cell(row=row, column=1, value="Phase progress").font = Font(
        name=FONT_FAMILY, size=12, bold=True, color=NAVY
    )
    row += 1
    phases = context.programme.get("phases")
    phase_records = sorted(
        [r.data for r in phases.records] if phases else [], key=_sort_by_id
    )
    _apply_header(
        sheet, row,
        ["Phase", "Name", "Status", "Planned start", "Planned finish",
         "% Complete", "RAG"],
        [14, 40, 18, 15, 15, 12, 8],
    )
    for index, phase in enumerate(phase_records, start=1):
        excel_row = row + index
        values = [
            phase.get("id", ""), phase.get("name", ""), phase.get("status", ""),
            phase.get("planned_start", ""), phase.get("planned_finish", ""),
            calculate_task_fields(phase, context.as_of)["calculated_percent_complete"],
            rag_status(phase, context.as_of),
        ]
        _apply_body(
            sheet, excel_row, values,
            ("id", "title", "status", "planned_start", "planned_finish",
             "calculated_percent_complete", "calculated_rag"),
            band=index % 2 == 0,
        )
    if not phase_records:
        sheet.cell(row=row + 1, column=1, value="AWAITING POPULATION").font = Font(
            name=FONT_FAMILY, size=BODY_SIZE, italic=True, color="595959"
        )

    row += len(phase_records) + 3
    sheet.cell(row=row, column=1, value="Register population by capability").font = Font(
        name=FONT_FAMILY, size=12, bold=True, color=NAVY
    )
    row += 1
    counts = _register_counts(context)
    _apply_header(
        sheet, row,
        ["Register", "Label", "Records", "Population state"],
        [26, 40, 12, 46],
    )
    for index, (name, count) in enumerate(sorted(counts.items()), start=1):
        excel_row = row + index
        spec = context.programme.register_specs.get(name)
        state = "POPULATED" if count else "AWAITING EVIDENCE / POPULATION"
        _apply_body(
            sheet, excel_row,
            [name, spec.label if spec else "", count, state],
            ("id", "title", "record_count", "status"),
            band=index % 2 == 0,
        )


# --------------------------------------------------------------------------
# Index, change log and data dictionary
# --------------------------------------------------------------------------
def render_index(sheet, context: SheetContext) -> None:
    from openpyxl.styles import Font
    from openpyxl.worksheet.hyperlink import Hyperlink

    _add_title_block(
        sheet,
        "Workbook Index and Navigation",
        "Every sheet, the register it is generated from, and the record "
        "count it currently holds. The repository under programme/registry "
        "is the source of truth; this workbook is a generated view.",
        context,
        5,
    )

    row = 5
    _apply_header(
        sheet, row,
        ["#", "Sheet", "Group", "Register", "Records"],
        [6, 40, 20, 26, 10],
    )

    index = 0
    for definition in SHEET_PLAN:
        index += 1
        excel_row = row + index
        register = context.programme.get(definition.register) if definition.register else None
        count = len(register.records) if register else ""
        _apply_body(
            sheet, excel_row,
            [index, definition.name, definition.group, definition.register, count],
            ("id", "title", "status", "name", "record_count"),
            band=index % 2 == 0,
        )
        link = Hyperlink(
            ref=f"B{excel_row}",
            target=f"#'{definition.name}'!A1",
            tooltip=f"Go to {definition.name}",
        )
        sheet.cell(row=excel_row, column=2).hyperlink = link

    row += index + 2
    sheet.cell(row=row, column=1, value="How to read this workbook").font = Font(
        name=FONT_FAMILY, size=12, bold=True, color=NAVY
    )
    rules = (
        "Machine-managed columns (suffix M in the Data Dictionary) are "
        "regenerated on every update from repository evidence.",
        "Human-controlled columns (H) are never modified by automation. They "
        "carry approvals, sign-offs, accepted risk and benefit acceptance.",
        "EXISTS is not PASS: a PRESENT_EMPTY or NOT_RUN value is reported "
        "as such and never promoted to a pass.",
        "Planned dates are planning baseline. Actual dates appear only where "
        "a record carries evidence.",
        "Machine evidence never becomes human approval.",
        "No secret value appears anywhere in this workbook.",
    )
    from openpyxl.styles import Alignment

    for offset, rule in enumerate(rules, start=1):
        cell = sheet.cell(row=row + offset, column=1, value=f"{offset}. {rule}")
        cell.font = Font(name=FONT_FAMILY, size=BODY_SIZE)
        cell.alignment = Alignment(wrap_text=True)


def render_change_log(sheet, context: SheetContext) -> None:
    _add_title_block(
        sheet,
        "Refresh Change Summary",
        "What this update changed relative to the previous workbook state. "
        "No record is ever silently deleted: a record that disappears from "
        "its register is retained here and flagged.",
        context,
        4,
    )
    labels = ["Change ID", "Sheet / Register", "Change type", "Detail"]
    widths = [14, 34, 20, 90]
    _apply_header(sheet, 5, labels, widths)

    if not context.changes:
        from openpyxl.styles import Font

        cell = sheet.cell(row=6, column=1, value="No changes recorded in this refresh")
        cell.font = Font(name=FONT_FAMILY, size=BODY_SIZE, italic=True, color="595959")
        return

    for index, change in enumerate(context.changes, start=1):
        excel_row = 5 + index
        _apply_body(
            sheet, excel_row,
            [change.get("id", ""), change.get("target", ""),
             change.get("type", ""), change.get("detail", "")],
            ("id", "name", "status", "description"),
            band=index % 2 == 0,
        )


def render_data_dictionary(sheet, context: SheetContext) -> None:
    from openpyxl.styles import Font

    _add_title_block(
        sheet,
        "Data Dictionary and Field Ownership",
        "Every column used in this workbook, what it means, and who may "
        "change it. M = machine-managed, H = human-controlled, "
        "S = shared (human-seeded, machine-maintained).",
        context,
        4,
    )
    _apply_header(
        sheet, 5,
        ["Sheet", "Column", "Definition", "Ownership"],
        [34, 34, 70, 42],
    )

    definitions = COLUMN_DEFINITIONS
    row = 5
    seen: set[tuple[str, str]] = set()
    for sheet_def in SHEET_PLAN:
        for column in sheet_def.columns:
            key = (sheet_def.name, column)
            if key in seen:
                continue
            seen.add(key)
            row += 1
            ownership = _ownership_marker(context.programme, column)
            _apply_body(
                sheet, row,
                [sheet_def.name, column,
                 definitions.get(column, _default_definition(column)),
                 ownership],
                ("title", "name", "description", "status"),
                band=(row % 2) == 0,
            )


def _default_definition(column: str) -> str:
    label = header_label(column)
    if label.endswith(" Ids") or label.endswith(" Id"):
        return f"{label}: stable identifier(s) traced to another register."
    return f"{label}: register field."


# --------------------------------------------------------------------------
# Data dictionary content and ownership markers
# --------------------------------------------------------------------------
COLUMN_DEFINITIONS: dict[str, str] = {
    "id": "Stable record identifier. Never reused, never renumbered.",
    "title": "Short human title of the record.",
    "name": "Short human name of the record.",
    "description": "What the record covers.",
    "status": "Current lifecycle state, from the register's closed vocabulary.",
    "owner": "Accountable role or team (role-based, not personal data).",
    "notes": "Free-text context. Human-maintained.",
    "evidence_path": "Repository path holding the supporting evidence.",
    "evidence_url": "External evidence reference, where an internal path is not applicable.",
    "planned_start": "PLANNING BASELINE start date. Not an actual date.",
    "planned_finish": "PLANNING BASELINE finish date. Not an actual date.",
    "actual_start": "Evidenced actual start. Blank when unevidenced; never invented.",
    "actual_finish": "Evidenced actual finish. Blank when unevidenced; never invented.",
    "forecast_finish": "Current forecast finish agreed by the accountable owner.",
    "calculated_percent_complete": "Machine-calculated, evidence-weighted percent complete. Weighted by planned duration.",
    "calculated_schedule_variance_days": "Machine-calculated days late against plan. Negative means ahead of plan.",
    "calculated_rag": "Machine-derived RAG from status, variance and evidence. A human override is held separately in manual_rag.",
    "calculated_days_to_finish": "Machine-calculated days from the refresh date to the planned finish.",
    "manual_rag": "HUMAN override of the derived RAG. Automation never writes it.",
    "risk_acceptance": "HUMAN residual-risk acceptance decision. Automation never writes it.",
    "residual_risk_acceptance": "HUMAN residual-risk acceptance after mitigation.",
    "business_approval": "HUMAN business approval. Automation never writes it.",
    "architecture_approval": "HUMAN architecture approval. Automation never writes it.",
    "uat_approval": "HUMAN UAT approval. Automation never writes it.",
    "milestone_sign_off": "HUMAN milestone sign-off. Automation never writes it.",
    "benefit_acceptance": "HUMAN benefit acceptance. Automation never writes it.",
    "budget_approved": "HUMAN budget approval. Automation never writes it.",
    "technical_status": "Machine-observed technical state, for example DISCOVERED or CONTRACT_ONLY.",
    "test_result": "Last recorded machine test result. NOT_RUN means no result is recorded, which is not a pass.",
    "test_count": "Machine-counted tests in the referenced suite.",
    "git_sha": "Machine-captured Git commit SHA.",
    "dbt_result": "Machine-observed dbt execution state. EMPTY_OUTPUT means the artefact exists but carries no result.",
    "dbt_model_count": "Machine-counted declared dbt models.",
    "dbt_test_count": "Machine-counted declared dbt tests.",
    "terraform_validation": "Machine-observed terraform validation state.",
    "compose_validation": "Machine-observed docker compose validation state.",
    "severity": "Impact classification from the register's closed vocabulary.",
    "classification": "Data or environment classification level.",
    "verification_status": "How a control was proven. VERIFIED_RUNTIME is distinct from DOCUMENTED_ONLY.",
    "implementation": "Whether the control is implemented, partially implemented, designed or absent.",
    "sha256": "Content hash of the referenced artefact.",
    "bytes": "Size in bytes. A zero-byte artefact is PRESENT_EMPTY, not a pass.",
    "location": "Repository path of the referenced artefact.",
    "record_count": "Machine-counted records in the register.",
}


def _ownership_marker(programme: Programme, column: str) -> str:
    from ..ownership import classify_field
    from ..model import HUMAN, MACHINE, SHARED

    kind = classify_field(programme, column)
    marker = {
        HUMAN: "H - Human-controlled",
        MACHINE: "M - Machine-managed",
        SHARED: "S - Shared (human-seeded)",
    }.get(kind, "H - Human-controlled")
    return marker


# --------------------------------------------------------------------------
# Workbook assembly
# --------------------------------------------------------------------------
SPECIAL_SHEETS = {
    "Index",
    "Executive Dashboard",
    "Programme Dashboard",
    "Gantt",
    "Change Log",
    "Data Dictionary",
}


#: Characters Excel forbids in a sheet title, plus the title length cap.
_INVALID_TITLE_CHARS = set(r"[]:*?/\\")
_MAX_TITLE_LENGTH = 31


def validate_sheet_plan() -> None:
    """Fail fast when the sheet plan cannot be rendered by Excel.

    Excel rejects a sheet title containing any of ``[]:*?/\\`` or longer
    than 31 characters, and rejects duplicate titles. Catching this here
    turns a late render crash into an immediate, actionable error.
    """
    seen: set[str] = set()
    for definition in SHEET_PLAN:
        name = definition.name
        if not name:
            raise ValueError("Sheet definition has an empty name")
        if any(char in _INVALID_TITLE_CHARS for char in name):
            raise ValueError(
                f"Sheet title {name!r} contains a character Excel forbids "
                f"({''.join(sorted(_INVALID_TITLE_CHARS))})"
            )
        if len(name) > _MAX_TITLE_LENGTH:
            raise ValueError(
                f"Sheet title {name!r} exceeds {_MAX_TITLE_LENGTH} characters"
            )
        if name in seen:
            raise ValueError(f"Duplicate sheet title {name!r}")
        seen.add(name)


def build_workbook(context: SheetContext) -> Any:
    """Build the complete workbook from the loaded programme."""
    openpyxl = _openpyxl()
    from openpyxl import Workbook

    validate_sheet_plan()

    workbook = Workbook()
    # Remove the default sheet so tab order exactly follows SHEET_PLAN.
    workbook.remove(workbook.active)

    for definition in SHEET_PLAN:
        sheet = workbook.create_sheet(title=definition.name)
        if definition.name == "Index":
            render_index(sheet, context)
        elif definition.name == "Executive Dashboard":
            render_executive_dashboard(sheet, context)
        elif definition.name == "Programme Dashboard":
            render_programme_dashboard(sheet, context)
        elif definition.name == "Gantt":
            render_gantt(sheet, context)
        elif definition.name == "Change Log":
            render_change_log(sheet, context)
        elif definition.name == "Data Dictionary":
            render_data_dictionary(sheet, context)
        else:
            render_register_sheet(sheet, definition, context)
        sheet.sheet_view.showGridLines = False

    config = context.programme.config or {}
    workbook_cfg = config.get("workbook", {}) or {}
    workbook.properties.title = str(
        workbook_cfg.get("title", "Enterprise Programme Governance and Assurance")
    )
    workbook.properties.creator = "programme/tooling/update_workbook.py"
    workbook.properties.description = (
        "Generated from programme/registry. The repository is the source of "
        "truth; human-controlled fields are preserved across refreshes."
    )
    workbook.properties.version = str(workbook_cfg.get("version", ""))
    return workbook
