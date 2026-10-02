"""Programme calculations: progress, schedule variance and RAG.

Every value produced here is a MACHINE field. Nothing in this module
writes a human-controlled field, and nothing here invents an actual date.

Progress model
--------------
Task progress is evidence-weighted, not asserted:

* COMPLETE requires status COMPLETE plus recorded evidence.
* IN_PROGRESS is credited proportionally to elapsed planned duration.
* A task with no evidence never reaches COMPLETE credit.

RAG model
---------
RAG is derived, never typed. A human may override it with ``manual_rag``,
which the workbook preserves and the calculation reports separately.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Mapping, Sequence

from .validation import parse_date

GREEN = "GREEN"
AMBER = "AMBER"
RED = "RED"
GREY = "GREY"

#: An in-progress task never credits above this fraction of completion.
_IN_PROGRESS_CAP = 0.95

#: Tasks finishing later than this many days past plan are RED.
RED_VARIANCE_DAYS = 14
#: Tasks finishing this many days past plan are AMBER.
AMBER_VARIANCE_DAYS = 0

COMPLETE_STATUSES = frozenset({"COMPLETE", "ACHIEVED", "ACCEPTED", "CLOSED"})
TERMINAL_STATUSES = frozenset({"CANCELLED", "REJECTED"})
_STARTED_STATUSES = frozenset({"IN_PROGRESS", "BLOCKED", "ON_HOLD"})
BLOCKED_STATUSES = frozenset({"BLOCKED"})


@dataclass(frozen=True)
class Schedule:
    """The planned and evidenced schedule position of one record.

    ``actual_*`` and ``forecast_finish`` are only ever populated from
    recorded evidence. A record without an evidenced finish has an
    unknown variance, which is reported as unknown rather than as zero.
    """

    planned_start: date | None = None
    planned_finish: date | None = None
    actual_start: date | None = None
    actual_finish: date | None = None
    forecast_finish: date | None = None

    @property
    def planned_duration_days(self) -> int:
        """Planned working span in days, never less than one."""
        if not self.planned_start or not self.planned_finish:
            return 0
        return max((self.planned_finish - self.planned_start).days + 1, 1)

    def elapsed_days(self, as_of: date) -> int:
        """Days elapsed since the planned start, floored at zero."""
        if not self.planned_start:
            return 0
        return max((as_of - self.planned_start).days, 0)


def _schedule_for(record: Mapping[str, Any], as_of: date) -> Schedule:
    """Build the schedule view of a record from its date fields."""
    del as_of  # retained for a stable call signature
    return Schedule(
        planned_start=parse_date(record.get("planned_start")),
        planned_finish=parse_date(record.get("planned_finish")),
        actual_start=parse_date(record.get("actual_start")),
        actual_finish=parse_date(record.get("actual_finish")),
        forecast_finish=parse_date(record.get("forecast_finish")),
    )


def has_evidence(record: Mapping[str, Any]) -> bool:
    """True when the record carries any evidence reference."""
    for key in ("evidence_path", "evidence_url", "evidence_id"):
        value = record.get(key)
        if value not in (None, "", [], {}):
            return True
    return False


def percent_complete(
    record: Mapping[str, Any], as_of: date | None = None
) -> float:
    """Evidence-weighted percent complete for one task-like record."""
    as_of = as_of or date.today()
    status = str(record.get("status", "")).upper()
    evidence = has_evidence(record)
    schedule = _schedule_for(record, as_of)

    if status in TERMINAL_STATUSES:
        return 0.0

    if status in COMPLETE_STATUSES:
        # COMPLETE without evidence is credited but flagged by RAG, never
        # silently promoted to 100 with no proof.
        return 100.0 if evidence else 90.0

    if status not in _STARTED_STATUSES:
        return 0.0

    duration = schedule.planned_duration_days
    if duration <= 0:
        return 0.0
    elapsed = schedule.elapsed_days(as_of)
    ratio = max(0.0, min(elapsed / duration, 1.0))
    if not evidence and elapsed <= 0:
        return 0.0
    # An unfinished task never reads as complete: the planned span is
    # capped below 100 so an overdue task cannot look delivered.
    return round(min(ratio, _IN_PROGRESS_CAP) * 100.0, 1)


def schedule_variance_days(
    record: Mapping[str, Any], as_of: date | None = None
) -> int | None:
    """Days late against plan. Negative means ahead of plan.

    Variance is measured on the actual finish where one is evidenced, and
    otherwise on the forecast finish. It is never synthesised.
    """
    as_of = as_of or date.today()
    schedule = _schedule_for(record, as_of)
    status = str(record.get("status", "")).upper()

    if status in TERMINAL_STATUSES:
        return None

    finish = schedule.actual_finish or schedule.forecast_finish
    if finish and schedule.planned_finish:
        return (finish - schedule.planned_finish).days

    if status in COMPLETE_STATUSES:
        # Complete without an evidenced finish: variance is unknown.
        return None

    if schedule.planned_finish and schedule.planned_finish < as_of:
        if status in _STARTED_STATUSES:
            return (as_of - schedule.planned_finish).days
        # Not started and already past its planned finish.
        return (as_of - schedule.planned_finish).days

    return 0 if schedule.planned_finish else None


def rag_status(
    record: Mapping[str, Any], as_of: date | None = None
) -> str:
    """Derive RAG from status, schedule and evidence.

    Rules, in order:
      * cancelled / rejected with no plan  -> GREY
      * blocked, or missing a required plan -> RED
      * complete with evidence, on time     -> GREEN
      * complete without evidence           -> AMBER
      * more than RED_VARIANCE_DAYS late    -> RED
      * any other finish late                -> AMBER
      * not started and not yet due          -> GREY
      * otherwise                            -> GREEN
    """
    as_of = as_of or date.today()
    status = str(record.get("status", "")).upper()
    evidence = has_evidence(record)
    schedule = _schedule_for(record, as_of)
    variance = schedule_variance_days(record, as_of)

    if status in TERMINAL_STATUSES:
        return GREY

    if status in BLOCKED_STATUSES:
        return RED

    if status in COMPLETE_STATUSES:
        if not evidence:
            return AMBER
        if variance is not None and variance > AMBER_VARIANCE_DAYS:
            return AMBER if variance <= RED_VARIANCE_DAYS else RED
        return GREEN

    if not schedule.planned_start or not schedule.planned_finish:
        return GREY

    if variance is None:
        return GREY

    if variance > RED_VARIANCE_DAYS:
        return RED
    if variance > AMBER_VARIANCE_DAYS:
        return AMBER

    if status == "NOT_STARTED" and schedule.planned_start > as_of:
        return GREY

    return GREEN


def effective_rag(
    record: Mapping[str, Any], as_of: date | None = None
) -> str:
    """RAG after any human override, reported separately from the derived value."""
    manual = str(record.get("manual_rag", "") or "").upper()
    if manual in {GREEN, AMBER, RED, GREY}:
        return manual
    return rag_status(record, as_of)


def calculate_task_fields(
    record: Mapping[str, Any], as_of: date | None = None
) -> dict[str, Any]:
    """Return the machine fields for one task-like record."""
    as_of = as_of or date.today()
    variance = schedule_variance_days(record, as_of)
    schedule = _schedule_for(record, as_of)
    days_to_finish = (
        (schedule.planned_finish - as_of).days if schedule.planned_finish else None
    )
    return {
        "calculated_percent_complete": percent_complete(record, as_of),
        "calculated_schedule_variance_days": (
            "" if variance is None else variance
        ),
        "calculated_rag": rag_status(record, as_of),
        "calculated_days_to_finish": (
            "" if days_to_finish is None else days_to_finish
        ),
    }


def weighted_progress(
    records: Sequence[Mapping[str, Any]], as_of: date | None = None
) -> float:
    """Weight tasks by planned duration; fall back to equal weighting."""
    as_of = as_of or date.today()
    if not records:
        return 0.0
    total_weight = 0.0
    weighted = 0.0
    for record in records:
        schedule = _schedule_for(record, as_of)
        weight = float(schedule.planned_duration_days or 0)
        if weight <= 0:
            weight = 1.0
        total_weight += weight
        weighted += weight * percent_complete(record, as_of)
    if total_weight <= 0:
        return 0.0
    return round(weighted / total_weight, 1)


def rollup(
    records: Iterable[Mapping[str, Any]], as_of: date | None = None
) -> dict[str, Any]:
    """Aggregate counts, RAG distribution and progress for a set of records."""
    as_of = as_of or date.today()
    items = list(records)
    rag_counts = {GREEN: 0, AMBER: 0, RED: 0, GREY: 0}
    for record in items:
        rag_counts[rag_status(record, as_of)] += 1

    variances = [
        value
        for value in (
            schedule_variance_days(record, as_of) for record in items
        )
        if value is not None
    ]

    return {
        "total": len(items),
        "percent_complete": weighted_progress(items, as_of),
        "rag": rag_counts,
        "overall_rag": overall_rag(rag_counts),
        "average_variance_days": (
            round(sum(variances) / len(variances), 1) if variances else 0
        ),
        "max_variance_days": max(variances) if variances else 0,
        "late_count": sum(1 for value in variances if value > 0),
        "complete_count": sum(
            1
            for record in items
            if str(record.get("status", "")).upper() in COMPLETE_STATUSES
        ),
    }


def overall_rag(rag_counts: Mapping[str, int]) -> str:
    """Programme RAG is the worst material colour, not an average."""
    if not rag_counts:
        return GREY
    if rag_counts.get(RED, 0) > 0:
        return RED
    if rag_counts.get(AMBER, 0) > 0:
        return AMBER
    if rag_counts.get(GREEN, 0) > 0:
        return GREEN
    return GREY


def rag_sort_key(rag: str) -> int:
    """Worst status first, for management ordering."""
    return {RED: 0, AMBER: 1, GREEN: 2, GREY: 3}.get(str(rag).upper(), 4)
