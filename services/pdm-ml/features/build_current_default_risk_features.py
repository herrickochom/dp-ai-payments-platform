#!/usr/bin/env python3
"""Build current PDM operational features for Strict Early Warning v1.

Reads the current PDMIS JSON files under data/pdmis and writes one JSONL row
per DISBURSED loan to:

    data/pdmis_ml/default_risk_current_scoring.jsonl

The output contains the exact 20-feature contract expected by
pdm_default_risk_strict_early_warning_v1 plus identifiers/geography used to
join predictions back to operational records.

Important POC limitation
-----------------------
The builder reads the current PDMIS JSON files under data/pdmis together with
the authoritative monthly repayment-event history written by
``pdmis_generator.py`` (data/pdmis/repayments.json). payment_failure_count and
repayment_trend_3m are reconstructed from those repayment events, not from
current-state proxies. If the repayment history file is absent the builder
fails loudly rather than silently substituting proxies.

Every output row carries feature_source_mode for auditability.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_PDMIS_ROOT = Path("data/pdmis")
DEFAULT_OUTPUT = Path("data/pdmis_ml/default_risk_current_scoring.jsonl")
DEFAULT_SUMMARY = Path("data/pdmis_ml/default_risk_current_scoring_summary.json")
DEFAULT_METADATA = Path(
    "services/pdm-ml/models/"
    "pdm_default_risk_strict_early_warning_v1_metadata.json"
)

LOAN_INTEREST_RATE = 8.0
LOAN_TERM_MONTHS = 12

EXPECTED_FEATURES = [
    "loan_age_days",
    "months_since_disbursement",
    "project_type",
    "special_group",
    "amount_approved",
    "amount_disbursed",
    "interest_rate",
    "interest_charged",
    "loan_term_months",
    "scheduled_monthly_repayment",
    "amount_repaid_as_of_observation",
    "outstanding_amount_as_of_observation",
    "repayment_rate_as_of_observation",
    "payment_failure_count",
    "repayment_trend_3m",
    "account_substituted",
    "shared_identity_alert",
    "nin_unverified",
    "identity_alert_count",
    "household_economic_score",
]

ECONOMIC_SCORE = {
    "LOW": 0,
    "MEDIUM": 1,
    "HIGH": 2,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--pdmis-root", type=Path, default=DEFAULT_PDMIS_ROOT)
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    p.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    p.add_argument("--metadata", type=Path, default=DEFAULT_METADATA)
    p.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=None,
        help=(
            "Observation date YYYY-MM-DD. When omitted, the authoritative "
            "max as_of_date from loans.json is used."
        ),
    )
    p.add_argument(
        "--repayments",
        type=Path,
        default=None,
        help=(
            "Repayment event JSON/JSONL. When omitted, defaults to "
            "<pdmis-root>/repayments.json and fails if that file is absent. "
            "Supported fields include loan_id plus amount/payment_amount and "
            "payment_date/event_date, with optional status/payment_status."
        ),
    )
    return p.parse_args()


def load_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def load_records(path: Path) -> list[dict[str, Any]]:
    value = load_json(path)
    if isinstance(value, list):
        return [dict(x) for x in value]
    if isinstance(value, dict):
        # Permit common wrappers if contracts evolve.
        for key in ("records", "data", "items"):
            if isinstance(value.get(key), list):
                return [dict(x) for x in value[key]]
    raise ValueError(f"{path}: expected JSON array of records")


def index_unique(
    rows: Sequence[Mapping[str, Any]],
    key: str,
    dataset: str,
) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        value = str(row.get(key, "")).strip()
        if not value:
            raise ValueError(f"{dataset}: blank {key}")
        if value in result:
            raise ValueError(f"{dataset}: duplicate {key}={value}")
        result[value] = row
    return result


def parse_date_value(value: Any, field: str) -> date:
    if value is None or str(value).strip() == "":
        raise ValueError(f"Missing required date: {field}")
    text = str(value).strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ValueError(f"Invalid {field}: {value!r}") from exc


def months_between(start: date, end: date) -> int:
    return max(0, (end.year - start.year) * 12 + end.month - start.month)


def numeric(row: Mapping[str, Any], names: Sequence[str], default: float = 0.0) -> float:
    for name in names:
        if name in row and row[name] not in (None, ""):
            try:
                return float(row[name])
            except (TypeError, ValueError):
                pass
    return float(default)


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {
        "1", "true", "yes", "y", "verified", "success", "successful"
    }


def derive_disbursement_date(loan: Mapping[str, Any]) -> date:
    # APPROVED is not DISBURSED and a disbursement business event requires an
    # authoritative disbursement lifecycle. Never fall back to approval_date.
    value = loan.get("disbursement_date")
    if value in (None, "", "NOT_APPLICABLE"):
        raise ValueError(
            f"{loan['loan_id']}: DISBURSED loan missing authoritative "
            "disbursement_date"
        )
    return parse_date_value(value, "disbursement_date")


def special_group_for(
    beneficiary: Mapping[str, Any],
    business_plan: Mapping[str, Any] | None,
) -> str:
    candidates = (
        beneficiary.get("special_group"),
        beneficiary.get("special_group_code"),
        beneficiary.get("beneficiary_group"),
        beneficiary.get("vulnerability_group"),
        (business_plan or {}).get("special_group"),
    )
    for value in candidates:
        if value not in (None, ""):
            return str(value).upper()

    # The operational generator's group cycle is deterministic by beneficiary
    # serial. This fallback preserves its current contract.
    beneficiary_id = str(beneficiary.get("beneficiary_id", ""))
    digits = "".join(ch for ch in beneficiary_id if ch.isdigit())
    if not digits:
        raise ValueError(
            f"Cannot derive special_group for beneficiary {beneficiary_id!r}"
        )
    i = int(digits)
    cycle = (
        "WOMEN", "WOMEN", "WOMEN",
        "YOUTH", "YOUTH", "YOUTH",
        "PWD", "ELDERLY", "GENERAL", "GENERAL",
    )
    return cycle[(i - 1) % len(cycle)]


def identity_features(
    beneficiary: Mapping[str, Any],
    loan: Mapping[str, Any],
) -> tuple[int, int, int, int, int, int]:
    """Return the five canonical assurance flags and their exact sum."""
    def flag(row: Mapping[str, Any], name: str) -> int:
        return int(truthy(row.get(name, False)))

    shared_identity = flag(beneficiary, "shared_identity_alert")
    nin_unverified = flag(beneficiary, "nin_unverified")
    shared_phone = flag(beneficiary, "shared_phone_alert")
    multiple_loan = flag(beneficiary, "multiple_loan_alert")
    account_substituted = (
        flag(loan, "account_substituted") or flag(beneficiary, "account_substituted")
    )
    identity_alert_count = sum(
        (
            shared_identity,
            nin_unverified,
            shared_phone,
            multiple_loan,
            account_substituted,
        )
    )
    return (
        shared_identity,
        nin_unverified,
        shared_phone,
        multiple_loan,
        account_substituted,
        identity_alert_count,
    )


def account_substituted_for(
    loan: Mapping[str, Any],
    beneficiary: Mapping[str, Any],
) -> int:
    # Prefer an explicit operational flag when present.
    for field in (
        "account_substituted",
        "account_substitution_flag",
        "substituted_account",
    ):
        if field in loan:
            return int(truthy(loan[field]))
        if field in beneficiary:
            return int(truthy(beneficiary[field]))

    # Match the current synthetic operational convention: every 12th loan.
    loan_id = str(loan.get("loan_id", ""))
    digits = "".join(ch for ch in loan_id if ch.isdigit())
    return int(bool(digits) and int(digits) % 12 == 0)


def household_score(household: Mapping[str, Any]) -> int:
    status = str(household.get("economic_status", "")).strip().upper()
    if status in ECONOMIC_SCORE:
        return ECONOMIC_SCORE[status]

    # If a numeric score is introduced later, accept it explicitly.
    if household.get("economic_score") not in (None, ""):
        return int(float(household["economic_score"]))

    raise ValueError(
        f"Unknown household economic status: {household.get('economic_status')!r}"
    )


def load_repayment_events(path: Path | None) -> dict[str, list[dict[str, Any]]]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(path)

    if path.suffix.lower() == ".jsonl":
        rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    else:
        value = load_json(path)
        rows = value if isinstance(value, list) else value.get("records", [])

    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        loan_id = str(row.get("loan_id", "")).strip()
        if loan_id:
            grouped.setdefault(loan_id, []).append(row)
    return grouped


def repayment_event_date(row: Mapping[str, Any]) -> date | None:
    for field in ("payment_date", "event_date", "transaction_date", "date"):
        if row.get(field):
            try:
                return parse_date_value(row[field], field)
            except ValueError:
                # Non-payment events deliberately use NOT_APPLICABLE for
                # payment_date; continue to their authoritative event_date.
                continue
    return None


def repayment_event_amount(row: Mapping[str, Any]) -> float:
    return max(
        0.0,
        numeric(
            row,
            ("amount", "payment_amount", "amount_paid", "repaid_amount"),
            0.0,
        ),
    )


def successful_event(row: Mapping[str, Any]) -> bool:
    for field in ("status", "payment_status", "transaction_status"):
        if row.get(field) not in (None, ""):
            status = str(row[field]).strip().upper()
            if status in {"FAILED", "MISSED", "REJECTED", "RJCT"}:
                return False
            return status in {
                "SUCCESS", "SUCCESSFUL", "COMPLETED", "PAID",
                "SETTLED", "ACSC", "CREDITED", "LATE", "PARTIAL", "RECOVERED",
            }
    return repayment_event_amount(row) > 0


def repayment_features_from_events(
    events: Sequence[Mapping[str, Any]],
    disbursement_date: date,
    observation_date: date,
    scheduled_monthly: float,
) -> tuple[float, int, float]:
    """Return paid amount, failure count, and 3-month trend."""
    relevant = []
    failures = 0
    for event in events:
        event_date = repayment_event_date(event)
        if event_date is None or event_date > observation_date:
            continue
        if event_date < disbursement_date:
            continue
        if successful_event(event):
            relevant.append((event_date, repayment_event_amount(event)))
        else:
            failures += 1

    paid = sum(amount for _, amount in relevant)

    def month_key(d: date) -> tuple[int, int]:
        return d.year, d.month

    monthly: dict[tuple[int, int], float] = {}
    for d, amount in relevant:
        monthly[month_key(d)] = monthly.get(month_key(d), 0.0) + amount

    # Build the last six calendar repayment months ending at observation month.
    keys: list[tuple[int, int]] = []
    y, m = observation_date.year, observation_date.month
    for offset in range(5, -1, -1):
        total = y * 12 + (m - 1) - offset
        keys.append((total // 12, total % 12 + 1))

    values = [monthly.get(k, 0.0) for k in keys]
    previous = values[:3]
    recent = values[3:]
    previous_avg = sum(previous) / 3.0
    recent_avg = sum(recent) / 3.0
    trend = (
        (recent_avg - previous_avg) / scheduled_monthly
        if scheduled_monthly > 0
        else 0.0
    )
    return paid, failures, trend


def repayment_due_at_or_before(
    loan: Mapping[str, Any],
    observation_date: date,
) -> bool:
    """True when the authoritative first instalment is due by observation date.

    A DISBURSED loan must carry first_repayment_date. The ML feature layer must
    not fabricate repayment schedule dates from disbursement_date or elapsed
    month heuristics.
    """
    value = loan.get("first_repayment_date")
    if value in (None, "", "NOT_APPLICABLE"):
        raise ValueError(
            f"{loan['loan_id']}: DISBURSED loan missing authoritative "
            "first_repayment_date"
        )
    first_due = parse_date_value(value, "first_repayment_date")
    return first_due <= observation_date

def validate_model_contract(metadata_path: Path) -> None:
    metadata = load_json(metadata_path)
    actual = metadata.get("input_features")
    if actual != EXPECTED_FEATURES:
        raise ValueError(
            "Strict model feature contract differs from this builder.\n"
            f"Expected: {EXPECTED_FEATURES}\n"
            f"Model:    {actual}"
        )


def main() -> None:
    args = parse_args()
    validate_model_contract(args.metadata)

    root = args.pdmis_root
    beneficiaries = load_records(root / "beneficiaries.json")
    households = load_records(root / "households.json")
    loans = load_records(root / "loans.json")
    business_plans = load_records(root / "business_plans.json")
    saccos = load_records(root / "saccos.json")

    beneficiary_by_id = index_unique(
        beneficiaries, "beneficiary_id", "beneficiaries"
    )
    household_by_id = index_unique(households, "household_id", "households")
    business_by_id = index_unique(
        business_plans, "business_plan_id", "business_plans"
    )
    sacco_by_id = index_unique(saccos, "sacco_id", "saccos")

    # Portfolios are observed at the authoritative source as_of_date
    # (PDM POC v1 decision 7); never infer it from latest approval/payment dates.
    observation_date = args.as_of_date or max(
        parse_date_value(
            loan["as_of_date"], f"{loan['loan_id']}: as_of_date"
        )
        for loan in loans
    )

    # Monthly repayment events are the canonical basis for repayment features
    # (decisions 1-2). If the history file is absent the build fails loudly
    # instead of silently substituting current-state proxies.
    repayment_path = args.repayments or (root / "repayments.json")
    repayments_by_loan = load_repayment_events(repayment_path)

    rows: list[dict[str, Any]] = []
    skipped_not_disbursed = 0
    event_backed_loans = 0
    not_yet_due_loans = 0
    missing_history_loans = 0
    proxy_backed_loans = 0

    for loan in loans:
        if str(loan.get("loan_status", "")).upper() != "DISBURSED":
            skipped_not_disbursed += 1
            continue

        loan_id = str(loan["loan_id"])
        beneficiary_id = str(loan["beneficiary_id"])
        beneficiary = beneficiary_by_id[beneficiary_id]
        household = household_by_id[str(beneficiary["household_id"])]
        business_plan = business_by_id.get(str(loan.get("business_plan_id", "")))
        sacco = sacco_by_id.get(str(loan.get("sacco_id", "")), {})

        disbursement_date = derive_disbursement_date(loan)
        if observation_date < disbursement_date:
            # Future approval cohorts cannot be meaningfully scored yet.
            continue

        amount_approved = numeric(loan, ("amount_approved",), 0.0)
        amount_disbursed = numeric(loan, ("amount_disbursed",), 0.0)
        if amount_disbursed <= 0:
            continue

        interest_rate = numeric(
            loan, ("interest_rate",), LOAN_INTEREST_RATE
        )
        loan_term = int(
            numeric(loan, ("loan_term_months",), LOAN_TERM_MONTHS)
        )
        if loan_term <= 0:
            loan_term = LOAN_TERM_MONTHS

        scheduled = amount_disbursed / loan_term
        months_elapsed = months_between(disbursement_date, observation_date)

        events = repayments_by_loan.get(loan_id, [])
        if events:
            paid, failures, trend = repayment_features_from_events(
                events, disbursement_date, observation_date, scheduled,
            )
            event_backed_loans += 1
            source_mode = "REPAYMENT_EVENTS"
        elif repayment_due_at_or_before(loan, observation_date):
            missing_history_loans += 1
            raise ValueError(
                f"{loan_id}: first repayment is due by {observation_date} but "
                f"no monthly repayment events exist in {repayment_path}"
            )
        else:
            # Valid observation: disbursed, but the first contractual
            # instalment is not yet due. No repayment history is fabricated.
            paid, failures, trend = 0.0, 0, 0.0
            not_yet_due_loans += 1
            source_mode = "NOT_YET_DUE"

        paid = min(max(0.0, paid), amount_disbursed)
        outstanding = max(0.0, amount_disbursed - paid)
        # Principal-based repayment rate (decision 1): principal repaid against
        # principal disbursed. Kept under the strict model's legacy feature name.
        repayment_rate = (
            paid / amount_disbursed if amount_disbursed > 0 else 0.0
        )

        (
            shared_identity,
            nin_unverified,
            shared_phone,
            multiple_loan,
            account_substituted,
            identity_alert_count,
        ) = identity_features(beneficiary, loan)
        account_substituted = int(
            truthy(account_substituted) or truthy(account_substituted_for(loan, beneficiary))
        )

        project_type = str(
            loan.get("project_type")
            or (business_plan or {}).get("project_type")
            or ""
        ).upper()
        if not project_type:
            raise ValueError(f"{loan_id}: missing project_type")

        row = {
            # Join/audit columns; not model features in Strict EW.
            "snapshot_id": f"CURRENT-{loan_id}",
            "loan_id": loan_id,
            "beneficiary_id": beneficiary_id,
            "sacco_id": str(loan.get("sacco_id", "")),
            "observation_date": observation_date.isoformat(),
            "approval_date": str(loan.get("approval_date", "")),
            "disbursement_date": disbursement_date.isoformat(),
            "region": str(beneficiary.get("region", sacco.get("region", ""))),
            "district": str(beneficiary.get("district", sacco.get("district", ""))),
            "county": str(beneficiary.get("county", "")),
            "sub_county": str(beneficiary.get("sub_county", sacco.get("sub_county", ""))),
            "parish": str(beneficiary.get("parish", sacco.get("parish", ""))),
            "village": str(beneficiary.get("village", "")),
            "feature_source_mode": source_mode,

            # Exact Strict Early Warning v1 feature contract.
            "loan_age_days": max(0, (observation_date - disbursement_date).days),
            "months_since_disbursement": months_elapsed,
            "project_type": project_type,
            "special_group": special_group_for(beneficiary, business_plan),
            "amount_approved": int(round(amount_approved)),
            "amount_disbursed": int(round(amount_disbursed)),
            "interest_rate": float(interest_rate),
            "interest_charged": round(amount_approved * interest_rate / 100.0, 2),
            "loan_term_months": loan_term,
            "scheduled_monthly_repayment": int(round(scheduled)),
            "amount_repaid_as_of_observation": int(round(paid)),
            "outstanding_amount_as_of_observation": int(round(outstanding)),
            "repayment_rate_as_of_observation": round(repayment_rate, 6),
            "payment_failure_count": int(failures),
            "repayment_trend_3m": round(float(trend), 6),
            "account_substituted": int(account_substituted),
            "shared_identity_alert": int(shared_identity),
            "nin_unverified": int(nin_unverified),
            "identity_alert_count": int(identity_alert_count),
            "household_economic_score": household_score(household),
        }

        missing = [name for name in EXPECTED_FEATURES if name not in row]
        if missing:
            raise RuntimeError(f"{loan_id}: missing features {missing}")

        rows.append(row)

    if not rows:
        raise ValueError("No eligible DISBURSED operational loans were found.")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(
                json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                + "\n"
            )

    summary = {
        "rows": len(rows),
        "as_of_date": observation_date.isoformat(),
        "model_feature_count": len(EXPECTED_FEATURES),
        "repayment_event_rows": event_backed_loans,
        "not_yet_due_rows": not_yet_due_loans,
        "missing_history_rows": missing_history_loans,
        "current_state_proxy_rows": proxy_backed_loans,
        "skipped_not_disbursed": skipped_not_disbursed,
        "output_file": str(args.output),
        "repayment_history_file": str(repayment_path),
        "feature_source_mode": "AUTHORITATIVE_REPAYMENT_HISTORY",
        "warning": (
            "Rows use authoritative repayment history. DISBURSED loans whose "
            "first instalment is not yet due are NOT_YET_DUE; no current-state "
            "proxies or fabricated repayment history are used."
        ),
    }
    args.summary.write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )

    print("PDM CURRENT DEFAULT-RISK FEATURES")
    print("=" * 72)
    print(f"As of date             : {observation_date}")
    print(f"Operational loans      : {len(loans):,}")
    print(f"Rows eligible/scored   : {len(rows):,}")
    print(f"Repayment-event rows   : {event_backed_loans:,}")
    print(f"Not-yet-due rows       : {not_yet_due_loans:,}")
    print(f"Missing-history rows   : {missing_history_loans:,}")
    print(f"Current-state proxies  : {proxy_backed_loans:,}")
    print(f"Skipped non-disbursed  : {skipped_not_disbursed:,}")
    print(f"Feature contract       : {len(EXPECTED_FEATURES)} / 20")
    print(f"Output                 : {args.output}")
    print(f"Summary                : {args.summary}")


if __name__ == "__main__":
    main()
