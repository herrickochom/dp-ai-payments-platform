#!/usr/bin/env python3
"""Generate synthetic historical PDM loan snapshots for ML proof-of-concept training.

This generator is intentionally separate from the operational PDMIS generator.
It reuses the same deterministic geography and project conventions but writes
historical, time-aware ML observations under ``data/pdmis_ml`` by default.

The primary output is JSON Lines so a laptop can generate tens of thousands of
training observations without holding the full dataset in memory.

Each row represents one loan at an observation date. Features contain only
information that would have been known on that date. The supervised target
``defaulted_within_90_days`` is derived from simulated repayment behaviour in
*the following* 90 days, avoiding target leakage by construction.

Example:
    python services/payment-xml-generator/pdm_history_generator.py

Smaller smoke test:
    python services/payment-xml-generator/pdm_history_generator.py --count 5000

Larger local POC:
    python services/payment-xml-generator/pdm_history_generator.py --count 50000
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Sequence, Tuple

from pdmis_generator import (
    DATA_ROOT,
    LOAN_INTEREST_RATE,
    LOAN_TERM_MONTHS,
    PDM_LOAN_AMOUNT_UGX,
    PDM_LOCATIONS,
    PROJECT_TYPES,
    SPECIAL_GROUP_CYCLE,
    Location,
    add_months,
    build_case_locations,
    month_start,
    project_type_for_case,
    sacco_id_for_location,
    special_group_for_case,
)


DEFAULT_HISTORY_COUNT = 50_000
DEFAULT_HISTORY_MONTHS = 36
DEFAULT_SEED = 20260908
DEFAULT_OUTPUT_ROOT = Path(
    os.getenv("PDM_ML_DATA_ROOT", str(DATA_ROOT / "pdmis_ml"))
)

SNAPSHOT_FILE = "default_risk_training_snapshots.jsonl"
SUMMARY_FILE = "default_risk_training_summary.json"


@dataclass(frozen=True)
class BehaviourProfile:
    name: str
    weight: int
    base_payment_factor: float
    volatility: float
    miss_probability: float
    failure_probability: float
    cure_probability: float


# Deliberately varied repayment archetypes. These are synthetic assumptions
# for a POC; they are not estimates of real PDM beneficiary behaviour.
BEHAVIOUR_PROFILES: Sequence[BehaviourProfile] = (
    BehaviourProfile("STRONG", 42, 1.05, 0.07, 0.02, 0.01, 0.90),
    BehaviourProfile("STEADY", 28, 0.95, 0.10, 0.05, 0.02, 0.80),
    BehaviourProfile("VOLATILE", 14, 0.80, 0.25, 0.18, 0.08, 0.55),
    BehaviourProfile("DISTRESSED", 10, 0.45, 0.22, 0.40, 0.18, 0.30),
    BehaviourProfile("DEFAULT_PRONE", 6, 0.20, 0.18, 0.68, 0.30, 0.12),
)

# Small synthetic geographic stress adjustments. They exist only so the POC
# has learnable cross-sectional structure; they are not real risk assessments.
REGION_STRESS: Dict[str, float] = {
    "Central": -0.05,
    "Eastern": 0.02,
    "North Eastern": 0.18,
    "Northern": 0.08,
    "North Western": 0.05,
    "Western": -0.02,
    "South": -0.03,
    "South Western": -0.04,
    "South Eastern": 0.01,
}


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def choose_profile(rng: random.Random) -> BehaviourProfile:
    return rng.choices(
        BEHAVIOUR_PROFILES,
        weights=[profile.weight for profile in BEHAVIOUR_PROFILES],
        k=1,
    )[0]


def days_between_months(start: date, end: date) -> int:
    return max(0, (end - start).days)


def months_between(start: date, end: date) -> int:
    return max(0, (end.year - start.year) * 12 + end.month - start.month)


def historical_approval_months(history_months: int) -> List[date]:
    if history_months < 12:
        raise ValueError("--history-months must be at least 12")
    latest = add_months(month_start(date.today()), -4)
    first = add_months(latest, -(history_months - 1))
    return [add_months(first, offset) for offset in range(history_months)]


def approval_date_for_history(index_zero_based: int, months: Sequence[date]) -> date:
    cohort = months[index_zero_based % len(months)]
    return cohort.replace(day=3 + ((index_zero_based * 7) % 23))


def simulate_monthly_repayments(
    *,
    rng: random.Random,
    principal: int,
    profile: BehaviourProfile,
    location: Location,
    case_index: int,
    months_to_simulate: int,
    account_substituted: int,
    identity_alert_count: int,
) -> Tuple[List[int], List[int]]:
    """Return monthly successful repayment amounts and payment failure flags."""
    scheduled = principal / LOAN_TERM_MONTHS
    region_stress = REGION_STRESS.get(location.region, 0.0)

    case_stress = (
        region_stress
        + (0.06 if account_substituted else 0.0)
        + min(identity_alert_count, 2) * 0.03
        + ((case_index % 17) - 8) / 250.0
    )

    miss_probability = clamp(profile.miss_probability + case_stress, 0.0, 0.92)
    failure_probability = clamp(
        profile.failure_probability + max(0.0, case_stress) * 0.35,
        0.0,
        0.65,
    )

    repayments: List[int] = []
    failures: List[int] = []
    arrears = 0.0

    for month_no in range(1, months_to_simulate + 1):
        failure = 1 if rng.random() < failure_probability else 0
        miss = failure == 1 or rng.random() < miss_probability

        if miss:
            payment = 0
            arrears += scheduled
        else:
            noise = rng.gauss(0.0, profile.volatility)
            factor = clamp(profile.base_payment_factor + noise - case_stress, 0.10, 1.45)
            payment = scheduled * factor

            if arrears > 0 and rng.random() < profile.cure_probability:
                cure_amount = min(arrears, scheduled * rng.uniform(0.25, 1.0))
                payment += cure_amount
                arrears = max(0.0, arrears - cure_amount)

        # Round to realistic thousand-shilling increments.
        payment_int = max(0, int(round(payment / 1000.0) * 1000))
        repayments.append(payment_int)
        failures.append(failure)

    return repayments, failures


def delinquency_state(
    repayments: Sequence[int],
    scheduled_monthly: float,
    months_elapsed: int,
) -> Tuple[float, int, int, float]:
    """Compute amount repaid, missed instalments, days past due and arrears."""
    paid = float(sum(repayments[:months_elapsed]))
    expected = scheduled_monthly * months_elapsed
    arrears = max(0.0, expected - paid)
    missed_equivalent = int(math.floor(arrears / scheduled_monthly + 1e-9))
    days_past_due = min(365, missed_equivalent * 30)
    return paid, missed_equivalent, days_past_due, arrears


def build_snapshot(
    index_one_based: int,
    location: Location,
    approval_months: Sequence[date],
    rng: random.Random,
) -> Dict[str, object]:
    idx = index_one_based - 1
    approval_date = approval_date_for_history(idx, approval_months)
    disbursement_date = approval_date + timedelta(days=7 + (index_one_based % 5))

    # Observe loans after 3-9 full repayment months. This leaves enough history
    # for features and enough future horizon for the 90-day target.
    observation_month = 3 + (index_one_based % 7)
    observation_date = add_months(disbursement_date.replace(day=1), observation_month)
    observation_date = observation_date.replace(day=28)

    principal = PDM_LOAN_AMOUNT_UGX
    if index_one_based % 7 == 0:
        principal -= 50_000
    elif index_one_based % 13 == 0:
        principal -= 100_000

    profile = choose_profile(rng)

    # Synthetic assurance signals, deterministic enough to reproduce while
    # remaining independent of the future target calculation.
    account_substituted = 1 if index_one_based % 12 == 0 else 0
    shared_identity = 1 if index_one_based % 29 == 0 else 0
    nin_unverified = 1 if index_one_based % 23 == 0 else 0
    identity_alert_count = shared_identity + nin_unverified

    total_months_needed = observation_month + 3
    repayments, failures = simulate_monthly_repayments(
        rng=rng,
        principal=principal,
        profile=profile,
        location=location,
        case_index=index_one_based,
        months_to_simulate=total_months_needed,
        account_substituted=account_substituted,
        identity_alert_count=identity_alert_count,
    )

    scheduled_monthly = principal / LOAN_TERM_MONTHS
    paid_at_obs, missed_at_obs, dpd_at_obs, arrears_at_obs = delinquency_state(
        repayments,
        scheduled_monthly,
        observation_month,
    )

    outstanding_at_obs = max(0.0, principal - paid_at_obs)
    repayment_rate_at_obs = 0.0 if principal == 0 else paid_at_obs / principal
    failures_at_obs = sum(failures[:observation_month])
    zero_payment_months = sum(1 for value in repayments[:observation_month] if value == 0)

    # Trend uses the most recent three known repayments versus the previous
    # known three. Positive means improving, negative means deteriorating.
    recent = repayments[max(0, observation_month - 3):observation_month]
    previous = repayments[max(0, observation_month - 6):max(0, observation_month - 3)]
    recent_avg = sum(recent) / len(recent) if recent else 0.0
    previous_avg = sum(previous) / len(previous) if previous else recent_avg
    repayment_trend_3m = 0.0
    if scheduled_monthly > 0:
        repayment_trend_3m = (recent_avg - previous_avg) / scheduled_monthly

    # Compute state 90 days later using future payments that are NOT exposed as
    # features. The label is 1 when the loan is seriously delinquent by then.
    future_month = observation_month + 3
    _, missed_future, dpd_future, arrears_future = delinquency_state(
        repayments,
        scheduled_monthly,
        future_month,
    )

    defaulted_within_90_days = int(
        dpd_future >= 90
        or arrears_future >= scheduled_monthly * 3.0
        or missed_future >= 3
    )

    # Current business context features that would be available at observation.
    household_economic_score = (index_one_based * 7) % 3
    project_type = project_type_for_case(idx)
    special_group = special_group_for_case(idx)

    interest_charged = round(principal * LOAN_INTEREST_RATE / 100.0, 2)

    return {
        "snapshot_id": f"SNAP-{index_one_based:07d}",
        "loan_id": f"HLOAN-{index_one_based:07d}",
        "beneficiary_id": f"HBEN-{index_one_based:07d}",
        "sacco_id": sacco_id_for_location(location),
        "observation_date": observation_date.isoformat(),
        "approval_date": approval_date.isoformat(),
        "disbursement_date": disbursement_date.isoformat(),
        "loan_age_days": days_between_months(disbursement_date, observation_date),
        "months_since_disbursement": months_between(disbursement_date, observation_date),
        "region": location.region,
        "district": location.district,
        "county": location.county,
        "sub_county": location.sub_county,
        "parish": location.parish,
        "village": location.village,
        "project_type": project_type,
        "special_group": special_group,
        "amount_approved": principal,
        "amount_disbursed": principal,
        "interest_rate": LOAN_INTEREST_RATE,
        "interest_charged": interest_charged,
        "loan_term_months": LOAN_TERM_MONTHS,
        "scheduled_monthly_repayment": int(round(scheduled_monthly)),
        "amount_repaid_as_of_observation": int(round(paid_at_obs)),
        "outstanding_amount_as_of_observation": int(round(outstanding_at_obs)),
        "repayment_rate_as_of_observation": round(repayment_rate_at_obs, 6),
        "arrears_amount_as_of_observation": int(round(arrears_at_obs)),
        "missed_instalment_count": missed_at_obs,
        "zero_payment_month_count": zero_payment_months,
        "days_past_due": dpd_at_obs,
        "payment_failure_count": failures_at_obs,
        "repayment_trend_3m": round(repayment_trend_3m, 6),
        "account_substituted": account_substituted,
        "shared_identity_alert": shared_identity,
        "nin_unverified": nin_unverified,
        "identity_alert_count": identity_alert_count,
        "household_economic_score": household_economic_score,
        # Synthetic audit fields. Keep these for POC validation but EXCLUDE
        # behaviour_profile from model features; it directly controls outcomes.
        "synthetic_behaviour_profile": profile.name,
        "target_horizon_days": 90,
        "defaulted_within_90_days": defaulted_within_90_days,
    }


def iter_snapshots(count: int, history_months: int, seed: int) -> Iterator[Dict[str, object]]:
    if count < 100:
        raise ValueError("--count must be at least 100 for a useful POC")

    approval_months = historical_approval_months(history_months)
    locations = build_case_locations(count)

    for index_one_based, location in enumerate(locations, start=1):
        # Per-record RNG gives reproducibility independent of output streaming.
        row_rng = random.Random(seed + index_one_based * 10_007)
        yield build_snapshot(index_one_based, location, approval_months, row_rng)


def write_jsonl(path: Path, rows: Iterable[Dict[str, object]]) -> Dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)

    row_count = 0
    default_count = 0
    districts = set()
    regions = set()
    profile_counts: Dict[str, int] = {}

    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            row_count += 1
            default_count += int(row["defaulted_within_90_days"])
            districts.add(str(row["district"]))
            regions.add(str(row["region"]))
            profile = str(row["synthetic_behaviour_profile"])
            profile_counts[profile] = profile_counts.get(profile, 0) + 1

    default_rate = default_count / row_count if row_count else 0.0
    return {
        "rows": row_count,
        "defaults": default_count,
        "non_defaults": row_count - default_count,
        "default_rate": round(default_rate, 6),
        "district_count": len(districts),
        "region_count": len(regions),
        "synthetic_behaviour_profiles": profile_counts,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=DEFAULT_HISTORY_COUNT)
    parser.add_argument("--history-months", type=int, default=DEFAULT_HISTORY_MONTHS)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Output directory. Defaults to data/pdmis_ml.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_root = args.output_root.resolve()
    snapshot_path = output_root / SNAPSHOT_FILE
    summary_path = output_root / SUMMARY_FILE

    summary = write_jsonl(
        snapshot_path,
        iter_snapshots(args.count, args.history_months, args.seed),
    )
    summary.update(
        {
            "seed": args.seed,
            "history_months": args.history_months,
            "target": "defaulted_within_90_days",
            "target_definition": (
                "Serious delinquency within the following 90 days: "
                ">=90 days past due, >=3 scheduled instalments in arrears, "
                "or >=3 missed-equivalent instalments."
            ),
            "snapshot_file": str(snapshot_path),
            "synthetic_only": True,
        }
    )

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print(f"Generated {summary['rows']:,} ML training snapshots")
    print(f"Output: {snapshot_path}")
    print(
        "Target distribution: "
        f"defaults={summary['defaults']:,} "
        f"non_defaults={summary['non_defaults']:,} "
        f"rate={summary['default_rate']:.2%}"
    )
    print(
        f"Coverage: {summary['region_count']} regions, "
        f"{summary['district_count']} districts"
    )
    print(f"Summary: {summary_path}")


if __name__ == "__main__":
    main()
