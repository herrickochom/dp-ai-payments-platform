#!/usr/bin/env python3
"""
PDMIS Synthetic Data Generator for Uganda PDM
=============================================

Generates a complete, internally consistent synthetic PDMIS data set:

    data/pdmis/
    ├── beneficiaries.json
    ├── households.json
    ├── business_plans.json
    ├── loans.json
    ├── repayments.json
    ├── saccos.json
    └── special_groups.json

Default development sample
--------------------------
1,000 beneficiary / loan cases across nine reporting regions. The regional
weights below are expressed per 100 cases and scale deterministically:

    Central          16
    Eastern          24
    North Eastern     8
    Northern         14
    North Western     8
    Western          14
    South             4
    South Western     6
    South Eastern     6
    -------------------
    Total           100

Important:
- These are synthetic reporting-region labels used by this project.
- Counts are absolute case counts for the default 100-case run, not percentages.
- For custom --count values, the same weighting is scaled deterministically.
- When --count >= 9, every configured reporting region receives at least one case.

Design goals
------------
1. Laptop-safe default population: 1,000 beneficiaries / 1,000 loans.
2. No NULL values.
3. No blank required strings.
4. Stable deterministic output for repeatable dbt tests.
5. Fixed standard PDM requested loan amount of UGX 1,000,000.
6. Multiple districts and regions for meaningful geographic analytics.
7. Monthly approval cohorts across the configured lifecycle window.
8. Referential consistency across beneficiary, household, SACCO,
   business-plan and loan records.
9. Preserve the current PDMIS JSON field contracts used by the platform.
10. Fail generation before writing files when validation fails.

Examples
--------
Normal development run:
    python services/payment-xml-generator/pdmis_generator.py

Smoke test:
    python services/payment-xml-generator/pdmis_generator.py --count 25

Heavier local integration test:
    python services/payment-xml-generator/pdmis_generator.py --count 250

Custom output directory:
    python services/payment-xml-generator/pdmis_generator.py \
        --output-root /tmp/pdmis-test
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import os
import random
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple


# =============================================================================
# Configuration
# =============================================================================

PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        "/home/hochom/projects/dp-ai-payments-platform",
    )
)

DATA_ROOT = Path(
    os.getenv(
        "DATA_ROOT",
        str(PROJECT_ROOT / "data"),
    )
)

DEFAULT_PDMIS_ROOT = DATA_ROOT / "pdmis"

DEFAULT_CASE_COUNT = 1000
REGIONAL_WEIGHT_BASE = 100
DEFAULT_RANDOM_SEED = 20260903
DEFAULT_START_DATE = date(2024, 1, 1)
DEFAULT_AS_OF_DATE = date(2026, 9, 8)

PDM_LOAN_AMOUNT_UGX = 1_000_000
BEHAVIOUR_PROFILES: Sequence[str] = (
    "STRONG", "STEADY", "VOLATILE", "DISTRESSED", "DEFAULT_PRONE"
)

# Absolute counts for the default 100-case development sample.
REGIONAL_CASE_COUNTS: Dict[str, int] = {
    "Central": 16,
    "Eastern": 24,
    "North Eastern": 8,
    "Northern": 14,
    "North Western": 8,
    "Western": 14,
    "South": 4,
    "South Western": 6,
    "South Eastern": 6,
}

if sum(REGIONAL_CASE_COUNTS.values()) != REGIONAL_WEIGHT_BASE:
    raise RuntimeError(
        "REGIONAL_CASE_COUNTS must total "
        f"{REGIONAL_WEIGHT_BASE}; got {sum(REGIONAL_CASE_COUNTS.values())}"
    )

PROJECT_TYPES: Sequence[str] = (
    "POULTRY",
    "CATTLE",
    "COFFEE",
    "MAIZE",
    "VEGETABLES",
    "FISH",
)

# 30 / 30 / 10 / 10 / 20 weighted cycle.
SPECIAL_GROUP_CYCLE: Sequence[str] = (
    "WOMEN",
    "WOMEN",
    "WOMEN",
    "YOUTH",
    "YOUTH",
    "YOUTH",
    "PWD",
    "ELDERLY",
    "GENERAL",
    "GENERAL",
)

SPECIAL_GROUPS: Sequence[Dict[str, Any]] = (
    {
        "group_code": "WOMEN",
        "group_name": "Women",
        "quota_percentage": 30.0,
        "description": (
            "Women entering the money economy under the "
            "Parish Development Model 30 percent quota."
        ),
    },
    {
        "group_code": "YOUTH",
        "group_name": "Youth",
        "quota_percentage": 30.0,
        "description": (
            "Young adults aged 18 to 35 accessing the "
            "Parish Development Model 30 percent quota."
        ),
    },
    {
        "group_code": "PWD",
        "group_name": "Persons with Disabilities",
        "quota_percentage": 10.0,
        "description": (
            "Persons with disabilities served through the "
            "Parish Development Model 10 percent quota."
        ),
    },
    {
        "group_code": "ELDERLY",
        "group_name": "Elderly Persons",
        "quota_percentage": 10.0,
        "description": (
            "Elderly persons served through the "
            "Parish Development Model 10 percent quota."
        ),
    },
    {
        "group_code": "GENERAL",
        "group_name": "General Community",
        "quota_percentage": 20.0,
        "description": (
            "Community members served through the remaining "
            "Parish Development Model quota."
        ),
    },
)

# ---------------------------------------------------------------------------
# Deterministic risk scenarios
# ---------------------------------------------------------------------------
# The consumption risk models only raise alerts when the source data actually
# contains the patterns they detect. These scenarios exercise every identity
# alert so the dashboards show realistic, non-zero risk:
#
# - SHARED_NIN_PAIRS: two beneficiaries enrolled with one national ID.
#   `identity_risk_band` becomes HIGH for both members of each pair.
# - UNVERIFIED_NIN_INDEXES: NINs that never completed verification (MEDIUM).
# - SUBSTITUTION_MODULUS (generator_common): a share of disbursements paid to
#   a third-party wallet, which payment facts flag as account substitution.
SHARED_NIN_PAIRS: Tuple[Tuple[int, int], ...] = (
    (4, 41),
    (17, 71),
    (23, 86),
)
UNVERIFIED_NIN_INDEXES: frozenset = frozenset({9, 30, 52, 68, 91})

# ---------------------------------------------------------------------------
# Deterministic money scenarios
# ---------------------------------------------------------------------------
# Every PDM loan requests the standard UGX 1,000,000 ceiling, but approvals,
# disbursements and repayments vary so totals, peer z-scores and repayment
# analytics are meaningful.
PARTIAL_DISBURSEMENT_MODULUS = 5
LOAN_INTEREST_RATE = 8.0
LOAN_TERM_MONTHS = 12
REPAYMENT_FREQUENCY = "MONTHLY"

FIRST_NAMES: Sequence[str] = (
    "Amina",
    "Akello",
    "Atim",
    "Auma",
    "Betty",
    "Charles",
    "David",
    "Esther",
    "Florence",
    "Grace",
    "Harriet",
    "Isaac",
    "Jane",
    "John",
    "Joseph",
    "Kevin",
    "Mary",
    "Moses",
    "Peter",
    "Rebecca",
    "Richard",
    "Rose",
    "Sarah",
    "Simon",
    "Stephen",
    "Agnes",
    "Andrew",
    "Beatrice",
    "Daniel",
    "Doreen",
    "Fred",
    "Gertrude",
    "Henry",
    "Irene",
    "James",
    "Janet",
    "Juliet",
    "Martin",
    "Mercy",
    "Patrick",
)

LAST_NAMES: Sequence[str] = (
    "Abalo",
    "Acen",
    "Akena",
    "Akello",
    "Auma",
    "Kato",
    "Kintu",
    "Lukwago",
    "Mugisha",
    "Mukasa",
    "Muwanga",
    "Nabirye",
    "Nakato",
    "Namuganza",
    "Nalwanga",
    "Nsubuga",
    "Obote",
    "Ochola",
    "Odongo",
    "Okello",
    "Opio",
    "Ssemanda",
    "Tumusiime",
    "Waiswa",
    "Wanyama",
    "Otim",
    "Ocen",
    "Amoding",
    "Ekiru",
    "Laker",
    "Anywar",
    "Ayaa",
    "Byaruhanga",
    "Turyasingura",
    "Mwesigwa",
    "Nankunda",
    "Namutebi",
    "Nabwire",
    "Wafula",
    "Mugerwa",
)


@dataclass(frozen=True)
class Location:
    code: str
    region: str
    district: str
    county: str
    sub_county: str
    parish: str
    village: str


# =============================================================================
# Synthetic geography
# =============================================================================
#
# These region labels are intentionally aligned with the project's reporting
# requirement. They are not intended to assert an official administrative
# regional classification.
#
# Each region has multiple districts so the dashboard can aggregate and drill
# meaningfully while the default development data set remains small.
# =============================================================================

PDM_LOCATIONS: Sequence[Location] = (
    # Central
    Location("KLA", "Central", "Kampala", "Kawempe County", "Kawempe Division", "Kawempe", "Kazo"),
    Location("WAK", "Central", "Wakiso", "Busiro County", "Nansana Municipality", "Nansana", "Nabweru"),
    Location("MKN", "Central", "Mukono", "Mukono County", "Mukono Municipality", "Goma", "Seeta"),
    Location("MPG", "Central", "Mpigi", "Mawokota County", "Mpigi Town Council", "Mpigi Central", "Kammengo"),

    # Eastern
    Location("KAM", "Eastern", "Kamuli", "Bugabula County", "Kamuli Municipality", "Kamuli Central", "Namwendwa"),
    Location("KUM", "Eastern", "Kumi", "Kumi County", "Kumi Municipality", "Kumi Central", "Boma"),
    Location("BUK", "Eastern", "Bukedea", "Bukedea County", "Bukedea Town Council", "Bukedea Central", "Emokor"),
    Location("MBA", "Eastern", "Mbale", "Bungokho County", "Mbale City", "Namakwekwe", "Namakwekwe"),
    Location("JIN", "Eastern", "Jinja", "Kagoma County", "Jinja City", "Walukuba", "Masese"),
    Location("IGG", "Eastern", "Iganga", "Kigulu County", "Iganga Municipality", "Iganga Central", "Nakavule"),
    Location("SRT", "Eastern", "Soroti", "Dakabela County", "Arapai", "Arapai", "Awoja"),
    Location("SRG", "Eastern", "Soroti", "Soroti County", "Gweri", "Gweri", "Aukot"),

    # North Eastern
    Location("MRT", "North Eastern", "Moroto", "Matheniko County", "Moroto Municipality", "Camp Swahili", "Camp Swahili"),
    Location("KTD", "North Eastern", "Kotido", "Jie County", "Kotido Municipality", "Kotido Central", "Kanawat"),
    Location("NPK", "North Eastern", "Nakapiripirit", "Chekwii County", "Nakapiripirit Town Council", "Nakapiripirit Central", "Namalu"),
    Location("NAP", "North Eastern", "Napak", "Bokora County", "Napak Town Council", "Napak Central", "Lokiteded"),

    # Northern
    Location("GBU", "Northern", "Gulu", "Aswa County", "Gulu City", "Laroo", "Pece"),
    Location("LRA", "Northern", "Lira", "Erute County", "Lira City", "Adyel", "Ojwina"),
    Location("KIT", "Northern", "Kitgum", "Chua County", "Kitgum Municipality", "Kitgum Central", "Pajimo"),
    Location("AGA", "Northern", "Agago", "Agago County", "Patongo Town Council", "Patongo Central", "Paimol"),

    # North Western
    Location("ARP", "North Western", "Arua", "Ayivu County", "Arua City", "Pajulu", "Anyafio"),
    Location("ADJ", "North Western", "Adjumani", "East Moyo County", "Adjumani Town Council", "Adjumani Central", "Cesiah"),
    Location("YBE", "North Western", "Yumbe", "Aringa County", "Yumbe Town Council", "Yumbe Central", "Kuru"),
    Location("NEB", "North Western", "Nebbi", "Padyere County", "Nebbi Municipality", "Nebbi Central", "Abindu"),

    # Western
    Location("HOM", "Western", "Hoima", "Bugahya County", "Hoima City", "Kahoora", "Bujumbura"),
    Location("MSD", "Western", "Masindi", "Bujenje County", "Masindi Municipality", "Masindi Central", "Kijura"),
    Location("FPO", "Western", "Kabarole", "Burahya County", "Fort Portal City", "Central Division", "Boma"),
    Location("KSE", "Western", "Kasese", "Busongora County", "Kasese Municipality", "Nyamwamba", "Kanyangeya"),

    # South
    Location("MSK", "South", "Masaka", "Bukoto County", "Masaka City", "Nyendo", "Nyendo"),
    Location("RAK", "South", "Rakai", "Kooki County", "Rakai Town Council", "Rakai Central", "Kibanda"),

    # South Western
    Location("MBR", "South Western", "Mbarara", "Kashari County", "Mbarara City", "Kakoba", "Nyamitanga"),
    Location("NTG", "South Western", "Ntungamo", "Rushenyi County", "Ntungamo Municipality", "Ntungamo Central", "Kafunjo"),
    Location("KBG", "South Western", "Kabale", "Ndorwa County", "Kabale Municipality", "Kabale Central", "Kigongi"),
    Location("RUK", "South Western", "Rukungiri", "Rujumbura County", "Rukungiri Municipality", "Rukungiri Central", "Kebisoni"),

    # South Eastern
    Location("TOR", "South Eastern", "Tororo", "West Budama County", "Tororo Municipality", "Tororo Central", "Osukuru"),
    Location("BUS", "South Eastern", "Busia", "Samia Bugwe County", "Busia Municipality", "Busia Central", "Masafu"),
    Location("BGR", "South Eastern", "Bugiri", "Bukooli County", "Bugiri Municipality", "Bugiri Central", "Nabukalu"),
    Location("MYG", "South Eastern", "Mayuge", "Bunya County", "Mayuge Town Council", "Mayuge Central", "Wairasa"),
)


def validate_location_hierarchy(locations: Sequence[Location]) -> None:
    """Ensure that every child geography has one deterministic parent."""
    hierarchy = (
        ("district", "region"),
        ("county", "district"),
        ("sub_county", "county"),
        ("parish", "sub_county"),
        ("village", "parish"),
    )

    for child_field, parent_field in hierarchy:
        parents_by_child: Dict[Tuple[str, str], set[str]] = {}
        for location in locations:
            # Include the district in the identity of lower-level names because
            # administrative names such as Central recur across Uganda.
            child_key = (location.district, getattr(location, child_field))
            parents_by_child.setdefault(child_key, set()).add(
                getattr(location, parent_field)
            )

        conflicts = {
            child: sorted(parents)
            for child, parents in parents_by_child.items()
            if len(parents) > 1
        }
        if conflicts:
            raise ValueError(
                f"Non-deterministic {parent_field} -> {child_field} hierarchy: "
                f"{conflicts}"
            )


# =============================================================================
# Generic helpers
# =============================================================================

def write_json(path: Path, payload: Any) -> None:
    validate_no_nulls_or_blanks(path.stem, payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def month_start(value: date) -> date:
    return value.replace(day=1)


def add_months(value: date, months: int) -> date:
    month_index = value.year * 12 + (value.month - 1) + months
    year = month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def reporting_months(start_date: date, as_of_date: date) -> List[date]:
    first = month_start(start_date)
    latest = month_start(as_of_date)
    count = (latest.year - first.year) * 12 + latest.month - first.month + 1
    return [add_months(first, offset) for offset in range(count)]


def deterministic_name(index: int) -> str:
    first = FIRST_NAMES[(index - 1) % len(FIRST_NAMES)]
    last = LAST_NAMES[
        (
            ((index - 1) // len(FIRST_NAMES))
            + index
            - 1
        )
        % len(LAST_NAMES)
    ]
    return f"{first} {last}"


def deterministic_phone(index: int) -> str:
    # Unique synthetic Uganda-like MSISDN for local development.
    return f"25677{index:07d}"


def deterministic_nin(index: int) -> str:
    # Synthetic test identifier only; it is not intended to represent
    # an actual Uganda NIN format or a real person.
    return f"CM{9_000_000 + index:07d}{100 + (index % 900):03d}X"


def make_beneficiary_token(nin: str) -> str:
    digest = hashlib.sha256(
        nin.encode("utf-8")
    ).hexdigest()
    return f"TOKEN-{digest[:16].upper()}"


def shared_nin_partner(index_one_based: int) -> int | None:
    """
    Return the paired case index when this case deliberately shares a NIN.

    Synthetic identity-risk scenario: both members of a pair enrolled with the
    same national ID, which the identity-alert model flags as HIGH risk.
    """
    for leader, follower in SHARED_NIN_PAIRS:
        if index_one_based == follower:
            return leader
    return None


def deterministic_date_of_birth(index_one_based: int) -> str:
    year = 1975 + (index_one_based * 7) % 29
    month = 1 + (index_one_based * 5) % 12
    day = 1 + (index_one_based * 11) % 28
    return f"{day:02d}/{month:02d}/{year:04d}"


def deterministic_gender(index_one_based: int) -> str:
    return "F" if index_one_based % 2 == 0 else "M"


def deterministic_alternative_phone(index_one_based: int) -> str:
    # Distinct secondary-contact MSISDN range so it never collides with a
    # primary wallet number.
    return f"25675{index_one_based:07d}"


def deterministic_email(name: str, index_one_based: int) -> str:
    handle = name.lower().replace(" ", ".")
    return f"{handle}.{index_one_based:04d}@pdm.example.ug"


def deterministic_created_at(index_one_based: int, day_offset: int = 0) -> str:
    moment = datetime(2026, 1, 1, 8, 0, 0) + timedelta(
        days=(index_one_based % 180) + day_offset,
        hours=index_one_based % 24,
    )
    return moment.isoformat() + "+03:00"


def dated_timestamp(value: date, hour: int = 9) -> str:
    return datetime(value.year, value.month, value.day, hour, 0, 0).isoformat() + "+03:00"


def sacco_id_for_location(location: Location) -> str:
    return f"SACCO-{location.code}-001"


def special_group_for_case(index_zero_based: int) -> str:
    return SPECIAL_GROUP_CYCLE[
        index_zero_based
        % len(SPECIAL_GROUP_CYCLE)
    ]


def project_type_for_case(index_zero_based: int) -> str:
    return PROJECT_TYPES[
        index_zero_based
        % len(PROJECT_TYPES)
    ]


def approval_date_for_case(
    index_zero_based: int,
    months: Sequence[date],
) -> date:
    cohort_month = months[
        index_zero_based
        % len(months)
    ]
    day = 5 + (
        index_zero_based
        % 20
    )
    return cohort_month.replace(day=day)


def application_date_from_approval(
    approval_date: date,
) -> date:
    return approval_date - timedelta(days=14)


def add_months_on_day(value: date, months: int) -> date:
    target = add_months(value, months)
    last_day = calendar.monthrange(target.year, target.month)[1]
    return target.replace(day=min(value.day, last_day))


def registration_date_for_case(
    index_zero_based: int,
    earliest_month: date,
) -> date:
    return earliest_month - timedelta(
        days=90
        + (index_zero_based % 60)
    )


def loan_status_for_case(
    index_one_based: int,
) -> str:
    """
    Deterministic local-development mix.

    Approximately:
      85% DISBURSED
      10% APPROVED
       5% REJECTED

    This is a business lifecycle status, not a fraud label.
    """
    slot = index_one_based % 20

    if slot == 0:
        return "REJECTED"

    if slot in {7, 14}:
        return "APPROVED"

    return "DISBURSED"


# =============================================================================
# Regional allocation
# =============================================================================

def scaled_regional_case_counts(
    count: int,
) -> Dict[str, int]:
    """
    Scale the default 100-case regional counts to an arbitrary sample size.

    For count >= number of configured regions:
    - every region receives at least one case;
    - remaining cases are allocated by largest-remainder weighting.

    For count < number of regions:
    - cases are assigned to the highest-weight regions first.
    """
    if count < 1:
        raise ValueError("count must be at least 1")

    regions = list(
        REGIONAL_CASE_COUNTS.keys()
    )

    if count < len(regions):
        ranked = sorted(
            regions,
            key=lambda region: (
                -REGIONAL_CASE_COUNTS[region],
                regions.index(region),
            ),
        )
        selected = set(
            ranked[:count]
        )
        return {
            region: (
                1
                if region in selected
                else 0
            )
            for region in regions
        }

    # Guarantee one case in each region.
    allocation = {
        region: 1
        for region in regions
    }

    remaining = count - len(regions)
    base_total = sum(
        REGIONAL_CASE_COUNTS.values()
    )

    raw_additions = {
        region: (
            REGIONAL_CASE_COUNTS[region]
            / base_total
        )
        * remaining
        for region in regions
    }

    floor_additions = {
        region: int(
            raw_additions[region]
        )
        for region in regions
    }

    for region in regions:
        allocation[region] += (
            floor_additions[region]
        )

    allocated = sum(
        allocation.values()
    )

    still_to_allocate = (
        count - allocated
    )

    remainder_order = sorted(
        regions,
        key=lambda region: (
            -(
                raw_additions[region]
                - floor_additions[region]
            ),
            -REGIONAL_CASE_COUNTS[region],
            regions.index(region),
        ),
    )

    for region in remainder_order[
        :still_to_allocate
    ]:
        allocation[region] += 1

    if sum(allocation.values()) != count:
        raise RuntimeError(
            "Regional allocation failed: "
            f"expected {count}, got "
            f"{sum(allocation.values())}"
        )

    return allocation


def locations_by_region() -> Dict[str, List[Location]]:
    grouped: Dict[
        str,
        List[Location],
    ] = {
        region: []
        for region
        in REGIONAL_CASE_COUNTS
    }

    for location in PDM_LOCATIONS:
        if location.region not in grouped:
            raise ValueError(
                "PDM location uses unconfigured "
                f"region: {location.region}"
            )

        grouped[
            location.region
        ].append(location)

    missing = [
        region
        for region, locations
        in grouped.items()
        if not locations
    ]

    if missing:
        raise ValueError(
            "No PDM locations configured for "
            f"regions: {missing}"
        )

    return grouped


def build_case_locations(
    count: int,
) -> List[Location]:
    """
    Build the deterministic beneficiary geography allocation.

    For the default 100 cases, the counts exactly match
    REGIONAL_CASE_COUNTS.
    """
    target_counts = (
        REGIONAL_CASE_COUNTS.copy()
        if count == REGIONAL_WEIGHT_BASE
        else scaled_regional_case_counts(
            count
        )
    )

    grouped = locations_by_region()

    allocations: List[
        Location
    ] = []

    for region in (
        REGIONAL_CASE_COUNTS.keys()
    ):
        region_target = (
            target_counts[region]
        )

        region_locations = (
            grouped[region]
        )

        for offset in range(
            region_target
        ):
            allocations.append(
                region_locations[
                    offset
                    % len(region_locations)
                ]
            )

    if len(allocations) != count:
        raise RuntimeError(
            "Location allocation failed: "
            f"expected {count}, got "
            f"{len(allocations)}"
        )

    return allocations


# =============================================================================
# PDMIS entity generation
# =============================================================================

def generate_saccos(
    loans: Sequence[Dict[str, Any]] | None = None,
    start_date: date = DEFAULT_START_DATE,
    as_of_date: date = DEFAULT_AS_OF_DATE,
) -> List[Dict[str, Any]]:
    records: List[
        Dict[str, Any]
    ] = []

    registration_base = start_date - timedelta(days=730)

    loans_by_sacco: Dict[str, List[Dict[str, Any]]] = {}
    for loan in loans or ():
        loans_by_sacco.setdefault(loan["sacco_id"], []).append(loan)

    official_titles = ("Chairperson", "Secretary", "Treasurer")

    for index, location in enumerate(
        PDM_LOCATIONS,
        start=1,
    ):
        sacco_id = sacco_id_for_location(location)
        sacco_loans = loans_by_sacco.get(sacco_id, [])

        total_funds_received = sum(
            loan["amount_approved"] for loan in sacco_loans
        )
        total_funds_disbursed = sum(
            loan["amount_disbursed"] for loan in sacco_loans
        )
        total_repayments = sum(loan["amount_repaid"] for loan in sacco_loans)

        records.append(
            {
                "sacco_id": sacco_id,
                "name": (
                    f"{location.parish} "
                    "PDM SACCO"
                ),
                "registration_number": (
                    f"{location.code}"
                    "-SACCO-001"
                ),
                "wendi_account": (
                    f"25679{index:07d}"
                ),
                "postbank_account": (
                    f"9010{index:08d}"
                ),
                "chairperson": (
                    f"{official_titles[0]} "
                    f"{deterministic_name(index * 3 + 1)}"
                ),
                "secretary": (
                    f"{official_titles[1]} "
                    f"{deterministic_name(index * 3 + 2)}"
                ),
                "treasurer": (
                    f"{official_titles[2]} "
                    f"{deterministic_name(index * 3 + 3)}"
                ),
                "office_address": (
                    f"{location.parish}, "
                    f"{location.sub_county}, "
                    f"{location.district}"
                ),
                "office_exists": True,
                "number_of_beneficiaries": (
                    len(sacco_loans)
                ),
                "total_funds_received": (
                    total_funds_received
                ),
                "total_funds_disbursed": (
                    total_funds_disbursed
                ),
                "total_repayments": (
                    total_repayments
                ),
                "parish": (
                    location.parish
                ),
                "village": (
                    location.village
                ),
                "sub_county": (
                    location.sub_county
                ),
                "county": (
                    location.county
                ),
                "district": (
                    location.district
                ),
                "region": (
                    location.region
                ),
                "created_at": (
                    deterministic_created_at(index, 4)
                ),
                "updated_at": (
                    deterministic_created_at(index, 5)
                ),
                "registration_date": (
                    registration_base
                    + timedelta(days=index)
                ).isoformat(),
                "created_at": dated_timestamp(registration_base + timedelta(days=index)),
                "updated_at": dated_timestamp(as_of_date, 17),
                "is_active": True,
            }
        )

    return records


def generate_beneficiaries(
    count: int,
    months: Sequence[date],
    case_locations: Sequence[
        Location
    ],
    as_of_date: date,
) -> List[Dict[str, Any]]:
    records: List[
        Dict[str, Any]
    ] = []

    for i in range(
        1,
        count + 1,
    ):
        index_zero_based = i - 1

        location = (
            case_locations[
                index_zero_based
            ]
        )

        nin_source_index = shared_nin_partner(i) or i
        nin = deterministic_nin(nin_source_index)
        name = deterministic_name(i)
        registration_date = registration_date_for_case(index_zero_based, months[0])

        records.append(
            {
                "beneficiary_id": (
                    f"BEN-{i:05d}"
                ),
                "nin": nin,
                "nin_hashed": (
                    hashlib.sha256(
                        nin.encode(
                            "utf-8"
                        )
                    ).hexdigest()
                ),
                "nin_verified": (
                    i not in UNVERIFIED_NIN_INDEXES
                ),
                "beneficiary_token": (
                    make_beneficiary_token(
                        nin
                    )
                ),
                "name": name,
                "date_of_birth": (
                    deterministic_date_of_birth(i)
                ),
                "gender": (
                    deterministic_gender(i)
                ),
                "phone": (
                    deterministic_phone(i)
                ),
                "alternative_phone": (
                    deterministic_alternative_phone(i)
                ),
                "email": (
                    deterministic_email(name, i)
                ),
                "phone_verified": True,
                "household_id": (
                    f"HH-{i:05d}"
                ),
                "village": (
                    location.village
                ),
                "parish": (
                    location.parish
                ),
                "sub_county": (
                    location.sub_county
                ),
                "county": (
                    location.county
                ),
                "district": (
                    location.district
                ),
                "region": (
                    location.region
                ),
                "special_group": (
                    special_group_for_case(
                        index_zero_based
                    )
                ),
                "special_group_code": (
                    special_group_for_case(
                        index_zero_based
                    )
                ),
                "created_at": (
                    dated_timestamp(registration_date, 8)
                ),
                "updated_at": (
                    dated_timestamp(as_of_date, 17)
                ),
                "registration_date": (
                    registration_date.isoformat()
                ),
                "is_active": True,
            }
        )

    return records


def generate_households(
    beneficiaries: Sequence[
        Dict[str, Any]
    ],
    rng: random.Random,
) -> List[Dict[str, Any]]:
    records: List[
        Dict[str, Any]
    ] = []

    for position, beneficiary in enumerate(
        beneficiaries,
        start=1,
    ):
        economic_status = (
            rng.choices(
                population=[
                    "LOW",
                    "MEDIUM",
                    "HIGH",
                ],
                weights=[
                    70,
                    25,
                    5,
                ],
                k=1,
            )[0]
        )

        adults_count = 1 + (position % 3)
        children_count = max(0, (position % 6) - adults_count % 2)
        food_security_status = ("SECURE", "MODERATE", "INSECURE")[position % 3]
        housing_type = ("PERMANENT", "SEMI_PERMANENT", "TEMPORARY")[position % 3]
        land_ownership = ("OWNED", "LEASED", "COMMUNAL")[position % 3]

        records.append(
            {
                "household_id": (
                    beneficiary[
                        "household_id"
                    ]
                ),
                "head_of_household": (
                    beneficiary["name"]
                ),
                "head_phone": (
                    beneficiary["alternative_phone"]
                ),
                "member_count": (
                    adults_count + children_count
                ),
                "adults_count": (
                    adults_count
                ),
                "children_count": (
                    children_count
                ),
                "food_security_status": (
                    food_security_status
                ),
                "housing_type": (
                    housing_type
                ),
                "land_ownership": (
                    land_ownership
                ),
                "village": (
                    beneficiary["village"]
                ),
                "parish": (
                    beneficiary["parish"]
                ),
                "sub_county": (
                    beneficiary[
                        "sub_county"
                    ]
                ),
                "county": (
                    beneficiary["county"]
                ),
                "district": (
                    beneficiary["district"]
                ),
                "region": (
                    beneficiary["region"]
                ),
                "economic_status": (
                    economic_status
                ),
                "created_at": (
                    beneficiary["created_at"]
                ),
                "updated_at": (
                    beneficiary["updated_at"]
                ),
                "registration_date": (
                    beneficiary[
                        "registration_date"
                    ]
                ),
            }
        )

    return records


def generate_loans(
    beneficiaries: Sequence[
        Dict[str, Any]
    ],
    months: Sequence[date],
    case_locations: Sequence[
        Location
    ],
    as_of_date: date,
) -> List[Dict[str, Any]]:
    records: List[
        Dict[str, Any]
    ] = []

    for i, beneficiary in enumerate(
        beneficiaries,
        start=1,
    ):
        index_zero_based = i - 1

        location = (
            case_locations[
                index_zero_based
            ]
        )

        status = (
            loan_status_for_case(i)
        )

        approval_date = (
            approval_date_for_case(
                index_zero_based,
                months,
            )
        )
        approval_date = min(approval_date, as_of_date - timedelta(days=2))
        if status == "APPROVED":
            # A still-pending approval is necessarily recent at the observation
            # date; do not leave two-year-old approvals artificially pending.
            approval_date = as_of_date - timedelta(days=2 + (i % 5))
        elif status == "DISBURSED" and approval_date + timedelta(days=8) > as_of_date:
            approval_date = as_of_date - timedelta(days=8)

        application_date = (
            application_date_from_approval(
                approval_date
            )
        )

        verification_date = approval_date + timedelta(days=2)
        disbursement_date = approval_date + timedelta(days=7)
        cashout_date = disbursement_date + timedelta(days=1)

        amount_approved = (
            PDM_LOAN_AMOUNT_UGX
            if status
            in {
                "APPROVED",
                "DISBURSED",
            }
            else 0
        )

        amount_disbursed = (
            PDM_LOAN_AMOUNT_UGX
            if status == "DISBURSED"
            else 0
        )

        # A deterministic share of disbursements is partial (kits released in
        # tranches), so disbursement totals and peer z-scores vary by parish.
        if (
            status == "DISBURSED"
            and i % PARTIAL_DISBURSEMENT_MODULUS == 0
        ):
            amount_disbursed = (
                PDM_LOAN_AMOUNT_UGX
                - 25_000
                - (i % 7) * 25_000
            )

        interest_charged = round(
            amount_disbursed * LOAN_INTEREST_RATE / 100.0,
            2,
        )

        first_repayment_date = add_months_on_day(disbursement_date, 1)
        last_repayment_date = add_months_on_day(
            disbursement_date, LOAN_TERM_MONTHS
        )

        records.append(
            {
                "loan_id": (
                    f"LOAN-{i:06d}"
                ),
                "beneficiary_id": (
                    beneficiary[
                        "beneficiary_id"
                    ]
                ),
                "sacco_id": (
                    sacco_id_for_location(
                        location
                    )
                ),
                "application_date": (
                    application_date
                    .isoformat()
                ),
                "approval_date": (
                    approval_date
                    .isoformat()
                ),
                "verification_date": verification_date.isoformat(),
                "disbursement_date": (
                    disbursement_date.isoformat()
                    if status == "DISBURSED" else "NOT_APPLICABLE"
                ),
                "cashout_date": (
                    cashout_date.isoformat()
                    if status == "DISBURSED" else "NOT_APPLICABLE"
                ),
                "as_of_date": as_of_date.isoformat(),
                "amount_requested": (
                    PDM_LOAN_AMOUNT_UGX
                ),
                "amount_approved": (
                    amount_approved
                ),
                "amount_disbursed": (
                    amount_disbursed
                ),
                "amount_repaid": (
                    0
                ),
                "principal_repaid": 0.0,
                "interest_rate": (
                    LOAN_INTEREST_RATE
                ),
                "interest_charged": (
                    interest_charged
                ),
                "interest_paid": (
                    0.0
                ),
                "loan_term_months": (
                    LOAN_TERM_MONTHS
                ),
                "repayment_frequency": (
                    REPAYMENT_FREQUENCY
                ),
                "first_repayment_date": (
                    first_repayment_date.isoformat()
                    if status == "DISBURSED" else "NOT_APPLICABLE"
                ),
                "last_repayment_date": (
                    last_repayment_date.isoformat()
                    if status == "DISBURSED" else "NOT_APPLICABLE"
                ),
                "scheduled_instalment": (
                    round((amount_disbursed + interest_charged) / LOAN_TERM_MONTHS, 2)
                    if status == "DISBURSED" else 0.0
                ),
                "principal_outstanding_balance": round(amount_disbursed, 2),
                "interest_outstanding_balance": round(interest_charged, 2),
                "outstanding_balance": round(amount_disbursed + interest_charged, 2),
                "principal_repayment_rate": 0.0,
                "contractual_repayment_progress": 0.0,
                "due_repayment_rate": 0.0,
                # Deprecated compatibility field. Its historical formula is
                # cumulative amount paid / scheduled amount due to date.
                "repayment_rate": 0.0,
                "repayment_status": "NOT_STARTED",
                "days_past_due": 0,
                "delinquency_bucket": "CURRENT",
                "loan_status": (
                    status
                ),
                "business_plan_id": (
                    f"BP-{i:05d}"
                ),
                "project_type": (
                    project_type_for_case(
                        index_zero_based
                    )
                ),
                "project_location": (
                    f"{location.sub_county}, "
                    f"{location.parish}, "
                    f"{location.district}"
                ),
                "created_at": (
                    dated_timestamp(application_date, 9)
                ),
                "updated_at": (
                    dated_timestamp(as_of_date, 17)
                ),
            }
        )

    return records


def generate_repayments(
    loans: Sequence[Dict[str, Any]],
    as_of_date: date,
) -> List[Dict[str, Any]]:
    """Generate deterministic monthly events and derive each loan's position."""
    events: List[Dict[str, Any]] = []

    for loan in loans:
        if loan["loan_status"] != "DISBURSED":
            continue

        serial = int(str(loan["loan_id"]).rsplit("-", 1)[-1])
        profile = BEHAVIOUR_PROFILES[(serial - 1) % len(BEHAVIOUR_PROFILES)]
        disbursement = date.fromisoformat(str(loan["disbursement_date"]))
        contractual_total = float(loan["amount_disbursed"]) + float(loan["interest_charged"])
        scheduled = contractual_total / int(loan["loan_term_months"])
        paid_total = 0.0
        scheduled_to_date = 0.0
        oldest_unpaid_due: date | None = None
        last_payment: date | None = None
        sequence = 0
        due_dates: List[date] = []

        for instalment in range(1, int(loan["loan_term_months"]) + 1):
            due_date = add_months_on_day(disbursement, instalment)
            if due_date > as_of_date:
                break
            scheduled_to_date += scheduled
            due_dates.append(due_date)

            days_late = 0
            multiplier = 1.0
            status = "SUCCESSFUL"
            if profile == "STRONG":
                multiplier = 1.05 if instalment % 4 == 0 else 1.0
            elif profile == "STEADY":
                days_late = (serial + instalment) % 4
                status = "LATE" if days_late else "SUCCESSFUL"
            elif profile == "VOLATILE":
                slot = (serial + instalment) % 5
                if slot == 0:
                    multiplier, status = 0.0, "FAILED"
                elif slot == 1:
                    multiplier, days_late, status = 0.5, 12, "PARTIAL"
                elif slot == 2:
                    days_late, status = 18, "LATE"
            elif profile == "DISTRESSED":
                if instalment % 3 == 0:
                    multiplier, status = 0.0, "MISSED"
                else:
                    multiplier, days_late, status = 0.45, 25 + instalment, "PARTIAL"
            elif profile == "DEFAULT_PRONE":
                if instalment > 2:
                    multiplier, status = 0.0, "MISSED"
                else:
                    days_late, status = 10, "LATE"

            intended_payment_date = due_date + timedelta(days=days_late)
            if multiplier > 0 and intended_payment_date > as_of_date:
                multiplier, status = 0.0, "MISSED"
            payment_date = intended_payment_date if multiplier > 0 else None
            amount = min(round(scheduled * multiplier, 2), round(contractual_total - paid_total, 2))
            if amount < 0:
                amount = 0.0
            if amount == 0 and oldest_unpaid_due is None:
                oldest_unpaid_due = due_date
            if amount > 0:
                paid_total += amount
                last_payment = payment_date

            principal_paid = round(amount * float(loan["amount_disbursed"]) / contractual_total, 2)
            interest_paid = round(amount - principal_paid, 2)
            cumulative_principal_paid = round(
                paid_total * float(loan["amount_disbursed"]) / contractual_total,
                2,
            )
            cumulative_interest_paid = round(
                paid_total * float(loan["interest_charged"]) / contractual_total,
                2,
            )
            principal_outstanding = round(
                max(0.0, float(loan["amount_disbursed"]) - cumulative_principal_paid),
                2,
            )
            interest_outstanding = round(
                max(0.0, float(loan["interest_charged"]) - cumulative_interest_paid),
                2,
            )
            event_days_late = (
                max(0, (payment_date - due_date).days)
                if payment_date else max(0, (as_of_date - due_date).days)
            )
            event_bucket = (
                "CURRENT" if event_days_late == 0 else
                "DPD_1_30" if event_days_late <= 30 else
                "DPD_31_60" if event_days_late <= 60 else
                "DPD_61_90" if event_days_late <= 90 else
                "DPD_90_PLUS"
            )

            sequence += 1
            events.append({
                "repayment_event_id": f"REPAY-{serial:06d}-{sequence:03d}",
                "loan_id": loan["loan_id"],
                "beneficiary_id": loan["beneficiary_id"],
                "sacco_id": loan["sacco_id"],
                "instalment_number": instalment,
                "event_sequence": sequence,
                "event_type": "REPAYMENT",
                "event_date": (payment_date or due_date).isoformat(),
                "repayment_due_date": due_date.isoformat(),
                "payment_date": payment_date.isoformat() if payment_date else "NOT_APPLICABLE",
                "scheduled_principal_amount": round(float(loan["amount_disbursed"]) / int(loan["loan_term_months"]), 2),
                "scheduled_interest_amount": round(float(loan["interest_charged"]) / int(loan["loan_term_months"]), 2),
                "scheduled_amount": round(scheduled, 2),
                "amount": amount,
                "principal_paid": principal_paid,
                "interest_paid": interest_paid,
                "days_late": event_days_late,
                "days_past_due": event_days_late,
                "delinquency_bucket": event_bucket,
                "payment_status": status,
                "principal_outstanding_balance": principal_outstanding,
                "interest_outstanding_balance": interest_outstanding,
                "outstanding_balance": round(max(0.0, contractual_total - paid_total), 2),
                "cumulative_amount_paid": round(paid_total, 2),
                "principal_repayment_rate": round(
                    cumulative_principal_paid / float(loan["amount_disbursed"]), 4
                ),
                "contractual_repayment_progress": round(
                    paid_total / contractual_total, 4
                ),
                "due_repayment_rate": round(
                    min(paid_total, scheduled_to_date) / scheduled_to_date, 4
                ),
                # Deprecated compatibility field; do not use as a canonical
                # financial metric because its denominator is due-to-date.
                "repayment_rate": round(paid_total / scheduled_to_date, 4),
                "currency": "UGX",
                "channel": ("WENDI" if instalment % 3 else "MOBILE_MONEY"),
                "as_of_date": as_of_date.isoformat(),
            })

            # Volatile borrowers can recover a failed instalment later. This is
            # a separate transaction rather than rewriting the failed attempt.
            if profile == "VOLATILE" and status == "FAILED" and due_date + timedelta(days=21) <= as_of_date:
                recovery_date = due_date + timedelta(days=21)
                recovery_amount = min(round(scheduled, 2), round(contractual_total - paid_total, 2))
                paid_total += recovery_amount
                last_payment = recovery_date
                cumulative_principal_paid = round(
                    paid_total * float(loan["amount_disbursed"]) / contractual_total,
                    2,
                )
                cumulative_interest_paid = round(
                    paid_total * float(loan["interest_charged"]) / contractual_total,
                    2,
                )
                principal_outstanding = round(
                    max(0.0, float(loan["amount_disbursed"]) - cumulative_principal_paid),
                    2,
                )
                interest_outstanding = round(
                    max(0.0, float(loan["interest_charged"]) - cumulative_interest_paid),
                    2,
                )
                if oldest_unpaid_due == due_date:
                    oldest_unpaid_due = None
                sequence += 1
                events.append({
                    **events[-1],
                    "repayment_event_id": f"REPAY-{serial:06d}-{sequence:03d}",
                    "event_sequence": sequence,
                    "event_type": "RECOVERY",
                    "event_date": recovery_date.isoformat(),
                    "payment_date": recovery_date.isoformat(),
                    "amount": recovery_amount,
                    "principal_paid": round(recovery_amount * float(loan["amount_disbursed"]) / contractual_total, 2),
                    "interest_paid": round(recovery_amount * float(loan["interest_charged"]) / contractual_total, 2),
                    "days_late": 21,
                    "days_past_due": 21,
                    "delinquency_bucket": "DPD_1_30",
                    "payment_status": "RECOVERED",
                    "principal_outstanding_balance": principal_outstanding,
                    "interest_outstanding_balance": interest_outstanding,
                    "outstanding_balance": round(max(0.0, contractual_total - paid_total), 2),
                    "cumulative_amount_paid": round(paid_total, 2),
                    "principal_repayment_rate": round(
                        cumulative_principal_paid / float(loan["amount_disbursed"]), 4
                    ),
                    "contractual_repayment_progress": round(
                        paid_total / contractual_total, 4
                    ),
                    "due_repayment_rate": round(
                        min(paid_total, scheduled_to_date) / scheduled_to_date, 4
                    ),
                    # Deprecated compatibility field; retained with its
                    # historical due-to-date denominator.
                    "repayment_rate": round(paid_total / scheduled_to_date, 4),
                })

        paid_total = min(paid_total, contractual_total)
        arrears = max(0.0, scheduled_to_date - paid_total)
        instalments_covered = int((paid_total + 0.01) // scheduled) if scheduled else 0
        oldest_contractual_due = (
            due_dates[instalments_covered]
            if instalments_covered < len(due_dates) else None
        )
        days_past_due = (
            max(0, (as_of_date - oldest_contractual_due).days)
            if arrears > 0.01 and oldest_contractual_due else 0
        )
        delinquency_bucket = (
            "CURRENT" if days_past_due == 0 else
            "DPD_1_30" if days_past_due <= 30 else
            "DPD_31_60" if days_past_due <= 60 else
            "DPD_61_90" if days_past_due <= 90 else
            "DPD_90_PLUS"
        )

        if scheduled_to_date == 0:
            repayment_status = "NOT_STARTED"
        elif paid_total >= contractual_total - 0.01:
            repayment_status = "REPAID"
        elif days_past_due > 90:
            repayment_status = "DEFAULTED"
        elif arrears > 0.01:
            repayment_status = "IN_ARREARS"
        else:
            repayment_status = "CURRENT"

        principal_ratio = float(loan["amount_disbursed"]) / contractual_total if contractual_total else 0.0
        loan["amount_repaid"] = round(paid_total, 2)
        loan["principal_repaid"] = round(paid_total * principal_ratio, 2)
        loan["interest_paid"] = round(paid_total * (1.0 - principal_ratio), 2)
        loan["principal_outstanding_balance"] = round(
            max(0.0, float(loan["amount_disbursed"]) - float(loan["principal_repaid"])), 2
        )
        loan["interest_outstanding_balance"] = round(
            max(0.0, float(loan["interest_charged"]) - float(loan["interest_paid"])), 2
        )
        loan["outstanding_balance"] = round(max(0.0, contractual_total - paid_total), 2)
        loan["principal_repayment_rate"] = round(
            float(loan["principal_repaid"]) / float(loan["amount_disbursed"]), 4
        )
        loan["contractual_repayment_progress"] = round(
            paid_total / contractual_total, 4
        )
        loan["due_repayment_rate"] = (
            round(min(paid_total, scheduled_to_date) / scheduled_to_date, 4)
            if scheduled_to_date else 0.0
        )
        # Deprecated compatibility field; retained with its historical
        # cumulative amount paid / scheduled amount due-to-date formula.
        loan["repayment_rate"] = round(paid_total / scheduled_to_date, 4) if scheduled_to_date else 0.0
        loan["repayment_status"] = repayment_status
        loan["days_past_due"] = days_past_due
        loan["delinquency_bucket"] = delinquency_bucket
        loan["last_payment_date"] = last_payment.isoformat() if last_payment else "NOT_APPLICABLE"

    return events


def generate_business_plans(
    beneficiaries: Sequence[
        Dict[str, Any]
    ],
    loans: Sequence[
        Dict[str, Any]
    ],
    rng: random.Random,
) -> List[Dict[str, Any]]:
    records: List[
        Dict[str, Any]
    ] = []

    loan_by_beneficiary = {
        loan["beneficiary_id"]: loan
        for loan in loans
    }

    for beneficiary in beneficiaries:
        loan = loan_by_beneficiary[
            beneficiary[
                "beneficiary_id"
            ]
        ]

        approval_date = (
            date.fromisoformat(
                loan["approval_date"]
            )
        )

        submission_date = (
            approval_date
            - timedelta(days=7)
        )

        expected_revenue = round(
            PDM_LOAN_AMOUNT_UGX
            * rng.uniform(
                1.20,
                2.00,
            ),
            2,
        )

        approval_status = (
            "REJECTED"
            if loan["loan_status"]
            == "REJECTED"
            else "APPROVED"
        )

        records.append(
            {
                "business_plan_id": (
                    loan[
                        "business_plan_id"
                    ]
                ),
                "loan_id": (
                    loan["loan_id"]
                ),
                "beneficiary_id": (
                    beneficiary[
                        "beneficiary_id"
                    ]
                ),
                "project_type": (
                    loan[
                        "project_type"
                    ]
                ),
                "description": (
                    f"{loan['project_type']} "
                    "PDM enterprise project"
                ),
                "location": (
                    f"{beneficiary['village']}, "
                    f"{beneficiary['sub_county']}, "
                    f"{beneficiary['parish']}, "
                    f"{beneficiary['district']}"
                ),
                "expected_revenue": (
                    expected_revenue
                ),
                "submission_date": (
                    submission_date
                    .isoformat()
                ),
                "approval_status": (
                    approval_status
                ),
            }
        )

    return records


# =============================================================================
# Validation
# =============================================================================

def validate_no_nulls_or_blanks(
    dataset_name: str,
    records: Sequence[
        Dict[str, Any]
    ],
) -> None:
    if not records:
        raise ValueError(
            f"{dataset_name}: "
            "no records generated"
        )

    for row_number, record in enumerate(
        records,
        start=1,
    ):
        for key, value in record.items():
            if value is None:
                raise ValueError(
                    f"{dataset_name}: "
                    f"row {row_number} "
                    f"field '{key}' "
                    "is NULL"
                )

            if (
                isinstance(
                    value,
                    str,
                )
                and not value.strip()
            ):
                raise ValueError(
                    f"{dataset_name}: "
                    f"row {row_number} "
                    f"field '{key}' "
                    "is blank"
                )


def validate_unique(
    dataset_name: str,
    records: Sequence[
        Dict[str, Any]
    ],
    fields: Iterable[str],
) -> None:
    for field in fields:
        values = [
            record[field]
            for record in records
        ]

        if len(values) != len(
            set(values)
        ):
            duplicates = [
                value
                for value, frequency
                in Counter(
                    values
                ).items()
                if frequency > 1
            ]

            raise ValueError(
                f"{dataset_name}: "
                f"duplicate {field}: "
                f"{duplicates[:10]}"
            )


def validate_regional_distribution(
    beneficiaries: Sequence[
        Dict[str, Any]
    ],
    expected_counts: Mapping[
        str,
        int,
    ],
) -> None:
    actual = Counter(
        beneficiary["region"]
        for beneficiary
        in beneficiaries
    )

    for region in (
        REGIONAL_CASE_COUNTS.keys()
    ):
        expected = (
            expected_counts[region]
        )
        observed = (
            actual.get(
                region,
                0,
            )
        )

        if observed != expected:
            raise ValueError(
                "Regional distribution "
                f"mismatch for {region}: "
                f"expected {expected}, "
                f"got {observed}"
            )


def validate_referential_integrity(
    beneficiaries: Sequence[
        Dict[str, Any]
    ],
    households: Sequence[
        Dict[str, Any]
    ],
    loans: Sequence[
        Dict[str, Any]
    ],
    business_plans: Sequence[
        Dict[str, Any]
    ],
    saccos: Sequence[
        Dict[str, Any]
    ],
    special_groups: Sequence[
        Dict[str, Any]
    ],
) -> None:
    beneficiary_ids = {
        row["beneficiary_id"]
        for row in beneficiaries
    }

    household_ids = {
        row["household_id"]
        for row in households
    }

    loan_ids = {
        row["loan_id"]
        for row in loans
    }

    business_plan_ids = {
        row["business_plan_id"]
        for row in business_plans
    }

    sacco_ids = {
        row["sacco_id"]
        for row in saccos
    }

    group_codes = {
        row["group_code"]
        for row in special_groups
    }

    for beneficiary in beneficiaries:
        if (
            beneficiary[
                "household_id"
            ]
            not in household_ids
        ):
            raise ValueError(
                "Beneficiary references "
                "missing household: "
                f"{beneficiary['household_id']}"
            )

        if (
            beneficiary[
                "special_group"
            ]
            not in group_codes
        ):
            raise ValueError(
                "Beneficiary references "
                "missing special group: "
                f"{beneficiary['special_group']}"
            )

    for loan in loans:
        if (
            loan[
                "beneficiary_id"
            ]
            not in beneficiary_ids
        ):
            raise ValueError(
                "Loan references "
                "missing beneficiary: "
                f"{loan['beneficiary_id']}"
            )

        if (
            loan["sacco_id"]
            not in sacco_ids
        ):
            raise ValueError(
                "Loan references "
                "missing SACCO: "
                f"{loan['sacco_id']}"
            )

        if (
            loan[
                "business_plan_id"
            ]
            not in business_plan_ids
        ):
            raise ValueError(
                "Loan references "
                "missing business plan: "
                f"{loan['business_plan_id']}"
            )

    for plan in business_plans:
        if (
            plan[
                "beneficiary_id"
            ]
            not in beneficiary_ids
        ):
            raise ValueError(
                "Business plan references "
                "missing beneficiary: "
                f"{plan['beneficiary_id']}"
            )

        if (
            plan["loan_id"]
            not in loan_ids
        ):
            raise ValueError(
                "Business plan references "
                "missing loan: "
                f"{plan['loan_id']}"
            )


def validate_pdm_loan_amounts(
    loans: Sequence[
        Dict[str, Any]
    ],
) -> None:
    as_of_dates = {str(loan["as_of_date"]) for loan in loans}
    if len(as_of_dates) != 1:
        raise ValueError(
            "Loans must contain exactly one authoritative as_of_date; "
            f"found {sorted(as_of_dates)}"
        )

    for loan in loans:
        if (
            loan[
                "amount_requested"
            ]
            != PDM_LOAN_AMOUNT_UGX
        ):
            raise ValueError(
                f"{loan['loan_id']}: "
                "amount_requested "
                "must be "
                f"{PDM_LOAN_AMOUNT_UGX}"
            )

        status = (
            loan["loan_status"]
        )

        if status == "DISBURSED":
            if (
                loan[
                    "amount_approved"
                ]
                != PDM_LOAN_AMOUNT_UGX
            ):
                raise ValueError(
                    f"{loan['loan_id']}: "
                    "DISBURSED loan "
                    "amount_approved "
                    "must be "
                    f"{PDM_LOAN_AMOUNT_UGX}"
                )

            if (
                loan[
                    "amount_disbursed"
                ]
                <= 0
                or loan[
                    "amount_disbursed"
                ]
                > loan[
                    "amount_approved"
                ]
            ):
                raise ValueError(
                    f"{loan['loan_id']}: "
                    "DISBURSED loan "
                    "amount_disbursed must be "
                    "greater than 0 and no more "
                    "than amount_approved"
                )

            lifecycle_dates = [
                date.fromisoformat(str(loan[field]))
                for field in (
                    "application_date",
                    "approval_date",
                    "verification_date",
                    "disbursement_date",
                    "cashout_date",
                )
            ]
            if (
                lifecycle_dates != sorted(lifecycle_dates)
                or lifecycle_dates[-1] > date.fromisoformat(str(loan["as_of_date"]))
            ):
                raise ValueError(
                    f"{loan['loan_id']}: lifecycle dates must be ordered "
                    "and no later than as_of_date"
                )

        elif status == "APPROVED":
            if (
                loan[
                    "amount_approved"
                ]
                != PDM_LOAN_AMOUNT_UGX
            ):
                raise ValueError(
                    f"{loan['loan_id']}: "
                    "APPROVED loan "
                    "amount_approved "
                    "must be "
                    f"{PDM_LOAN_AMOUNT_UGX}"
                )

            if (
                loan[
                    "amount_disbursed"
                ]
                != 0
            ):
                raise ValueError(
                    f"{loan['loan_id']}: "
                    "APPROVED loan "
                    "must have "
                    "amount_disbursed = 0"
                )
            if loan["disbursement_date"] != "NOT_APPLICABLE" or loan[
                "cashout_date"
            ] != "NOT_APPLICABLE":
                raise ValueError(
                    f"{loan['loan_id']}: APPROVED loan cannot have "
                    "disbursement_date or cashout_date"
                )

        elif status == "REJECTED":
            if (
                loan[
                    "amount_approved"
                ]
                != 0
            ):
                raise ValueError(
                    f"{loan['loan_id']}: "
                    "REJECTED loan "
                    "must have "
                    "amount_approved = 0"
                )

            if (
                loan[
                    "amount_disbursed"
                ]
                != 0
            ):
                raise ValueError(
                    f"{loan['loan_id']}: "
                    "REJECTED loan "
                    "must have "
                    "amount_disbursed = 0"
                )
            if loan["disbursement_date"] != "NOT_APPLICABLE" or loan[
                "cashout_date"
            ] != "NOT_APPLICABLE":
                raise ValueError(
                    f"{loan['loan_id']}: REJECTED loan cannot have "
                    "disbursement_date or cashout_date"
                )

        else:
            raise ValueError(
                f"{loan['loan_id']}: "
                "unexpected loan_status "
                f"{status}"
            )


def validate_all(
    beneficiaries: Sequence[
        Dict[str, Any]
    ],
    households: Sequence[
        Dict[str, Any]
    ],
    loans: Sequence[
        Dict[str, Any]
    ],
    business_plans: Sequence[
        Dict[str, Any]
    ],
    saccos: Sequence[
        Dict[str, Any]
    ],
    special_groups: Sequence[
        Dict[str, Any]
    ],
    expected_region_counts: Mapping[
        str,
        int,
    ],
) -> None:
    validate_location_hierarchy(PDM_LOCATIONS)

    datasets = {
        "beneficiaries": beneficiaries,
        "households": households,
        "loans": loans,
        "business_plans": business_plans,
        "saccos": saccos,
        "special_groups": special_groups,
    }

    for (
        dataset_name,
        records,
    ) in datasets.items():
        validate_no_nulls_or_blanks(
            dataset_name,
            records,
        )

    validate_unique(
        "beneficiaries",
        beneficiaries,
        (
            "beneficiary_id",
            "phone",
            "household_id",
        ),
    )

    validate_unique(
        "households",
        households,
        (
            "household_id",
        ),
    )

    validate_unique(
        "loans",
        loans,
        (
            "loan_id",
            "business_plan_id",
        ),
    )

    validate_unique(
        "business_plans",
        business_plans,
        (
            "business_plan_id",
            "loan_id",
        ),
    )

    validate_unique(
        "saccos",
        saccos,
        (
            "sacco_id",
            "registration_number",
            "wendi_account",
        ),
    )

    validate_unique(
        "special_groups",
        special_groups,
        (
            "group_code",
            "group_name",
        ),
    )

    validate_referential_integrity(
        beneficiaries,
        households,
        loans,
        business_plans,
        saccos,
        special_groups,
    )

    validate_pdm_loan_amounts(
        loans
    )

    validate_regional_distribution(
        beneficiaries,
        expected_region_counts,
    )


def validate_repayment_contract(
    loans: Sequence[Mapping[str, Any]],
    repayments: Sequence[Mapping[str, Any]],
    tolerance: float = 0.05,
) -> None:
    loans_by_id = {str(loan["loan_id"]): loan for loan in loans}
    latest_events: Dict[str, Mapping[str, Any]] = {}
    for event in repayments:
        loan_id = str(event["loan_id"])
        if loan_id not in loans_by_id:
            raise ValueError(f"Orphan repayment event: {event['repayment_event_id']}")
        if abs(
            float(event["amount"])
            - float(event["principal_paid"])
            - float(event["interest_paid"])
        ) > tolerance:
            raise ValueError(f"{event['repayment_event_id']}: payment allocation mismatch")
        if abs(
            float(event["outstanding_balance"])
            - float(event["principal_outstanding_balance"])
            - float(event["interest_outstanding_balance"])
        ) > tolerance:
            raise ValueError(
                f"{event['repayment_event_id']}: outstanding allocation mismatch"
            )
        contractual_total = (
            float(loans_by_id[loan_id]["amount_disbursed"])
            + float(loans_by_id[loan_id]["interest_charged"])
        )
        if abs(
            contractual_total
            - float(event["cumulative_amount_paid"])
            - float(event["outstanding_balance"])
        ) > tolerance:
            raise ValueError(
                f"{event['repayment_event_id']}: contractual balance does not reconcile"
            )
        principal_repaid = (
            float(loans_by_id[loan_id]["amount_disbursed"])
            - float(event["principal_outstanding_balance"])
        )
        expected_principal_rate = principal_repaid / float(
            loans_by_id[loan_id]["amount_disbursed"]
        )
        expected_contractual_progress = (
            float(event["cumulative_amount_paid"]) / contractual_total
        )
        scheduled_to_date = (
            contractual_total
            / int(loans_by_id[loan_id]["loan_term_months"])
            * int(event["instalment_number"])
        )
        expected_due_rate = min(
            float(event["cumulative_amount_paid"]), scheduled_to_date
        ) / scheduled_to_date
        for field, expected in (
            ("principal_repayment_rate", expected_principal_rate),
            ("contractual_repayment_progress", expected_contractual_progress),
            ("due_repayment_rate", expected_due_rate),
        ):
            if abs(float(event[field]) - expected) > 0.0001:
                raise ValueError(
                    f"{event['repayment_event_id']}: {field} formula mismatch"
                )

        previous = latest_events.get(loan_id)
        if previous is None or int(event["event_sequence"]) > int(
            previous["event_sequence"]
        ):
            latest_events[loan_id] = event

    for loan in loans:
        if abs(
            float(loan["amount_repaid"])
            - float(loan["principal_repaid"])
            - float(loan["interest_paid"])
        ) > tolerance:
            raise ValueError(f"{loan['loan_id']}: repaid allocation mismatch")
        if abs(
            float(loan["outstanding_balance"])
            - float(loan["principal_outstanding_balance"])
            - float(loan["interest_outstanding_balance"])
        ) > tolerance:
            raise ValueError(f"{loan['loan_id']}: outstanding allocation mismatch")
        if abs(
            float(loan["amount_disbursed"])
            + float(loan["interest_charged"])
            - float(loan["amount_repaid"])
            - float(loan["outstanding_balance"])
        ) > tolerance:
            raise ValueError(f"{loan['loan_id']}: contractual balance does not reconcile")

        latest_event = latest_events.get(str(loan["loan_id"]))
        if latest_event is not None:
            for balance_field in (
                "principal_outstanding_balance",
                "interest_outstanding_balance",
                "outstanding_balance",
            ):
                if abs(
                    float(latest_event[balance_field]) - float(loan[balance_field])
                ) > tolerance:
                    raise ValueError(
                        f"{loan['loan_id']}: latest repayment event "
                        f"{balance_field} does not match loan position"
                    )
            for rate_field in (
                "principal_repayment_rate",
                "contractual_repayment_progress",
                "due_repayment_rate",
            ):
                if abs(
                    float(latest_event[rate_field]) - float(loan[rate_field])
                ) > 0.0001:
                    raise ValueError(
                        f"{loan['loan_id']}: latest repayment event "
                        f"{rate_field} does not match loan position"
                    )


# =============================================================================
# Summary
# =============================================================================

def print_summary(
    output_root: Path,
    beneficiaries: Sequence[
        Dict[str, Any]
    ],
    households: Sequence[
        Dict[str, Any]
    ],
    loans: Sequence[
        Dict[str, Any]
    ],
    business_plans: Sequence[
        Dict[str, Any]
    ],
    saccos: Sequence[
        Dict[str, Any]
    ],
    special_groups: Sequence[
        Dict[str, Any]
    ],
    expected_region_counts: Mapping[
        str,
        int,
    ],
) -> None:
    district_counts = Counter(
        row["district"]
        for row in beneficiaries
    )

    region_counts = Counter(
        row["region"]
        for row in beneficiaries
    )

    special_group_counts = Counter(
        row["special_group"]
        for row in beneficiaries
    )

    loan_status_counts = Counter(
        row["loan_status"]
        for row in loans
    )

    approval_month_counts = Counter(
        row["approval_date"][:7]
        for row in loans
    )

    print("=" * 76)
    print(
        "PDMIS SYNTHETIC DATA "
        "GENERATION COMPLETE"
    )
    print("=" * 76)

    print(
        f"Output directory : "
        f"{output_root}"
    )
    print(
        f"Beneficiaries    : "
        f"{len(beneficiaries)}"
    )
    print(
        f"Households       : "
        f"{len(households)}"
    )
    print(
        f"Loans            : "
        f"{len(loans)}"
    )
    print(
        f"Business plans   : "
        f"{len(business_plans)}"
    )
    print(
        f"SACCOs           : "
        f"{len(saccos)}"
    )
    print(
        f"Special groups   : "
        f"{len(special_groups)}"
    )
    print(
        f"Districts covered: "
        f"{len(district_counts)}"
    )
    print(
        f"Regions covered  : "
        f"{len(region_counts)}"
    )
    print(
        f"PDM loan amount  : "
        f"UGX "
        f"{PDM_LOAN_AMOUNT_UGX:,}"
    )
    print(
        "Validation       : PASS"
    )
    print(
        "NULL/blank check : PASS"
    )
    print(
        "Referential check: PASS"
    )
    print(
        "Regional counts  : PASS"
    )

    print()
    print(
        "Regional case distribution:"
    )

    for region in (
        REGIONAL_CASE_COUNTS.keys()
    ):
        print(
            f"  {region:<16} "
            f"{region_counts.get(region, 0):>4}"
            f"  "
            f"(expected "
            f"{expected_region_counts[region]})"
        )

    print()
    print(
        "Loan status distribution:"
    )

    for key in sorted(
        loan_status_counts
    ):
        print(
            f"  {key:<12} "
            f"{loan_status_counts[key]:>4}"
        )

    print()
    print("Approval cohorts:")

    for key in sorted(
        approval_month_counts
    ):
        print(
            f"  {key:<12} "
            f"{approval_month_counts[key]:>4}"
        )

    print()
    print(
        "Special-group distribution:"
    )

    for key in (
        "WOMEN",
        "YOUTH",
        "PWD",
        "ELDERLY",
        "GENERAL",
    ):
        print(
            f"  {key:<12} "
            f"{special_group_counts[key]:>4}"
        )

    print("=" * 76)


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate complete synthetic "
            "Uganda PDMIS source data."
        )
    )

    parser.add_argument(
        "--count",
        "-c",
        type=int,
        default=DEFAULT_CASE_COUNT,
        help=(
            "Number of beneficiary / loan "
            "cases to generate "
            f"(default: {DEFAULT_CASE_COUNT})"
        ),
    )

    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=DEFAULT_START_DATE,
        help=f"Lifecycle start date (default: {DEFAULT_START_DATE.isoformat()})",
    )

    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=DEFAULT_AS_OF_DATE,
        help=f"Observation date (default: {DEFAULT_AS_OF_DATE.isoformat()})",
    )

    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing PDMIS JSON files before generation",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_RANDOM_SEED,
        help=(
            "Deterministic random seed "
            f"(default: {DEFAULT_RANDOM_SEED})"
        ),
    )

    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_PDMIS_ROOT,
        help=(
            "Directory receiving the seven "
            "PDMIS JSON files "
            f"(default: {DEFAULT_PDMIS_ROOT})"
        ),
    )

    args = parser.parse_args()

    if args.count < 1:
        parser.error(
            "--count must be at least 1"
        )
    if args.start_date >= args.as_of_date:
        parser.error("--start-date must be before --as-of-date")

    rng = random.Random(
        args.seed
    )

    months = reporting_months(args.start_date, args.as_of_date)

    expected_region_counts = (
        REGIONAL_CASE_COUNTS.copy()
        if args.count == sum(REGIONAL_CASE_COUNTS.values())
        else scaled_regional_case_counts(
            args.count
        )
    )

    case_locations = (
        build_case_locations(
            args.count
        )
    )

    special_groups = [
        dict(record)
        for record
        in SPECIAL_GROUPS
    ]

    beneficiaries = (
        generate_beneficiaries(
            args.count,
            months,
            case_locations,
            args.as_of_date,
        )
    )

    households = (
        generate_households(
            beneficiaries,
            rng,
        )
    )

    loans = generate_loans(
        beneficiaries,
        months,
        case_locations,
        args.as_of_date,
    )

    repayments = generate_repayments(loans, args.as_of_date)

    saccos = generate_saccos(loans, args.start_date, args.as_of_date)

    business_plans = (
        generate_business_plans(
            beneficiaries,
            loans,
            rng,
        )
    )

    validate_all(
        beneficiaries,
        households,
        loans,
        business_plans,
        saccos,
        special_groups,
        expected_region_counts,
    )
    validate_repayment_contract(loans, repayments)

    output_root = (
        args.output_root
    )

    if args.clean and output_root.exists():
        for existing in output_root.glob("*.json"):
            existing.unlink()

    write_json(
        output_root
        / "beneficiaries.json",
        beneficiaries,
    )

    write_json(
        output_root
        / "households.json",
        households,
    )

    write_json(
        output_root
        / "loans.json",
        loans,
    )

    validate_no_nulls_or_blanks("repayments", repayments)
    validate_unique("repayments", repayments, ("repayment_event_id",))
    write_json(output_root / "repayments.json", repayments)

    write_json(
        output_root
        / "business_plans.json",
        business_plans,
    )

    write_json(
        output_root
        / "saccos.json",
        saccos,
    )

    write_json(
        output_root
        / "special_groups.json",
        special_groups,
    )

    print_summary(
        output_root,
        beneficiaries,
        households,
        loans,
        business_plans,
        saccos,
        special_groups,
        expected_region_counts,
    )


if __name__ == "__main__":
    main()
