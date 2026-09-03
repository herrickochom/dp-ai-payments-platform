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
    ├── saccos.json
    └── special_groups.json

Default laptop-safe development sample
--------------------------------------
100 beneficiary / loan cases across nine reporting regions:

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
1. Laptop-safe default population: 100 beneficiaries / 100 loans.
2. No NULL values.
3. No blank required strings.
4. Stable deterministic output for repeatable dbt tests.
5. Fixed standard PDM requested loan amount of UGX 1,000,000.
6. Multiple districts and regions for meaningful geographic analytics.
7. Six approval cohorts for month-on-month dashboard metrics.
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
import hashlib
import json
import os
import random
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence


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

DEFAULT_CASE_COUNT = 100
DEFAULT_RANDOM_SEED = 20260903

PDM_LOAN_AMOUNT_UGX = 1_000_000
REPORTING_MONTH_COUNT = 6

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

if sum(REGIONAL_CASE_COUNTS.values()) != DEFAULT_CASE_COUNT:
    raise RuntimeError(
        "REGIONAL_CASE_COUNTS must total "
        f"{DEFAULT_CASE_COUNT}; got {sum(REGIONAL_CASE_COUNTS.values())}"
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
    },
    {
        "group_code": "YOUTH",
        "group_name": "Youth",
        "quota_percentage": 30.0,
    },
    {
        "group_code": "PWD",
        "group_name": "Persons with Disabilities",
        "quota_percentage": 10.0,
    },
    {
        "group_code": "ELDERLY",
        "group_name": "Elderly Persons",
        "quota_percentage": 10.0,
    },
    {
        "group_code": "GENERAL",
        "group_name": "General Community",
        "quota_percentage": 20.0,
    },
)

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
    Location("KLA", "Central", "Kampala", "Kawempe Division", "Kawempe", "Kazo"),
    Location("WAK", "Central", "Wakiso", "Nansana Municipality", "Nansana", "Nabweru"),
    Location("MKN", "Central", "Mukono", "Mukono Municipality", "Goma", "Seeta"),
    Location("MPG", "Central", "Mpigi", "Mpigi Town Council", "Mpigi Central", "Kammengo"),

    # Eastern
    Location("KAM", "Eastern", "Kamuli", "Kamuli Municipality", "Kamuli Central", "Namwendwa"),
    Location("KUM", "Eastern", "Kumi", "Kumi Municipality", "Kumi Central", "Boma"),
    Location("BUK", "Eastern", "Bukedea", "Bukedea Town Council", "Bukedea Central", "Emokor"),
    Location("MBA", "Eastern", "Mbale", "Mbale City", "Namakwekwe", "Namakwekwe"),
    Location("JIN", "Eastern", "Jinja", "Jinja City", "Walukuba", "Masese"),
    Location("IGG", "Eastern", "Iganga", "Iganga Municipality", "Iganga Central", "Nakavule"),

    # North Eastern
    Location("MRT", "North Eastern", "Moroto", "Moroto Municipality", "Camp Swahili", "Camp Swahili"),
    Location("KTD", "North Eastern", "Kotido", "Kotido Municipality", "Kotido Central", "Kanawat"),
    Location("NPK", "North Eastern", "Nakapiripirit", "Nakapiripirit Town Council", "Nakapiripirit Central", "Namalu"),
    Location("NAP", "North Eastern", "Napak", "Napak Town Council", "Napak Central", "Lokiteded"),

    # Northern
    Location("GBU", "Northern", "Gulu", "Gulu City", "Laroo", "Pece"),
    Location("LRA", "Northern", "Lira", "Lira City", "Adyel", "Ojwina"),
    Location("KIT", "Northern", "Kitgum", "Kitgum Municipality", "Kitgum Central", "Pajimo"),
    Location("AGA", "Northern", "Agago", "Patongo Town Council", "Patongo Central", "Paimol"),

    # North Western
    Location("ARP", "North Western", "Arua", "Arua City", "Pajulu", "Anyafio"),
    Location("ADJ", "North Western", "Adjumani", "Adjumani Town Council", "Adjumani Central", "Cesiah"),
    Location("YBE", "North Western", "Yumbe", "Yumbe Town Council", "Yumbe Central", "Kuru"),
    Location("NEB", "North Western", "Nebbi", "Nebbi Municipality", "Nebbi Central", "Abindu"),

    # Western
    Location("HOM", "Western", "Hoima", "Hoima City", "Kahoora", "Bujumbura"),
    Location("MSD", "Western", "Masindi", "Masindi Municipality", "Masindi Central", "Kijura"),
    Location("FPO", "Western", "Kabarole", "Fort Portal City", "Central Division", "Boma"),
    Location("KSE", "Western", "Kasese", "Kasese Municipality", "Nyamwamba", "Kanyangeya"),

    # South
    Location("MSK", "South", "Masaka", "Masaka City", "Nyendo", "Nyendo"),
    Location("RAK", "South", "Rakai", "Rakai Town Council", "Rakai Central", "Kibanda"),

    # South Western
    Location("MBR", "South Western", "Mbarara", "Mbarara City", "Kakoba", "Nyamitanga"),
    Location("NTG", "South Western", "Ntungamo", "Ntungamo Municipality", "Ntungamo Central", "Kafunjo"),
    Location("KBG", "South Western", "Kabale", "Kabale Municipality", "Kabale Central", "Kigongi"),
    Location("RUK", "South Western", "Rukungiri", "Rukungiri Municipality", "Rukungiri Central", "Kebisoni"),

    # South Eastern
    Location("TOR", "South Eastern", "Tororo", "Tororo Municipality", "Tororo Central", "Osukuru"),
    Location("BUS", "South Eastern", "Busia", "Busia Municipality", "Busia Central", "Masafu"),
    Location("BGR", "South Eastern", "Bugiri", "Bugiri Municipality", "Bugiri Central", "Nabukalu"),
    Location("MYG", "South Eastern", "Mayuge", "Mayuge Town Council", "Mayuge Central", "Wairasa"),
)


# =============================================================================
# Generic helpers
# =============================================================================

def write_json(path: Path, payload: Any) -> None:
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


def reporting_months(count: int = REPORTING_MONTH_COUNT) -> List[date]:
    latest = month_start(date.today())
    first = add_months(latest, -(count - 1))
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
        if count == DEFAULT_CASE_COUNT
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

def generate_saccos() -> List[Dict[str, Any]]:
    records: List[
        Dict[str, Any]
    ] = []

    registration_base = (
        date.today()
        - timedelta(days=730)
    )

    for index, location in enumerate(
        PDM_LOCATIONS,
        start=1,
    ):
        records.append(
            {
                "sacco_id": (
                    sacco_id_for_location(
                        location
                    )
                ),
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
                "parish": (
                    location.parish
                ),
                "sub_county": (
                    location.sub_county
                ),
                "district": (
                    location.district
                ),
                "region": (
                    location.region
                ),
                "registration_date": (
                    registration_base
                    + timedelta(days=index)
                ).isoformat(),
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

        nin = deterministic_nin(i)

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
                "nin_verified": True,
                "beneficiary_token": (
                    make_beneficiary_token(
                        nin
                    )
                ),
                "name": (
                    deterministic_name(i)
                ),
                "phone": (
                    deterministic_phone(i)
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
                "registration_date": (
                    registration_date_for_case(
                        index_zero_based,
                        months[0],
                    ).isoformat()
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

    for beneficiary in beneficiaries:
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
                "member_count": (
                    rng.randint(2, 8)
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
                "district": (
                    beneficiary["district"]
                ),
                "region": (
                    beneficiary["region"]
                ),
                "economic_status": (
                    economic_status
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

        application_date = (
            application_date_from_approval(
                approval_date
            )
        )

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
                "amount_requested": (
                    PDM_LOAN_AMOUNT_UGX
                ),
                "amount_approved": (
                    amount_approved
                ),
                "amount_disbursed": (
                    amount_disbursed
                ),
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
            }
        )

    return records


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
                != PDM_LOAN_AMOUNT_UGX
            ):
                raise ValueError(
                    f"{loan['loan_id']}: "
                    "DISBURSED loan "
                    "amount_disbursed "
                    "must be "
                    f"{PDM_LOAN_AMOUNT_UGX}"
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
            "nin",
            "nin_hashed",
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
            "Directory receiving the six "
            "PDMIS JSON files "
            f"(default: {DEFAULT_PDMIS_ROOT})"
        ),
    )

    args = parser.parse_args()

    if args.count < 1:
        parser.error(
            "--count must be at least 1"
        )

    rng = random.Random(
        args.seed
    )

    months = reporting_months()

    expected_region_counts = (
        REGIONAL_CASE_COUNTS.copy()
        if args.count
        == DEFAULT_CASE_COUNT
        else scaled_regional_case_counts(
            args.count
        )
    )

    case_locations = (
        build_case_locations(
            args.count
        )
    )

    saccos = generate_saccos()

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
    )

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

    output_root = (
        args.output_root
    )

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
