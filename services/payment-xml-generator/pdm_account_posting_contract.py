"""Evidence-based account posting contract for CAMT.053/CAMT.054.

No postings, balances, institutional roles or financial events are inferred
from loan lifecycle statuses. This module performs validation only.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping


class AccountPostingContractError(ValueError):
    """Invalid or unreconciled account posting evidence."""


EVENT_TYPES = frozenset({
    "FUNDING_CREDIT",
    "DISBURSEMENT",
    "WALLET_CREDIT",
    "SETTLEMENT",
    "CASH_OUT",
})

DIRECTIONS = frozenset({"CRDT", "DBIT"})

REQUIRED_POSTING_FIELDS = (
    "posting_id",
    "account_id",
    "account_owner_id",
    "payment_transaction_id",
    "event_type",
    "amount_ugx",
    "credit_debit_indicator",
    "booking_datetime",
    "value_date",
    "account_servicer_reference",
    "bank_transaction_code",
)


def required_text(row: Mapping, field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise AccountPostingContractError(
            f"missing or invalid {field}"
        )
    return value.strip()


def money(value, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise AccountPostingContractError(f"invalid {field}")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise AccountPostingContractError(
            f"invalid {field}"
        ) from None

    if not amount.is_finite():
        raise AccountPostingContractError(f"invalid {field}")

    if positive and amount <= 0:
        raise AccountPostingContractError(
            f"{field} must be positive"
        )

    if amount != amount.to_integral_value():
        raise AccountPostingContractError(
            f"{field} must be integral UGX"
        )

    return amount


def parse_datetime(value, field: str) -> datetime:
    if not isinstance(value, str):
        raise AccountPostingContractError(f"invalid {field}")
    try:
        result = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except ValueError:
        raise AccountPostingContractError(
            f"invalid {field}"
        ) from None

    if result.tzinfo is None or result.utcoffset() is None:
        raise AccountPostingContractError(
            f"{field} requires timezone"
        )

    return result


def parse_date(value, field: str) -> date:
    if not isinstance(value, str):
        raise AccountPostingContractError(f"invalid {field}")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise AccountPostingContractError(
            f"invalid {field}"
        ) from None


def _validate_postings_base_v1(
    postings,
    *,
    governed_bank_transaction_codes,
) -> list[dict]:
    """Validate supplied posting evidence without creating new postings."""

    if not isinstance(postings, (list, tuple)):
        raise AccountPostingContractError(
            "postings must be a sequence"
        )

    if not isinstance(governed_bank_transaction_codes, (set, frozenset)):
        raise AccountPostingContractError(
            "governed bank transaction codes must be a set"
        )

    if not governed_bank_transaction_codes:
        raise AccountPostingContractError(
            "governed bank transaction codes cannot be empty"
        )

    if not all(
        isinstance(code, str) and code.strip()
        for code in governed_bank_transaction_codes
    ):
        raise AccountPostingContractError(
            "invalid governed bank transaction code"
        )

    validated = []
    identifiers = []

    for index, posting in enumerate(postings, 1):
        if not isinstance(posting, dict):
            raise AccountPostingContractError(
                f"posting {index} must be an object"
            )

        row = dict(posting)

        for field in REQUIRED_POSTING_FIELDS:
            if field not in row:
                raise AccountPostingContractError(
                    f"posting {index} missing {field}"
                )

        for field in (
            "posting_id",
            "account_id",
            "account_owner_id",
            "payment_transaction_id",
            "account_servicer_reference",
            "bank_transaction_code",
        ):
            row[field] = required_text(row, field)

        event_type = required_text(row, "event_type")
        if event_type not in EVENT_TYPES:
            raise AccountPostingContractError(
                f"unsupported event_type: {event_type}"
            )

        direction = required_text(
            row, "credit_debit_indicator"
        )
        if direction not in DIRECTIONS:
            raise AccountPostingContractError(
                f"unsupported credit/debit direction: {direction}"
            )

        if (
            row["bank_transaction_code"]
            not in governed_bank_transaction_codes
        ):
            raise AccountPostingContractError(
                "bank transaction code is not governed"
            )

        row["amount_ugx"] = money(
            row["amount_ugx"],
            "amount_ugx",
            positive=True,
        )

        row["booking_datetime"] = parse_datetime(
            row["booking_datetime"],
            "booking_datetime",
        )

        row["value_date"] = parse_date(
            row["value_date"],
            "value_date",
        )

        if row.get("loan_id") is not None:
            row["loan_id"] = required_text(row, "loan_id")

        identifiers.append(row["posting_id"])
        validated.append(row)

    if any(n > 1 for n in Counter(identifiers).values()):
        raise AccountPostingContractError(
            "duplicate posting_id"
        )

    return validated


def _reconcile_statement_base_v1(
    *,
    account_id: str,
    period_start: str,
    period_end: str,
    opening_balance_ugx,
    closing_balance_ugx,
    postings,
) -> dict:
    """Reconcile supplied statement balances and in-period postings.

    Positive balances represent credit balances; negative balances
    represent debit balances. No opening balance is inferred.
    """

    if not isinstance(account_id, str) or not account_id.strip():
        raise AccountPostingContractError(
            "invalid statement account_id"
        )

    start = parse_datetime(period_start, "period_start")
    end = parse_datetime(period_end, "period_end")

    if end <= start:
        raise AccountPostingContractError(
            "statement period must be increasing"
        )

    opening = money(
        opening_balance_ugx, "opening_balance_ugx"
    )
    closing = money(
        closing_balance_ugx, "closing_balance_ugx"
    )

    if not isinstance(postings, (list, tuple)):
        raise AccountPostingContractError(
            "postings must be a sequence"
        )

    credits = Decimal(0)
    debits = Decimal(0)

    for posting in postings:
        if not isinstance(posting, dict):
            raise AccountPostingContractError(
                "invalid posting"
            )

        if posting.get("account_id") != account_id:
            raise AccountPostingContractError(
                "posting account mismatch"
            )

        booked = posting.get("booking_datetime")
        if not isinstance(booked, datetime):
            raise AccountPostingContractError(
                "postings must be validated first"
            )

        if booked.tzinfo is None or booked.utcoffset() is None:
            raise AccountPostingContractError(
                "posting booking datetime requires timezone"
            )

        if not (start <= booked < end):
            raise AccountPostingContractError(
                "posting outside statement period"
            )

        amount = money(
            posting.get("amount_ugx"),
            "amount_ugx",
            positive=True,
        )

        direction = posting.get("credit_debit_indicator")
        if direction == "CRDT":
            credits += amount
        elif direction == "DBIT":
            debits += amount
        else:
            raise AccountPostingContractError(
                "invalid posting direction"
            )

    calculated = opening + credits - debits

    if calculated != closing:
        raise AccountPostingContractError(
            "statement balance reconciliation failed"
        )

    return {
        "account_id": account_id,
        "opening_balance_ugx": opening,
        "total_credits_ugx": credits,
        "total_debits_ugx": debits,
        "closing_balance_ugx": closing,
        "posting_count": len(postings),
    }


# ---------------------------------------------------------------------------
# Phase 2 — independently evidenced account postings
# ---------------------------------------------------------------------------

def validate_bank_transaction_code_v1(code, governed_codes):
    """Validate structured ISO 20022 domain/family/subfamily classification.

    The caller supplies the approved code combinations. This function does
    not invent or approve a transaction classification.
    """
    if not isinstance(code, dict):
        raise AccountPostingContractError(
            "bank transaction code must be structured"
        )

    required = ("domain", "family", "subfamily")
    if set(code) != set(required):
        raise AccountPostingContractError(
            "bank transaction code requires domain/family/subfamily"
        )

    values = tuple(required_text(code, key) for key in required)

    if not isinstance(governed_codes, (set, frozenset)):
        raise AccountPostingContractError(
            "governed transaction codes must be a set"
        )

    if values not in governed_codes:
        raise AccountPostingContractError(
            "unapproved bank transaction classification"
        )

    return dict(zip(required, values))


def validate_postings_v1(
    postings,
    *,
    governed_bank_transaction_codes,
    account_ownership,
    authoritative_posting_ids,
):
    """Validate explicit posting evidence, ownership and ISO classification.

    authoritative_posting_ids is an independently supplied allowlist.
    Membership proves only that an ID was supplied as authoritative;
    source authenticity must be established by the caller.
    """

    if not isinstance(postings, (list, tuple)):
        raise AccountPostingContractError(
            "postings must be a sequence"
        )

    if not isinstance(account_ownership, dict):
        raise AccountPostingContractError(
            "account ownership must be a mapping"
        )

    if not isinstance(
        authoritative_posting_ids, (set, frozenset)
    ):
        raise AccountPostingContractError(
            "authoritative posting IDs must be a set"
        )

    if not isinstance(
        governed_bank_transaction_codes, (set, frozenset)
    ):
        raise AccountPostingContractError(
            "governed transaction codes must be a set"
        )

    validated = []
    identifiers = set()

    for posting in postings:
        if not isinstance(posting, dict):
            raise AccountPostingContractError(
                "posting must be an object"
            )

        row = dict(posting)

        for field in REQUIRED_POSTING_FIELDS:
            if field not in row:
                raise AccountPostingContractError(
                    f"missing {field}"
                )

        posting_id = required_text(row, "posting_id")
        account_id = required_text(row, "account_id")
        owner_id = required_text(row, "account_owner_id")

        if posting_id in identifiers:
            raise AccountPostingContractError(
                "duplicate posting_id"
            )

        identifiers.add(posting_id)

        if posting_id not in authoritative_posting_ids:
            raise AccountPostingContractError(
                "posting lacks authoritative evidence"
            )

        if account_ownership.get(account_id) != owner_id:
            raise AccountPostingContractError(
                "account ownership mismatch"
            )

        code = validate_bank_transaction_code_v1(
            row["bank_transaction_code"],
            governed_bank_transaction_codes,
        )

        # Reuse the original Phase 1 field and temporal validation.
        # A temporary internal string token is used only to invoke the
        # existing validator; the returned posting retains the structure.
        token = "|".join(
            (code["domain"], code["family"], code["subfamily"])
        )

        row["bank_transaction_code"] = token

        normalized = _validate_postings_base_v1(
            [row],
            governed_bank_transaction_codes={token},
        )[0]

        normalized["bank_transaction_code"] = code
        validated.append(normalized)

    return validated


def reconcile_statement_v1(
    *,
    account_id,
    account_owner_id,
    period_start,
    period_end,
    opening_balance_ugx,
    closing_balance_ugx,
    postings,
    governed_bank_transaction_codes,
    account_ownership,
    authoritative_posting_ids,
):
    """Validate all posting evidence before reconciling a statement."""

    if account_ownership.get(account_id) != account_owner_id:
        raise AccountPostingContractError(
            "statement account ownership mismatch"
        )

    validated = validate_postings_v1(
        postings,
        governed_bank_transaction_codes=governed_bank_transaction_codes,
        account_ownership=account_ownership,
        authoritative_posting_ids=authoritative_posting_ids,
    )

    result = _reconcile_statement_base_v1(
        account_id=account_id,
        period_start=period_start,
        period_end=period_end,
        opening_balance_ugx=opening_balance_ugx,
        closing_balance_ugx=closing_balance_ugx,
        postings=validated,
    )

    result["account_owner_id"] = account_owner_id
    return result
