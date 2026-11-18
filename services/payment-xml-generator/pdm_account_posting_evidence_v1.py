"""Fail-closed Wendi account-posting evidence boundary."""

from collections.abc import Mapping


class MissingAuthoritativePostingEvidence(ValueError):
    """Authoritative ledger evidence has not been established."""


REQUIRED_FIELDS = (
    "account_id",
    "account_owner_id",
    "period_start",
    "period_end",
    "opening_balance_ugx",
    "closing_balance_ugx",
    "postings",
    "governed_bank_transaction_codes",
    "account_ownership",
    "authoritative_posting_ids",
)


def require_authoritative_posting_evidence_v1(
    context: Mapping,
    *,
    document_type: str,
):
    if document_type not in ("camt.053", "camt.054"):
        raise ValueError("Unsupported CAMT document type")

    if not isinstance(context, Mapping):
        raise MissingAuthoritativePostingEvidence(
            "Payment context must be a mapping"
        )

    evidence = context.get("account_posting_evidence_v1")

    if not isinstance(evidence, Mapping):
        raise MissingAuthoritativePostingEvidence(
            f"{document_type}: authoritative posting evidence missing"
        )

    missing = [
        field for field in REQUIRED_FIELDS
        if field not in evidence or evidence[field] is None
    ]

    if missing:
        raise MissingAuthoritativePostingEvidence(
            f"{document_type}: missing evidence: {', '.join(missing)}"
        )

    # A complete dictionary is not proof of authoritative provenance.
    # No ledger provider has yet been verified.
    raise MissingAuthoritativePostingEvidence(
        f"{document_type}: independent ledger provenance and posting "
        "completeness remain unverified"
    )
