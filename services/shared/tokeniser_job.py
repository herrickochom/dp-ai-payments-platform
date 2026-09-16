"""Gate 2 canonical beneficiary tokenisation job (bounded, offline-first).

Reads canonical beneficiary_id values (Bronze-shaped rows supplied by the
caller), generates versioned HMAC-SHA256 beneficiary_token values via the
single shared implementation, and returns a versioned ROW-BASED token-link
mapping. No Bronze/Silver/Gold/Consumption table is modified here; the future
Silver rebuild consumes only the active canonical token column.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence

from data_protection import TokenisationKeyMissing, keyed_token

TOKEN_CONTEXT = "gate2-beneficiary"
LEGACY_TOKEN_PREFIX = "TOKEN-"
MISSING_VERSION_MESSAGE = (
    "DP_TOKEN_KEY_VERSION is not configured; tokenisation is mandatory so fail closed"
)


class TokenisationVersionMissing(RuntimeError):
    """Raised when DP_TOKEN_KEY_VERSION is absent: fail closed."""


@dataclass(frozen=True)
class TokenLinkRow:
    beneficiary_key_internal: str
    token_version: str
    beneficiary_token: str
    is_active: bool
    generated_at: str


def _require_version(version: str | None) -> str:
    if version is None or not str(version).strip():
        raise TokenisationVersionMissing(MISSING_VERSION_MESSAGE)
    return str(version).strip()


def canonical_token_for(beneficiary_id: str, *, version: str) -> str:
    """Canonical token: HMAC-SHA256 over the fixed Gate 2 input domain."""
    token = keyed_token(beneficiary_id, context=TOKEN_CONTEXT)
    if token is None:
        raise ValueError("beneficiary_id must be a non-empty canonical identifier")
    _require_version(version)
    digest = token.split(":", 1)[1]
    return f"{_require_version(version)}:{digest}"


def build_token_links(
    beneficiary_ids: Sequence[str],
    *,
    version: str,
    generated_at: str | None = None,
) -> list[TokenLinkRow]:
    """Build active-version token-link rows for one rotation version."""
    active_version = _require_version(version)
    stamp = generated_at or datetime.now(timezone.utc).isoformat()
    rows: list[TokenLinkRow] = []
    seen: set[str] = set()
    for beneficiary_id in beneficiary_ids:
        if beneficiary_id is None or not str(beneficiary_id).strip():
            raise ValueError("beneficiary_id must be a non-empty canonical identifier")
        canonical_id = str(beneficiary_id)
        if canonical_id in seen:
            raise ValueError(f"duplicate beneficiary_id in tokenisation batch: {canonical_id}")
        seen.add(canonical_id)
        rows.append(
            TokenLinkRow(
                beneficiary_key_internal=f"internal:{canonical_id}",
                token_version=active_version,
                beneficiary_token=canonical_token_for(canonical_id, version=active_version),
                is_active=True,
                generated_at=stamp,
            )
        )
    return rows


def rows_from_bronze_records(
    records: Iterable[Mapping[str, object]],
) -> list[str]:
    """Extract canonical beneficiary_id values from Bronze-shaped rows."""
    ids: list[str] = []
    for record in records:
        value = record.get("beneficiary_id")
        if value is None or not str(value).strip():
            raise ValueError("Bronze row is missing canonical beneficiary_id")
        ids.append(str(value))
    return ids
