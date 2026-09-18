"""Gate 2 shared data-protection library (additive, non-destructive).

Single implementation of keyed tokenisation + masking + log redaction so
services do not hardcode the same policy independently. Import-only: no
importer regenerates Raw, replaces Iceberg tables, or alters Phase 6 SQL.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
from dataclasses import dataclass


class TokenisationKeyMissing(RuntimeError):
    """Raised when mandatory keyed tokenisation has no key: fail closed."""


def _active_key() -> tuple[bytes, str]:
    raw = os.getenv("DP_TOKEN_KEY")
    if not raw or not raw.strip():
        raise TokenisationKeyMissing(
            "DP_TOKEN_KEY is not configured; tokenisation is mandatory so fail closed"
        )
    version = os.getenv("DP_TOKEN_KEY_VERSION", "v1")
    return raw.encode("utf-8"), version


def keyed_token(value: str | None, *, context: str = "gate2") -> str | None:
    """Deterministic HMAC-SHA256 token, stable for legitimate joins.

    Never pass a secret as `value`, and never log the input or the key.
    """
    if value is None:
        return None
    text = str(value)
    if not text.strip():
        return None
    key, version = _active_key()
    digest = hmac.new(key, f"{context}||{text}".encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{version}:{digest}"


@dataclass(frozen=True)
class ProtectionConfig:
    key_version: str = "v1"


def mask_phone(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    if len(text) <= 4:
        return "*" * len(text)
    return "*" * (len(text) - 4) + text[-4:]


def mask_account(value: str | None) -> str | None:
    return mask_phone(value)


def mask_national_id(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    if len(text) <= 3:
        return "*" * len(text)
    return "*" * (len(text) - 3) + text[-3:]


def mask_email(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    if "@" not in text:
        return "***"
    local, domain = text.split("@", 1)
    return f"{local[0]}***@{domain}" if local else f"***@{domain}"


def mask_name(value: str | None) -> str | None:
    return None if value is None else "***"


# Best-effort scrubber for operational logs: structured loggers must call this
# rather than interpolating raw payloads. Patterns intentionally cover the
# platform's known identifier shapes without logging the values themselves.
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE_RE = re.compile(r"\+?256\d{9}|\b0\d{9}\b")
_NIN_RE = re.compile(r"\b[A-Z]{2}\d{6,}[A-Z0-9]*\b")


def redact_for_log(message: str) -> str:
    text = _EMAIL_RE.sub("[REDACTED_EMAIL]", message or "")
    text = _PHONE_RE.sub("[REDACTED_PHONE]", text)
    return text


def log_record_is_clean(record: str) -> bool:
    return (
        "[REDACTED_EMAIL]" in record
        if "@" in record and "REDACTED" not in record
        else True
    ) and ("[REDACTED_PHONE]" in record if re.search(r"\+?256\d{9}|\b0\d{9}\b", record or "") else True)
