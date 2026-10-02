"""Secret detection for programme metadata.

The programme system stores safe metadata only: secret NAME, OWNER,
PROVIDER, ROTATION DATE and STATUS. This module is the automated gate
that proves no secret value has leaked into a register, the workbook, a
report or a log line.

Detection is intentionally conservative: it looks for well-known
credential shapes and high-entropy tokens assigned to secret-bearing
keys. It never attempts to read ``.env`` files or key material.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

# Field names whose values must never contain a credential.
SECRET_BEARING_KEY = re.compile(
    r"(?i)(password|passwd|pwd|secret|token|api[_-]?key|apikey|"
    r"access[_-]?key|private[_-]?key|credential|passphrase|"
    r"client[_-]?secret|connection[_-]?string|auth[_-]?token|"
    r"bearer|session[_-]?key|kms[_-]?key)"
)

# Values that are legitimately metadata, not secrets.
SAFE_VALUE_MARKERS = frozenset(
    {
        "",
        "-",
        "n/a",
        "na",
        "none",
        "null",
        "not_applicable",
        "not applicable",
        "notconfigured",
        "not_configured",
        "unset",
        "unknown",
        "tbd",
        "unresolved",
        "notset",
        "not_set",
        "prohibited",
        "forbidden",
        "required",
        "metadata_only",
        "secret_reference",
        "reference_only",
        "rotated",
        "active",
        "planned",
        "revoked",
        "yes",
        "no",
        "true",
        "false",
    }
)

# High-signal credential shapes.
PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("private_key_block", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("aws_access_key_id", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("github_token", re.compile(r"\bgh[pousr]_[A-Za-z0-9]{16,}\b")),
    ("slack_token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}\b")),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b")),
    ("bearer_header", re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._-]{20,}")),
    (
        "pg_or_mysql_dsn",
        re.compile(r"(?i)\b(?:postgres|postgresql|mysql|redis|mongodb)://[^\s:@/]+:[^\s:@/]{3,}@"),
    ),
    ("http_basic_dsn", re.compile(r"(?i)\bhttps?://[^\s:@/]+:[^\s:@/]{3,}@")),
    ("pem_body", re.compile(r"-----BEGIN CERTIFICATE-----|-----BEGIN PUBLIC KEY-----")),
    (
        "assigned_secret",
        re.compile(
            r"(?i)\b(?:password|passwd|pwd|secret|token|api[_-]?key|apikey|"
            r"access[_-]?key|private[_-]?key|passphrase|client[_-]?secret)\b"
            r"\s*[:=]\s*[\"']?[^\s\"',;]{6,}"
        ),
    ),
)

_MIN_ENTROPY = 3.4
_MIN_TOKEN_LENGTH = 16


@dataclass(frozen=True)
class SecretFinding:
    """One suspected secret leak."""

    location: str
    field: str
    detector: str
    preview: str

    def describe(self) -> str:
        return (
            f"{self.location} field={self.field} detector={self.detector} "
            f"preview={self.preview}"
        )


def _redact(value: str) -> str:
    if len(value) <= 4:
        return "*" * len(value)
    return value[:2] + "*" * (len(value) - 4) + value[-2:]


def shannon_entropy(value: str) -> float:
    if not value:
        return 0.0
    counts: dict[str, int] = {}
    for char in value:
        counts[char] = counts.get(char, 0) + 1
    total = len(value)
    return -sum(
        (count / total) * math.log2(count / total) for count in counts.values()
    )


def _is_safe_metadata(value: str) -> bool:
    normalised = value.strip().strip("\"'").lower()
    if normalised in SAFE_VALUE_MARKERS:
        return True
    # A metadata reference (file path, env var name, placeholder) is safe.
    if re.fullmatch(r"\$\{?[A-Za-z_][A-Za-z0-9_]*\}?", normalised):
        return True
    if re.fullmatch(r"[A-Z][A-Z0-9_]{2,}", normalised):
        return True
    if normalised.startswith(("s3://", "s3a://", "arn:", "file:", "./", "/")):
        return True
    if "<" in normalised and ">" in normalised:
        return True
    if re.fullmatch(r"[0-9a-f]{8,}", normalised):
        return True
    return False


def scan_value(location: str, field: str, value: Any) -> list[SecretFinding]:
    """Scan a single value for secret-shaped content."""
    findings: list[SecretFinding] = []
    if value is None or isinstance(value, bool) or isinstance(value, (int, float)):
        return findings
    text = str(value)
    if not text.strip():
        return findings
    if _is_safe_metadata(text):
        return findings

    for detector, pattern in PATTERNS:
        match = pattern.search(text)
        if match:
            findings.append(
                SecretFinding(
                    location=location,
                    field=field,
                    detector=detector,
                    preview=_redact(match.group(0)),
                )
            )

    # A secret-bearing field holding a long, high-entropy value is a leak
    # even when no well-known credential shape matched.
    if (
        SECRET_BEARING_KEY.search(field)
        and len(text) >= _MIN_TOKEN_LENGTH
        and shannon_entropy(text) >= _MIN_ENTROPY
    ):
        findings.append(
            SecretFinding(
                location=location,
                field=field,
                detector="high_entropy_secret_field",
                preview=_redact(text),
            )
        )
    return findings


def scan_mapping(
    location: str, mapping: Mapping[str, Any], prefix: str = ""
) -> list[SecretFinding]:
    """Recursively scan a mapping for secret-shaped content."""
    findings: list[SecretFinding] = []
    for key, value in mapping.items():
        field_path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, Mapping):
            findings.extend(scan_mapping(location, value, field_path))
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                if isinstance(item, Mapping):
                    findings.extend(
                        scan_mapping(location, item, f"{field_path}[{index}]")
                    )
                else:
                    findings.extend(scan_value(location, field_path, item))
        else:
            findings.extend(scan_value(location, field_path, value))
    return findings


def scan_records(
    register: str, records: Iterable[Mapping[str, Any]]
) -> list[SecretFinding]:
    """Scan a sequence of registry records."""
    findings: list[SecretFinding] = []
    for record in records:
        record_id = str(record.get("id", "<no-id>"))
        findings.extend(
            scan_mapping(f"register:{register}:{record_id}", record)
        )
    return findings


def scan_workbook_values(
    sheet_name: str, row_number: int, column_name: str, value: Any
) -> list[SecretFinding]:
    """Scan a single workbook cell value."""
    return scan_value(f"sheet:{sheet_name}:row{row_number}", column_name, value)
