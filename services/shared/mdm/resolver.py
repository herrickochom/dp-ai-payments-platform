from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True)
class Resolution:
    master_domain: str
    golden_record_id: str | None
    source_system: str
    source_entity: str
    source_record_id: str
    match_rule: str
    match_status: str


def stable_golden_id(
    domain: str,
    source_system: str,
    source_record_id: str,
) -> str:
    """
    Deterministic non-PII MDM identifier.

    This is NOT the downstream beneficiary HMAC token and MUST NOT
    replace or rematerialise the protected token-link.
    """
    raw = (
        f"pdm-mdm|{domain}|{source_system}|{source_record_id}"
    ).encode("utf-8")

    digest = hashlib.sha256(raw).hexdigest()[:24]

    prefixes = {
        "beneficiary": "MDMBEN",
        "sacco": "MDMSAC",
        "agent": "MDMAGT",
        "geography": "MDMGEO",
    }

    try:
        prefix = prefixes[domain]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported MDM domain: {domain}"
        ) from exc

    return f"{prefix}-{digest}"


def _required(
    row: dict[str, Any],
    field: str,
) -> Any:
    value = row.get(field)

    if value is None or value == "":
        raise ValueError(
            f"Missing required field: {field}"
        )

    return value


def resolve_beneficiaries(
    rows: Iterable[dict[str, Any]],
) -> tuple[list[Resolution], list[dict[str, Any]]]:
    records = list(rows)

    # NIN is restricted identity data. It is used only inside
    # this restricted resolver and is never copied to crosswalk output.
    verified_nins = [
        str(row["nin"])
        for row in records
        if row.get("nin")
        and row.get("nin_verified") is True
    ]

    nin_counts = Counter(verified_nins)

    resolutions: list[Resolution] = []
    alerts: list[dict[str, Any]] = []

    for row in records:
        source_system = str(
            _required(row, "source_system")
        )
        source_entity = str(
            _required(row, "source_entity")
        )
        source_record_id = str(
            _required(row, "source_record_id")
        )

        nin = row.get("nin")
        nin_verified = row.get("nin_verified") is True

        golden_id = stable_golden_id(
            "beneficiary",
            source_system,
            source_record_id,
        )

        if nin and nin_verified and nin_counts[str(nin)] > 1:
            # Shared verified NIN is an identity-quality signal,
            # not permission to collapse distinct source persons.
            resolutions.append(
                Resolution(
                    master_domain="beneficiary",
                    golden_record_id=golden_id,
                    source_system=source_system,
                    source_entity=source_entity,
                    source_record_id=source_record_id,
                    match_rule="shared_verified_nin",
                    match_status="QUARANTINED",
                )
            )

            alerts.append(
                {
                    "master_domain": "beneficiary",
                    "golden_record_id": golden_id,
                    "source_system": source_system,
                    "source_entity": source_entity,
                    "source_record_id": source_record_id,
                    "alert_type": "SHARED_VERIFIED_NIN",
                    "severity": "HIGH",
                }
            )

            continue

        if nin and nin_verified:
            rule = "verified_nin_exact"
        else:
            # At present there is only one beneficiary master source.
            # Source ID establishes provenance, not cross-source identity.
            rule = "source_record_provenance"

        resolutions.append(
            Resolution(
                master_domain="beneficiary",
                golden_record_id=golden_id,
                source_system=source_system,
                source_entity=source_entity,
                source_record_id=source_record_id,
                match_rule=rule,
                match_status="MATCHED",
            )
        )

    return resolutions, alerts


def resolve_simple_domain(
    domain: str,
    rows: Iterable[dict[str, Any]],
) -> list[Resolution]:
    output: list[Resolution] = []

    for row in rows:
        source_system = str(
            _required(row, "source_system")
        )
        source_entity = str(
            _required(row, "source_entity")
        )
        source_record_id = str(
            _required(row, "source_record_id")
        )

        output.append(
            Resolution(
                master_domain=domain,
                golden_record_id=stable_golden_id(
                    domain,
                    source_system,
                    source_record_id,
                ),
                source_system=source_system,
                source_entity=source_entity,
                source_record_id=source_record_id,
                match_rule="authoritative_source_key",
                match_status="MATCHED",
            )
        )

    return output


def crosswalk_record(
    resolution: Resolution,
    *,
    valid_from: str,
) -> dict[str, Any]:
    """
    Crosswalk deliberately contains no NIN, name, phone, email,
    DOB or other direct beneficiary identity attributes.
    """
    return {
        "master_domain": resolution.master_domain,
        "golden_record_id": resolution.golden_record_id,
        "source_system": resolution.source_system,
        "source_entity": resolution.source_entity,
        "source_record_id": resolution.source_record_id,
        "match_rule": resolution.match_rule,
        "match_status": resolution.match_status,
        "valid_from": valid_from,
        "valid_to": None,
        "is_current": True,
    }
