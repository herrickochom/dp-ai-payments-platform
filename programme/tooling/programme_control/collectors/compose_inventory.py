"""Docker Compose service inventory.

Parsed statically. The collector never runs ``docker compose up``, never
starts a container and never mutates Compose files. Service names and
image references are recorded; environment values are not, because a
Compose environment block may carry a secret.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .base import (
    KIND_COMPOSE,
    PRESENT,
    CollectorResult,
    EvidenceItem,
    file_status,
    register,
    utc_now_iso,
)

COMPOSE_FILES = (
    "docker-compose.yaml",
    "docker-compose.production-tls.yaml",
    "docker-compose.transform-execution.yaml",
    "docker-compose.plugin-dev.yaml",
)

# Compose service keys that are infrastructure helpers, not platform
# capabilities; recorded but flagged so they are not counted as features.
INFRASTRUCTURE_SERVICES = frozenset(
    {
        "redis",
        "postgres",
        "postgres-healthcheck",
        "pgadmin",
        "minio",
        "minio-init-buckets",
        "minio-init-databases",
        "minio-init-iam",
        "kafka-volume-init",
        "kafka-init",
        "schema-registry-init",
        "kafka-ui",
        "airflow-init",
        "superset-init",
        "transform-ledger-migrate",
        "data-platform-network",
    }
)


def _parse_services(path: Path) -> list[str]:
    """Extract top-level service names without a YAML round trip.

    Compose service names sit at two-space indentation under ``services``.
    Using the parser would materialise environment values we must not keep.
    """
    names: list[str] = []
    in_services = False
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        if not raw_line.startswith(" "):
            in_services = raw_line.rstrip(":") == "services"
            continue
        if not in_services:
            continue
        if not raw_line.startswith("  ") or raw_line.startswith("   "):
            continue
        key = raw_line.strip()
        if key.endswith(":"):
            name = key[:-1]
            if name and not name.startswith("-"):
                names.append(name)
    return names


@register("compose")
def collect_compose(root: Path) -> CollectorResult:
    result = CollectorResult(name="compose")
    observed = utc_now_iso()

    services: dict[str, list[str]] = {}
    files_present: list[str] = []
    for relative in COMPOSE_FILES:
        path = root / relative
        status, _, digest = file_status(path)
        if status not in (PRESENT, "PRESENT_EMPTY"):
            continue
        files_present.append(relative)
        names = _parse_services(path)
        services[relative] = sorted(names)
        result.add(
            EvidenceItem(
                evidence_id=f"EV-COMPOSE-{relative.replace('.', '-').upper()[:40]}",
                kind=KIND_COMPOSE,
                title=f"Compose definition {relative}",
                location=relative,
                status=status,
                source_path=relative,
                observed_at=observed,
                detail={
                    "service_count": len(names),
                    "sha256": digest,
                    "services": names,
                },
            )
        )

    authoritative = services.get("docker-compose.yaml", [])
    capability_services = [
        name for name in authoritative if name not in INFRASTRUCTURE_SERVICES
    ]

    result.facts = {
        "file_count": len(files_present),
        "service_count": len(authoritative),
        "capability_service_count": len(capability_services),
        "capability_services": capability_services,
        "infrastructure_service_count": len(authoritative) - len(capability_services),
        "services": services,
        "files": files_present,
        "compose_validation": "NOT_RUN",
        "observed_at": observed,
    }

    result.add(
        EvidenceItem(
            evidence_id="EV-COMPOSE-VALIDATION",
            kind=KIND_COMPOSE,
            title="docker compose config validation",
            location="docker-compose.yaml",
            status="NOT_RUN",
            source_path="docker-compose.yaml",
            observed_at=observed,
            detail={
                "note": (
                    "The updater never runs docker compose. CI job "
                    "'compose-validation' owns this check."
                ),
                "ci_job": "compose_validation",
            },
        )
    )
    return result
