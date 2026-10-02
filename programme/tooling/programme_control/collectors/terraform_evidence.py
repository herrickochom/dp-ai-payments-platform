"""Terraform / IaC evidence.

The collector parses ``.tf`` files and the governance contracts. It never
runs ``terraform init``, ``plan``, ``apply`` or ``destroy``. Whether the
Terraform CLI is available is recorded as a fact, not acted upon.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .base import (
    KIND_TERRAFORM,
    PRESENT,
    CollectorResult,
    EvidenceItem,
    file_status,
    register,
    utc_now_iso,
)

TF_ROOT = "infra/terraform"
CONTRACTS_DIR = f"{TF_ROOT}/contracts"


def _scan_tf(root: Path) -> dict[str, Any]:
    base = root / TF_ROOT
    files = sorted(base.rglob("*.tf")) if base.is_dir() else []
    combined = "\n".join(
        path.read_text(encoding="utf-8", errors="replace") for path in files
    )
    return {
        "file_count": len(files),
        "files": [str(path.relative_to(root)) for path in files],
        "has_provider_block": 'provider "' in combined,
        "has_resource_block": 'resource "' in combined,
        "has_backend_block": 'backend "' in combined,
        "required_version": "1.16.x" if 'required_version = "~> 1.16.0"' in combined else "",
    }


def _read_contract(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


@register("terraform")
def collect_terraform(root: Path) -> CollectorResult:
    result = CollectorResult(name="terraform")
    observed = utc_now_iso()

    scan = _scan_tf(root)
    binary = shutil.which("terraform")

    contracts: dict[str, Any] = {}
    contracts_dir = root / CONTRACTS_DIR
    if contracts_dir.is_dir():
        for path in sorted(contracts_dir.glob("*.json")):
            status, _, digest = file_status(path)
            relative = str(path.relative_to(root))
            contracts[relative] = {"status": status, "sha256": digest}

    # Static structural validation only. No state, no provider, no network.
    validation_state = "NOT_RUN"
    if scan["file_count"]:
        validation_state = "STATIC_ONLY"

    result.facts = {
        **scan,
        "cli_available": bool(binary),
        "terraform_cli": binary or "",
        "terraform_validation": validation_state,
        "contract_count": len(contracts),
        "contracts": contracts,
        "observed_at": observed,
    }

    result.add(
        EvidenceItem(
            evidence_id="EV-TF-FOUNDATION",
            kind=KIND_TERRAFORM,
            title="Terraform foundation (variables, outputs, versions)",
            location=TF_ROOT,
            status=PRESENT if scan["file_count"] else "MISSING",
            source_path=TF_ROOT,
            observed_at=observed,
            detail={
                "file_count": scan["file_count"],
                "has_provider_block": scan["has_provider_block"],
                "has_resource_block": scan["has_resource_block"],
                "has_backend_block": scan["has_backend_block"],
                "terraform_validation": validation_state,
                "note": (
                    "No resource or provider block exists: Terraform currently "
                    "defines governance contracts only and provisions nothing."
                ),
            },
        )
    )

    for relative, info in sorted(contracts.items()):
        result.add(
            EvidenceItem(
                evidence_id=f"EV-TF-CONTRACT-{Path(relative).stem[:40].upper()}",
                kind=KIND_TERRAFORM,
                title=f"IaC governance contract {Path(relative).stem}",
                location=relative,
                status=info["status"],
                source_path=relative,
                observed_at=observed,
                detail={"sha256": info["sha256"]},
            )
        )

    result.add(
        EvidenceItem(
            evidence_id="EV-TF-VALIDATION",
            kind=KIND_TERRAFORM,
            title="terraform validate / fmt result",
            location="infra/terraform",
            status="NOT_RUN" if not binary else "AVAILABLE",
            source_path=TF_ROOT,
            observed_at=observed,
            detail={
                "terraform_validation": validation_state,
                "cli_available": bool(binary),
                "note": (
                    "The updater does not run terraform. Run "
                    "'terraform -chdir=infra/terraform validate' as a separate "
                    "explicit action to produce runtime evidence."
                ),
            },
        )
    )
    return result
