"""dbt evidence: manifest and run_results presence, model and test counts.

``transform/dbt/target/manifest.json`` and ``run_results.json`` are build
outputs, not committed source. This collector reports their real state:
an existing but zero-byte manifest is ``PRESENT_EMPTY`` and proves no
dbt result at all.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .base import (
    KIND_DBT,
    PRESENT,
    CollectorResult,
    EvidenceItem,
    file_status,
    register,
    utc_now_iso,
)

TARGET_DIR = "transform/dbt/target"
MANIFEST = f"{TARGET_DIR}/manifest.json"
RUN_RESULTS = f"{TARGET_DIR}/run_results.json"
CONTRACT = "orchestration/transform_runtime/contracts/lakehouse_transform.json"
MODELS_DIR = "transform/dbt/models"


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


@register("dbt")
def collect_dbt(root: Path) -> CollectorResult:
    result = CollectorResult(name="dbt")
    observed = utc_now_iso()

    manifest_path = root / MANIFEST
    results_path = root / RUN_RESULTS
    contract_path = root / CONTRACT

    manifest_status, manifest_bytes, manifest_hash = file_status(manifest_path)
    results_status, results_bytes, results_hash = file_status(results_path)
    contract_status, _, contract_hash = file_status(contract_path)

    contract = _read_json(contract_path) if contract_status == PRESENT else None

    model_sql = sorted((root / MODELS_DIR).rglob("*.sql")) if (root / MODELS_DIR).is_dir() else []
    layer_counts: dict[str, int] = {}
    for path in model_sql:
        relative = path.relative_to(root / MODELS_DIR)
        layer = relative.parts[0] if len(relative.parts) > 1 else "root"
        layer_counts[layer] = layer_counts.get(layer, 0) + 1

    contract_model_count = int((contract or {}).get("manifest_model_count", 0) or 0)
    contract_test_count = int((contract or {}).get("manifest_test_count", 0) or 0)

    # A dbt result exists only when run_results.json carries real content.
    dbt_result = "NOT_RUN"
    if results_status == PRESENT:
        dbt_result = "RECORDED"
    elif results_status == "PRESENT_EMPTY":
        dbt_result = "EMPTY_OUTPUT"

    result.facts = {
        "manifest_status": manifest_status,
        "manifest_bytes": manifest_bytes,
        "run_results_status": results_status,
        "contract_status": contract_status,
        "dbt_result": dbt_result,
        "dbt_model_count": contract_model_count,
        "dbt_test_count": contract_test_count,
        "source_model_file_count": len(model_sql),
        "layer_counts": layer_counts,
        "technical_status": (
            "CONTRACT_ONLY" if contract_status == PRESENT else "UNKNOWN"
        ),
        "observed_at": observed,
    }

    result.add(
        EvidenceItem(
            evidence_id="EV-DBT-MANIFEST",
            kind=KIND_DBT,
            title="dbt manifest.json",
            location=MANIFEST,
            status=manifest_status,
            source_path=MANIFEST,
            observed_at=observed,
            detail={
                "bytes": manifest_bytes,
                "sha256": manifest_hash,
                "note": (
                    "PRESENT_EMPTY: the file exists but carries no manifest. "
                    "Existence is not a dbt result."
                ),
            },
        )
    )
    result.add(
        EvidenceItem(
            evidence_id="EV-DBT-RUN-RESULTS",
            kind=KIND_DBT,
            title="dbt run_results.json",
            location=RUN_RESULTS,
            status=results_status,
            source_path=RUN_RESULTS,
            observed_at=observed,
            detail={
                "bytes": results_bytes,
                "sha256": results_hash,
                "dbt_result": dbt_result,
            },
        )
    )
    result.add(
        EvidenceItem(
            evidence_id="EV-DBT-CONTRACT",
            kind=KIND_DBT,
            title="Bounded lakehouse transform contract",
            location=CONTRACT,
            status=contract_status,
            source_path=CONTRACT,
            observed_at=observed,
            detail={
                "sha256": contract_hash,
                "model_count": contract_model_count,
                "test_count": contract_test_count,
                "contract_version": (contract or {}).get("contract_version", ""),
                "note": (
                    "Committed contract records the declared model and test "
                    "inventory. It is a declared count, not an execution result."
                ),
            },
        )
    )
    result.add(
        EvidenceItem(
            evidence_id="EV-DBT-SOURCE-MODELS",
            kind=KIND_DBT,
            title="dbt model source files",
            location=MODELS_DIR,
            status=PRESENT if model_sql else "MISSING",
            source_path=MODELS_DIR,
            observed_at=observed,
            detail={
                "model_count": len(model_sql),
                "layer_counts": layer_counts,
            },
        )
    )
    return result
