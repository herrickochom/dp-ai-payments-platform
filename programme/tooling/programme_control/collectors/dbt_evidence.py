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
    manifest_status, manifest_bytes, manifest_hash = file_status(manifest_path)
    results_status, results_bytes, results_hash = file_status(results_path)

    manifest = _read_json(manifest_path) if manifest_status == PRESENT else None

    model_sql = sorted((root / MODELS_DIR).rglob("*.sql")) if (root / MODELS_DIR).is_dir() else []
    layer_counts: dict[str, int] = {}
    for path in model_sql:
        relative = path.relative_to(root / MODELS_DIR)
        layer = relative.parts[0] if len(relative.parts) > 1 else "root"
        layer_counts[layer] = layer_counts.get(layer, 0) + 1

    manifest_nodes = (manifest or {}).get("nodes", {}) or {}
    manifest_model_count = sum(
        1 for n in manifest_nodes.values() if n.get("resource_type") == "model"
    )
    manifest_test_count = sum(
        1 for n in manifest_nodes.values() if n.get("resource_type") == "test"
    )

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
        "dbt_result": dbt_result,
        "dbt_model_count": manifest_model_count,
        "dbt_test_count": manifest_test_count,
        "source_model_file_count": len(model_sql),
        "layer_counts": layer_counts,
        "technical_status": (
            "MANIFEST_ONLY" if manifest_status == PRESENT else "UNKNOWN"
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
            evidence_id="EV-DBT-MANIFEST-COUNTS",
            kind=KIND_DBT,
            title="dbt manifest model and test counts",
            location=MANIFEST,
            status=manifest_status,
            source_path=MANIFEST,
            observed_at=observed,
            detail={
                "sha256": manifest_hash,
                "model_count": manifest_model_count,
                "test_count": manifest_test_count,
                "note": (
                    "Counts are read from manifest.json when present. "
                    "A declared count, not an execution result."
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
