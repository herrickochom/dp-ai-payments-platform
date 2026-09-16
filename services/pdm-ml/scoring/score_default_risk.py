#!/usr/bin/env python3
"""Score current PDM operational cases with Strict Early Warning v1.

This script deliberately separates operational feature construction from model
inference. It loads the exact fitted sklearn/XGBoost pipeline produced by
train_default_risk.py and requires the operational scoring dataset to contain
the same model input features.

Default input:
  data/pdmis_ml/default_risk_current_scoring.jsonl

Default outputs:
  data/pdmis_ml/default_risk_current_predictions.jsonl
  data/pdmis_ml/default_risk_current_predictions_summary.json

The scoring-input dataset is the next pipeline contract to build from the
current PDMIS operational data. The script fails clearly if required model
features are absent rather than silently inventing them.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd


DEFAULT_MODEL = Path(
    "services/pdm-ml/models/"
    "pdm_default_risk_strict_early_warning_v1.joblib"
)
DEFAULT_METADATA = Path(
    "services/pdm-ml/models/"
    "pdm_default_risk_strict_early_warning_v1_metadata.json"
)
DEFAULT_INPUT = Path(
    "data/pdmis_ml/default_risk_current_scoring.jsonl"
)
DEFAULT_OUTPUT = Path(
    "data/pdmis_ml/default_risk_current_predictions.jsonl"
)
DEFAULT_SUMMARY = Path(
    "data/pdmis_ml/default_risk_current_predictions_summary.json"
)

MODEL_NAME = "pdm_default_risk_strict_early_warning"
MODEL_VERSION = "v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score current PDM cases for 90-day default risk."
    )
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--metadata", type=Path, default=DEFAULT_METADATA)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    return parser.parse_args()


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8"))


def load_scoring_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Scoring dataset not found: {path}\n"
            "Build the current operational ML feature dataset before scoring."
        )
    df = pd.read_json(path, lines=True, convert_dates=False)
    if df.empty:
        raise ValueError(f"Scoring dataset is empty: {path}")
    return df


def risk_band(probability: float) -> str:
    # POC thresholds. These are presentation/triage bands, not calibrated
    # policy thresholds and must be validated on real PDM outcomes later.
    if probability >= 0.75:
        return "SEVERE"
    if probability >= 0.50:
        return "HIGH"
    if probability >= 0.25:
        return "MEDIUM"
    return "LOW"


def validate_contract(
    df: pd.DataFrame,
    metadata: dict[str, Any],
) -> list[str]:
    features = metadata.get("input_features")
    if not isinstance(features, list) or not features:
        raise ValueError("Model metadata has no input_features contract.")

    missing = [c for c in features if c not in df.columns]
    if missing:
        raise ValueError(
            "Operational scoring dataset is missing model features:\n  - "
            + "\n  - ".join(missing)
        )
    return features


def identity_columns(df: pd.DataFrame) -> list[str]:
    preferred = [
        "snapshot_id",
        "beneficiary_id",
        "loan_id",
        "sacco_id",
        "region",
        "district",
        "county",
        "sub_county",
        "parish",
        "village",
        "project_type",
        "special_group",
        "observation_date",
    ]
    return [c for c in preferred if c in df.columns]


def main() -> None:
    args = parse_args()

    if not args.model.exists():
        raise FileNotFoundError(
            f"Fitted pipeline not found: {args.model}\n"
            "Re-run train_default_risk.py after the joblib update."
        )

    metadata = read_json(args.metadata)
    df = load_scoring_data(args.input)
    features = validate_contract(df, metadata)

    pipeline = joblib.load(args.model)
    probability = pipeline.predict_proba(df[features])[:, 1]

    scored_at = datetime.now(timezone.utc).isoformat()

    ids = identity_columns(df)
    result = df[ids].copy() if ids else pd.DataFrame(index=df.index)

    result["probability_default_90d"] = probability.astype(float)
    result["ai_risk_band"] = [
        risk_band(float(p)) for p in probability
    ]
    result["model_name"] = MODEL_NAME
    result["model_version"] = MODEL_VERSION
    result["scored_at_utc"] = scored_at

    # Rank 1 = highest predicted risk in this scoring batch.
    result["risk_rank"] = (
        pd.Series(probability, index=result.index)
        .rank(method="first", ascending=False)
        .astype(int)
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    result.to_json(
        args.output,
        orient="records",
        lines=True,
        force_ascii=False,
    )

    counts = result["ai_risk_band"].value_counts().to_dict()
    summary = {
        "rows_scored": int(len(result)),
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "synthetic_poc": True,
        "scored_at_utc": scored_at,
        "mean_probability_default_90d": float(np.mean(probability)),
        "min_probability_default_90d": float(np.min(probability)),
        "max_probability_default_90d": float(np.max(probability)),
        "risk_band_counts": {
            band: int(counts.get(band, 0))
            for band in ("LOW", "MEDIUM", "HIGH", "SEVERE")
        },
        "input_file": str(args.input),
        "output_file": str(args.output),
        "model_file": str(args.model),
    }
    args.summary.write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )

    print("PDM 90-DAY DEFAULT RISK — CURRENT SCORING")
    print("=" * 72)
    print(f"Rows scored : {len(result):,}")
    print(f"Model       : {MODEL_NAME} {MODEL_VERSION}")
    print(f"Mean risk   : {np.mean(probability):.2%}")
    print(
        "Bands       : "
        + ", ".join(
            f"{band}={int(counts.get(band, 0))}"
            for band in ("LOW", "MEDIUM", "HIGH", "SEVERE")
        )
    )
    print(f"Predictions : {args.output}")
    print(f"Summary     : {args.summary}")

    # Privacy-safe operational diagnostic.
    # Do not emit beneficiary IDs, loan IDs, locations, tokens, or any other
    # case-level identifiers to stdout/stderr because container/terminal
    # output may be retained as operational logs or audit evidence.
    top_risk = (
        result.sort_values(
            "probability_default_90d",
            ascending=False,
        )
        .head(10)
    )

    print("\nHighest-risk summary:")
    print(f"Cases reviewed : {len(top_risk)}")
    print(
        "Risk range     : "
        f"{top_risk['probability_default_90d'].min():.2%}"
        " - "
        f"{top_risk['probability_default_90d'].max():.2%}"
    )


if __name__ == "__main__":
    main()
