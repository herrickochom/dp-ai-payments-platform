#!/usr/bin/env python3
"""Train PDM 90-day default-risk POC models from synthetic history.

Trains three XGBoost classifiers:
  1. baseline             - all legitimate observation-time features
  2. early_warning        - excludes strongest near-default delinquency signals
  3. strict_early_warning - also excludes geography and SACCO identifiers

The split is temporal: older observations train the models and newer
observations evaluate them.

This is a synthetic proof of concept and is not a production credit decision
model.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.compose import ColumnTransformer
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


TARGET = "defaulted_within_90_days"

DEFAULT_INPUT = Path(
    "data/pdmis_ml/default_risk_training_snapshots.jsonl"
)
DEFAULT_MODEL_DIR = Path("services/pdm-ml/models")

# These fields must never enter the model.
#
# snapshot_id is included explicitly because it is an identifier. The previous
# version did not exclude it, which allowed the string value SNAP-... to reach
# XGBoost and caused the observed "could not convert string to float" error.
ALWAYS_EXCLUDE = {
    TARGET,
    "snapshot_id",
    "synthetic_behaviour_profile",
    "loan_id",
    "beneficiary_id",
    "observation_date",
    "approval_date",
    "disbursement_date",
}

# Model B removes the strongest current-delinquency indicators. It asks a more
# useful question: can we identify likely deterioration before default is
# already obvious from severe arrears?
EARLY_WARNING_EXCLUDE = {
    "days_past_due",
    "missed_instalment_count",
    "zero_payment_month_count",
    "arrears_amount_as_of_observation",
}

# Model C removes both near-default delinquency signals and geography/SACCO
# identifiers. This tests whether predictive performance survives without
# memorising synthetic location or SACCO patterns.
STRICT_EARLY_WARNING_EXCLUDE = EARLY_WARNING_EXCLUDE | {
    "sacco_id",
    "region",
    "district",
    "county",
    "sub_county",
    "parish",
    "village",
}

# Additional defensive name patterns. These protect the POC if the historical
# generator later gains explicit future/target/label columns.
FORBIDDEN_NAME_FRAGMENTS = (
    "defaulted_within_",
    "future_",
    "_future",
    "target_",
    "_target",
    "label_",
    "_label",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train PDM 90-day default-risk synthetic POC models."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Historical training snapshot JSONL file.",
    )
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help="Directory for trained models and metadata.",
    )
    parser.add_argument(
        "--test-fraction",
        type=float,
        default=0.20,
        help="Newest fraction of observations reserved for temporal testing.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="Maximum XGBoost CPU threads.",
    )
    return parser.parse_args()


def load_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Training data not found: {path}"
        )

    df = pd.read_json(
        path,
        lines=True,
        convert_dates=False,
    )

    if df.empty:
        raise ValueError(
            f"Training data is empty: {path}"
        )

    if TARGET not in df.columns:
        raise ValueError(
            f"Training data is missing target column: {TARGET}"
        )

    if "observation_date" not in df.columns:
        raise ValueError(
            "Training data is missing observation_date; "
            "a temporal split cannot be performed."
        )

    df["observation_date"] = pd.to_datetime(
        df["observation_date"],
        errors="raise",
    )

    # Normalize target defensively.
    df[TARGET] = pd.to_numeric(
        df[TARGET],
        errors="raise",
    ).astype("int8")

    invalid_targets = sorted(
        set(df[TARGET].unique()) - {0, 1}
    )
    if invalid_targets:
        raise ValueError(
            f"Target must contain only 0/1; found: {invalid_targets}"
        )

    if df[TARGET].nunique() != 2:
        raise ValueError(
            "Target must contain both default and non-default observations."
        )

    # Stable ordering is useful when many snapshots share the same date.
    sort_columns = ["observation_date"]
    if "snapshot_id" in df.columns:
        sort_columns.append("snapshot_id")

    return (
        df.sort_values(sort_columns, kind="stable")
        .reset_index(drop=True)
    )


def temporal_split(
    df: pd.DataFrame,
    test_fraction: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not 0.05 <= test_fraction <= 0.40:
        raise ValueError(
            "--test-fraction must be between 0.05 and 0.40"
        )

    cut = int(len(df) * (1.0 - test_fraction))

    if cut < 1 or cut >= len(df):
        raise ValueError(
            "Temporal split produced an empty train or test set."
        )

    train = df.iloc[:cut].copy()
    test = df.iloc[cut:].copy()

    if train[TARGET].nunique() != 2:
        raise ValueError(
            "Training portion of temporal split does not contain both classes."
        )

    if test[TARGET].nunique() != 2:
        raise ValueError(
            "Test portion of temporal split does not contain both classes."
        )

    return train, test


def looks_forbidden(column: str) -> bool:
    lower = column.lower()

    if lower in ALWAYS_EXCLUDE:
        return True

    return any(
        fragment in lower
        for fragment in FORBIDDEN_NAME_FRAGMENTS
    )


def feature_columns(
    df: pd.DataFrame,
    extra_exclude: set[str],
) -> list[str]:
    excluded = ALWAYS_EXCLUDE | extra_exclude

    features: list[str] = []

    for column in df.columns:
        if column in excluded:
            continue

        if looks_forbidden(column):
            continue

        features.append(column)

    if not features:
        raise ValueError(
            "No model features remain after exclusions."
        )

    return features


def classify_columns(
    df: pd.DataFrame,
    features: Sequence[str],
) -> tuple[list[str], list[str]]:
    """Split features into numeric and categorical columns safely.

    Any non-numeric feature is encoded categorically. This is intentionally
    defensive: an identifier-like string can never be passed straight through
    to XGBoost merely because pandas gave it an unexpected dtype.
    """
    numeric: list[str] = []
    categorical: list[str] = []

    for column in features:
        series = df[column]

        if pd.api.types.is_numeric_dtype(series.dtype):
            numeric.append(column)
        else:
            categorical.append(column)

    return numeric, categorical


def validate_feature_frame(
    df: pd.DataFrame,
    features: Sequence[str],
    model_name: str,
) -> None:
    """Fail early on unsupported nested/list/dict values."""
    unsupported: list[str] = []

    for column in features:
        sample = df[column].dropna().head(100)
        if sample.map(
            lambda value: isinstance(
                value,
                (dict, list, tuple, set),
            )
        ).any():
            unsupported.append(column)

    if unsupported:
        raise ValueError(
            f"{model_name}: unsupported nested feature columns: "
            f"{unsupported}"
        )


def build_pipeline(
    df: pd.DataFrame,
    features: Sequence[str],
    threads: int,
) -> tuple[Pipeline, list[str], list[str]]:
    numeric, categorical = classify_columns(
        df,
        features,
    )

    transformers: list[tuple[str, Any, list[str]]] = []

    if numeric:
        transformers.append(
            (
                "num",
                "passthrough",
                numeric,
            )
        )

    if categorical:
        transformers.append(
            (
                "cat",
                OneHotEncoder(
                    handle_unknown="ignore",
                    sparse_output=True,
                    dtype=np.float32,
                ),
                categorical,
            )
        )

    if not transformers:
        raise ValueError(
            "No numeric or categorical features available."
        )

    preprocessor = ColumnTransformer(
        transformers=transformers,
        remainder="drop",
        sparse_threshold=0.3,
        verbose_feature_names_out=True,
    )

    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.85,
        colsample_bytree=0.85,
        min_child_weight=3,
        reg_alpha=0.0,
        reg_lambda=1.0,
        objective="binary:logistic",
        eval_metric="logloss",
        tree_method="hist",
        n_jobs=threads,
        random_state=20260908,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    return pipeline, numeric, categorical


def evaluate(
    y_true: np.ndarray,
    probability: np.ndarray,
    threshold: float = 0.50,
) -> dict[str, Any]:
    prediction = (
        probability >= threshold
    ).astype(np.int8)

    matrix = confusion_matrix(
        y_true,
        prediction,
        labels=[0, 1],
    )

    tn, fp, fn, tp = matrix.ravel()

    return {
        "threshold": threshold,
        "roc_auc": float(
            roc_auc_score(
                y_true,
                probability,
            )
        ),
        "pr_auc": float(
            average_precision_score(
                y_true,
                probability,
            )
        ),
        "precision": float(
            precision_score(
                y_true,
                prediction,
                zero_division=0,
            )
        ),
        "recall": float(
            recall_score(
                y_true,
                prediction,
                zero_division=0,
            )
        ),
        "f1": float(
            f1_score(
                y_true,
                prediction,
                zero_division=0,
            )
        ),
        "confusion_matrix": {
            "tn": int(tn),
            "fp": int(fp),
            "fn": int(fn),
            "tp": int(tp),
        },
    }


def feature_importance(
    pipeline: Pipeline,
    limit: int = 20,
) -> list[dict[str, Any]]:
    preprocessor = pipeline.named_steps[
        "preprocessor"
    ]
    model = pipeline.named_steps["model"]

    names = np.asarray(
        preprocessor.get_feature_names_out(),
        dtype=object,
    )
    values = np.asarray(
        model.feature_importances_,
        dtype=float,
    )

    if len(names) != len(values):
        raise RuntimeError(
            "Transformed feature-name count does not match "
            "XGBoost feature-importance count."
        )

    order = np.argsort(values)[::-1][:limit]

    return [
        {
            "feature": str(names[index]),
            "importance": float(
                values[index]
            ),
        }
        for index in order
    ]


def save_preprocessor_metadata(
    pipeline: Pipeline,
) -> dict[str, Any]:
    preprocessor = pipeline.named_steps[
        "preprocessor"
    ]

    return {
        "transformed_features": [
            str(name)
            for name
            in preprocessor.get_feature_names_out()
        ],
    }


def train_one(
    name: str,
    train: pd.DataFrame,
    test: pd.DataFrame,
    extra_exclude: set[str],
    model_dir: Path,
    threads: int,
) -> dict[str, Any]:
    features = feature_columns(
        train,
        extra_exclude,
    )

    validate_feature_frame(
        train,
        features,
        name,
    )

    pipeline, numeric, categorical = (
        build_pipeline(
            train,
            features,
            threads,
        )
    )

    print(
        f"\nPreparing {name}: "
        f"{len(features)} input features "
        f"({len(numeric)} numeric, "
        f"{len(categorical)} categorical)"
    )

    if categorical:
        print(
            "Categorical encoding: "
            + ", ".join(categorical)
        )

    pipeline.fit(
        train[features],
        train[TARGET],
    )

    probability = pipeline.predict_proba(
        test[features]
    )[:, 1]

    metrics = evaluate(
        test[TARGET].to_numpy(),
        probability,
    )

    top_features = feature_importance(
        pipeline
    )

    # Save the native booster. Scoring code will reconstruct the preprocessing
    # from metadata rather than relying on a version-fragile pickle.
    booster_path = (
        model_dir
        / f"pdm_default_risk_{name}_v1.json"
    )

    pipeline.named_steps[
        "model"
    ].save_model(booster_path)

    # Canonical inference artifact: exact fitted preprocessing + classifier.
    pipeline_path = (
        model_dir
        / f"pdm_default_risk_{name}_v1.joblib"
    )
    joblib.dump(
        pipeline,
        pipeline_path,
        compress=3,
    )

    preprocessing_metadata = (
        save_preprocessor_metadata(
            pipeline
        )
    )

    metadata = {
        "model_name": (
            f"pdm_default_risk_{name}"
        ),
        "model_version": "v1",
        "synthetic_poc": True,
        "target": TARGET,
        "trained_at_utc": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "training_rows": int(
            len(train)
        ),
        "test_rows": int(
            len(test)
        ),
        "train_date_min": str(
            train[
                "observation_date"
            ].min().date()
        ),
        "train_date_max": str(
            train[
                "observation_date"
            ].max().date()
        ),
        "test_date_min": str(
            test[
                "observation_date"
            ].min().date()
        ),
        "test_date_max": str(
            test[
                "observation_date"
            ].max().date()
        ),
        "train_default_rate": float(
            train[TARGET].mean()
        ),
        "test_default_rate": float(
            test[TARGET].mean()
        ),
        "input_features": list(features),
        "numeric_features": numeric,
        "categorical_features": categorical,
        "excluded_fields": sorted(
            ALWAYS_EXCLUDE
            | extra_exclude
        ),
        **preprocessing_metadata,
        "metrics": metrics,
        "top_feature_importance": (
            top_features
        ),
        "booster_file": str(
            booster_path
        ),
        "pipeline_file": str(
            pipeline_path
        ),
    }

    metadata_path = (
        model_dir
        / (
            f"pdm_default_risk_"
            f"{name}_v1_metadata.json"
        )
    )

    metadata_path.write_text(
        json.dumps(
            metadata,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print()
    print(
        name.upper().replace(
            "_",
            " ",
        )
    )
    print("=" * 72)
    print(
        f"Train: {len(train):,}  "
        f"Test: {len(test):,}"
    )
    print(
        f"Train default rate: "
        f"{train[TARGET].mean():.2%}"
    )
    print(
        f"Test default rate : "
        f"{test[TARGET].mean():.2%}"
    )
    print(
        f"Dates: "
        f"{metadata['train_date_min']}.."
        f"{metadata['train_date_max']} "
        f"-> "
        f"{metadata['test_date_min']}.."
        f"{metadata['test_date_max']}"
    )
    print(
        f"ROC-AUC   : "
        f"{metrics['roc_auc']:.4f}"
    )
    print(
        f"PR-AUC    : "
        f"{metrics['pr_auc']:.4f}"
    )
    print(
        f"Precision : "
        f"{metrics['precision']:.4f}"
    )
    print(
        f"Recall    : "
        f"{metrics['recall']:.4f}"
    )
    print(
        f"F1        : "
        f"{metrics['f1']:.4f}"
    )
    print(
        f"Confusion : "
        f"{metrics['confusion_matrix']}"
    )

    print("\nTop features:")
    for row in top_features[:10]:
        print(
            f"  "
            f"{row['feature']:<60} "
            f"{row['importance']:.5f}"
        )

    print(
        f"\nSaved booster : "
        f"{booster_path}"
    )
    print(
        f"Saved pipeline: "
        f"{pipeline_path}"
    )
    print(
        f"Saved metadata: "
        f"{metadata_path}"
    )

    return metadata


def main() -> None:
    args = parse_args()

    if args.threads < 1:
        raise ValueError(
            "--threads must be at least 1"
        )

    args.model_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    df = load_data(
        args.input
    )

    train, test = temporal_split(
        df,
        args.test_fraction,
    )

    print(
        "PDM 90-DAY DEFAULT RISK "
        "— SYNTHETIC POC"
    )
    print("=" * 72)
    print(
        f"Input      : {args.input}"
    )
    print(
        f"Rows       : {len(df):,}"
    )
    print(
        f"Columns    : {len(df.columns):,}"
    )
    print(
        f"Default rate: "
        f"{df[TARGET].mean():.2%}"
    )
    print(
        f"Split      : temporal "
        f"{1.0 - args.test_fraction:.0%}/"
        f"{args.test_fraction:.0%}"
    )
    print(
        f"Threads    : {args.threads}"
    )

    baseline = train_one(
        name="baseline",
        train=train,
        test=test,
        extra_exclude=set(),
        model_dir=args.model_dir,
        threads=args.threads,
    )

    early_warning = train_one(
        name="early_warning",
        train=train,
        test=test,
        extra_exclude=EARLY_WARNING_EXCLUDE,
        model_dir=args.model_dir,
        threads=args.threads,
    )

    strict_early_warning = train_one(
        name="strict_early_warning",
        train=train,
        test=test,
        extra_exclude=STRICT_EARLY_WARNING_EXCLUDE,
        model_dir=args.model_dir,
        threads=args.threads,
    )

    comparison = {
        "synthetic_poc": True,
        "target": TARGET,
        "rows": int(len(df)),
        "temporal_test_fraction": (
            args.test_fraction
        ),
        "baseline": (
            baseline["metrics"]
        ),
        "early_warning": (
            early_warning["metrics"]
        ),
        "strict_early_warning": (
            strict_early_warning["metrics"]
        ),
    }

    comparison_path = (
        args.model_dir
        / "pdm_default_risk_v1_comparison.json"
    )

    comparison_path.write_text(
        json.dumps(
            comparison,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print()
    print("MODEL COMPARISON")
    print("=" * 72)
    print(
        "Baseline ROC-AUC      : "
        f"{baseline['metrics']['roc_auc']:.4f}"
    )
    print(
        "Baseline PR-AUC       : "
        f"{baseline['metrics']['pr_auc']:.4f}"
    )
    print(
        "Early-warning ROC-AUC : "
        f"{early_warning['metrics']['roc_auc']:.4f}"
    )
    print(
        "Early-warning PR-AUC  : "
        f"{early_warning['metrics']['pr_auc']:.4f}"
    )
    print(
        "Strict EW ROC-AUC     : "
        f"{strict_early_warning['metrics']['roc_auc']:.4f}"
    )
    print(
        "Strict EW PR-AUC      : "
        f"{strict_early_warning['metrics']['pr_auc']:.4f}"
    )
    print(
        f"\nComparison: "
        f"{comparison_path}"
    )
    print(
        f"Models written to: "
        f"{args.model_dir}"
    )


if __name__ == "__main__":
    main()
