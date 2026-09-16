# 07 - AI Default Risk Validation

Model purpose:

Predictive default-risk early warning.

Required semantic label:

`PREDICTIVE_DEFAULT_RISK_NOT_FRAUD_DETERMINATION`

The output must not be represented as a fraud determination.

## Model

Model identifier:

`pdm_default_risk_strict_early_warning_v1`

Persisted artefacts:

- joblib estimator
- model metadata

## Historical Gate 2 state

An earlier Gate 2 acceptance state contained:

213 AI-risk rows.

That count is historical and is not a target for regenerated current data.

## Current regenerated source state

As of the current regenerated PDM source state:

- operational loans: 250
- eligible/scored: 151
- repayment-event rows: 144
- not-yet-due rows: 7
- missing-history rows: 0
- current-state proxy rows: 0
- skipped non-disbursed: 99
- feature contract: 20 / 20

Current scorer result:

- rows: 151
- mean risk: 31.90%
- LOW: 102
- MEDIUM: 1
- HIGH: 0
- SEVERE: 48

Runtime output was remediated to aggregate risk summaries only.

No model retraining was performed.

Consumption validation:

- rows: 151
- unique tokens: 151
- null tokens: 0
- semantic label exact match

## Reproducibility follow-up

Current runtime scikit-learn version was observed as 1.9.1.

Earlier persisted-estimator warnings suggested 1.9.0.

The POC accepts this with a follow-up requirement to pin dependencies and
record training/runtime versions explicitly.

The model must not be casually retrained merely to remove a version warning.
