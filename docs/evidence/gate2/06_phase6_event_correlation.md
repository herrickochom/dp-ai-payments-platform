# 06 - Phase 6 Event Correlation

## Event semantics

Phase 6 uses project-specific observability/correlation events.

- VPM: pain.001 business initiation
- PMN: pain.001-associated technical routing/orchestration
- PSN: pain.002 business status/outcome
- PLM: pain.002-associated provider/final-mile technical event

PMN specialised fields include route/origin/intermediary decision and
submission/validation attributes.

PLM specialised fields include provider/network, beneficiary operational
reference, wallet/provider transaction reference, credit and cash-out
status.

`XBeneficiarySa` belongs to PLM, not PMN.

## Privacy treatment

PLM beneficiary operational identity may exist in controlled Bronze for
source/operational fidelity.

It must not flow into ordinary Silver, Gold or Consumption outputs.

The Silver correction removed PLM `x_beneficiary_sa` from ordinary
technical-event and lifecycle analytical models.

## Historical state

An earlier Phase 6 state recorded:

- total technical events: 476
- matched: 426
- explicitly unmatched: 50

This is retained as historical evidence.

## Current regenerated state

Current deterministic correlation using END_TO_END_ID produced:

- total technical events: 476
- unique technical_event_id: 476
- MATCHED: 476
- UNMATCHED: 0
- correlation key: END_TO_END_ID
- one-to-one correlation within each stream

The current state is not manipulated to recreate the historical 426/50
distribution.

## PMN/PLM preservation

Raw:

- PMN: 238
- PLM: 238

Staging/Bronze specialisation checks passed with all expected specialised
fields populated.

The validated Bronze state is frozen for Gate 2.
