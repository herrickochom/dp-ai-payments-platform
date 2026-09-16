# 11 - Gate 2 Final Acceptance

Status: GATE 2 - FROZEN

Evidence date: 2026-09-16

## Baseline

Git branch:

`main`

Gate 2 implementation freeze commit:

`ba080c74b351ce995649b2e3cd7b2d340bb2aca5`

Nessie branch:

`main`

Frozen Nessie data-state baseline:

`ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`

Pre-Gate2 recovery reference:

`f85893b23c441db5abd0d2400da134cbb5fc4d0cb52a1e3c24e3ae02d85b35df`

Recovery tag:

`pre-gate2-20260915T155541Z`

## Acceptance results

### Data protection

PASS

Final automated privacy suite:

13 / 13 PASS

### Tokenisation

PASS

Tokeniser tests:

9 / 9 PASS

Restricted token-link materialisation:

250 rows / 250 unique canonical IDs / 250 unique canonical tokens /
0 null canonical tokens

### Kafka-derived Bronze lineage

PASS

25 / 25 Kafka-derived Bronze tables passed row, lineage, offset and event
uniqueness checks.

### Phase 6

PASS for current regenerated state.

476 technical events / 476 unique technical event IDs /
476 MATCHED / 0 UNMATCHED.

Historical 426 MATCHED / 50 UNMATCHED remains historical evidence.

### AI default risk

PASS for current regenerated state.

151 scored rows / 151 unique tokens / 0 null tokens.

Semantic contract:

`PREDICTIVE_DEFAULT_RISK_NOT_FRAUD_DETERMINATION`

Historical 213-row state remains historical evidence.

### RBAC

PASS

Least-privilege checks demonstrate that ordinary Trino, Metabase and
agent-api identities cannot access the restricted token-link layer.

### Geography

PASS

Current parish distribution:

13 LOW / 25 MEDIUM

Current alerts:

25 / 25 unique alert parishes

Missing ISO:

0

Missing coordinates:

0

Floating-point boundary and hierarchy-test defects corrected.

### Silver heterogeneous payment grain

PASS

`record_identifier` is the canonical heterogeneous record key.

Legitimate pain.001/VPM NULL transaction IDs are preserved.

### Gold payment grain

PASS

Targeted model rebuild:

1 / 1 PASS

Targeted tests:

8 / 8 PASS

Physical validation:

- rows: 1103
- unique payment_sk: 1103
- null payment_sk: 0
- legitimate null transaction_id: 476

### Gold beneficiary dimension

PASS

Mandatory Unknown member intentionally has no beneficiary token/version.

Stale token-version not-null test removed.

Targeted tests:

10 / 10 PASS

### Recovery

PASS

Pre-Gate2 Nessie recovery reference and MinIO backup sets remain available
and verified.

## Historical failures retained

Acceptance does not erase earlier failures.

The record includes:

- privacy-test failures corrected and retested;
- audit harness authentication failures;
- failed ML Compose profile attempts;
- token-link materialisation attempts that failed without mutation;
- Agent ingestion failure and controlled replay;
- AI Bronze accidental drop/restore;
- geography hierarchy and floating-point defects;
- broad dbt selector expansion;
- stale Silver transaction-id test;
- stale Gold Unknown-member token-version test;
- Gold payment surrogate-key collision.

Each acceptance-blocking defect identified above was either corrected and
retested or explicitly classified as a historical/POC limitation.

## POC residual items

These are follow-up engineering items rather than reasons to manipulate the
accepted data state:

- pin and record ML training/runtime dependency versions;
- productionise secret management/KMS;
- production TLS and enterprise IAM;
- production encryption and operational monitoring;
- complete BI/agent connectivity checks where required for the demo.

## Freeze rule

After the final repository/evidence checks and freeze commit:

- no transformation/privacy-contract changes;
- no Bronze changes;
- no Kafka replay;
- no token-link rematerialisation;
- no broad dbt rebuild;
- no recovery-tag movement;
- no backup mutation;

unless a genuine acceptance-blocking defect is discovered and the Gate 2
freeze is explicitly reopened.

## Final declaration

The validated implementation is frozen at Git commit:

`ba080c74b351ce995649b2e3cd7b2d340bb2aca5`

The corresponding accepted Nessie data-state baseline is:

`ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`

Gate 2 status:

`GATE 2 - FROZEN`
