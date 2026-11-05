# GCP Payments Local Integration Evidence

Status: SYNTHETIC LOCAL SIMULATION
Cloud deployment: NOT AUTHORISED

## Purpose

Exercise bounded payment admission, execution identity,
state persistence, simulated failure, recovery and
acceptance evidence without cloud access.

## Implemented

- Reuses the Stage 5 bounded manifest validator.
- Reuses deterministic execution identifiers.
- Stores simulated execution state in SQLite.
- Prevents repeat acceptance of the same manifest.
- Simulates failure before acceptance.
- Supports recovery of a recorded FAILED state.
- Persists synthetic acceptance evidence.
- Provides offline integration tests.

## Limitations

This is not an actual GCP end-to-end integration.

No GCS object bytes or generations are independently verified.
No Dataflow jobs are submitted.
No BigQuery records are reconciled.
No Airflow tasks are executed.
No production PostgreSQL queue is integrated.

The simulated FAILED-to-ACCEPTED transition does not
establish lease-based recovery, orphan reconciliation,
automatic scheduling, or exactly-once cloud execution.

The synthetic object count is not a payment record count.

A crash leaving a RUNNING row requires an explicit
reconciliation mechanism, which is not implemented.

## Deployment decision

BLOCKED.

The existing Composer deployment gate remains authoritative
only as an offline assessment tool, not an enforced
cloud deployment control.

## Next required work

1. Verify GCS object generations and checksums.
2. Integrate the existing PostgreSQL durable queue.
3. Replace wildcard Dataflow input with bounded inputs.
4. Implement stable cloud job submission and reconciliation.
5. Verify actual BigQuery landing and quarantine.
6. Execute Airflow runtime integration tests.
7. Complete IAM, network and billing approvals.
