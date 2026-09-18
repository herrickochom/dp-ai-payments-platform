# DP-AI Payments Platform — Airflow

Airflow provides bounded batch and operational orchestration.

It does not replace Kafka.

## Event-driven boundary

Payment producers and consumers remain event-driven services.

Airflow does not start Kafka consumers for each DAG run and does not reset
Kafka offsets.

## Payment source generation

`payment-xml-generator` is an on-demand synthetic-source utility.

It MUST NOT run as part of every platform pipeline execution.

The source-generation DAG:

- has no schedule;
- requires an explicit manual trigger;
- accepts a bounded record count;
- does not clean existing source data by default;
- treats clean/regeneration as a separately authorised operation.

`payment-producer` is therefore not lifecycle-dependent on
`payment-xml-generator`.

## Main orchestration

The main DAG coordinates bounded downstream operations:

1. platform readiness
2. source/raw readiness
3. lakehouse transformation
4. data-quality validation
5. ML feature generation
6. ML scoring
7. reconciliation
8. acceptance

ML model training is not part of the normal pipeline.

## Explicit exclusions

Scheduled DAGs must not perform:

- Kafka offset resets
- live DLQ replay
- destructive lifecycle execution
- recovery/restore execution
- token-link rematerialisation
- ML retraining
- deletion of protected Gate 2 or Gate 3 evidence

## Execution boundary

Airflow workers must not receive unrestricted access to the host Docker
socket.

Platform jobs are invoked through an allowlisted execution boundary.
