# Gate 3.6B — Offline Durable Replay Ledger

## Status

Gate 3.6B implements an isolated, metadata-only replay-control ledger for the
POC.

It does not replay Kafka messages and is not connected to live infrastructure.

## Purpose

The ledger provides durable replay reservation, duplicate suppression,
state-transition control and replay-attempt auditability before any future
Kafka replay executor is authorised.

The implementation deliberately separates replay governance from replay
execution.

DUPLICATE SCAN != DURABLE LEDGER

APPROVAL != EXECUTION

LEDGER RESERVATION != KAFKA REPLAY

## Implementation

Implementation:

`services/shared/replay_ledger.py`

Focused tests:

`tests/test_replay_ledger.py`

The POC persistence mechanism is SQLite.

The database path must be explicitly supplied by the caller. The
implementation has no implicit live ledger path and validation uses temporary
test databases.

No live Gate 2 or Gate 3 platform database was created by this phase.

## Canonical Replay Identity

A deterministic SHA-256 replay-control identity is calculated from canonical
non-sensitive replay coordinates:

- source topic
- source partition
- source offset
- target topic

Python `hash()` is not used.

This digest identifies a replay-control coordinate. It is not a beneficiary
identity and must not be repurposed as one.

## Durable Reservation

The ledger uses SQLite transaction and uniqueness controls, including
`BEGIN IMMEDIATE`, a primary replay identity and a unique replay request ID.

A competing reservation cannot independently authorise the same canonical
replay identity.

This is a local POC concurrency control. It is not represented as distributed
exactly-once processing.

## Durable Attempt Audit

Replay attempts are recorded separately from the primary reservation.

A successful reservation records a metadata-only REQUESTED attempt.

When a prior reservation already exists for the canonical replay identity, the
new attempt is blocked and durably recorded as:

`SUPPRESSED_DUPLICATE`

with a non-sensitive result code.

This provides audit evidence of the rejected attempt without reopening or
overwriting the existing replay reservation.

## Replay States

The implemented state contract is:

- REQUESTED
- APPROVED
- IN_PROGRESS
- SUCCEEDED
- FAILED
- SUPPRESSED_DUPLICATE
- BLOCKED_POLICY

Allowed transitions are enforced explicitly.

A terminal SUCCEEDED replay cannot be silently reopened.

Invalid transitions fail closed.

SUCCEEDED means that the ledger was explicitly told that execution succeeded.
The ledger itself does not produce Kafka messages.

## Fail-Closed Controls

Reservation fails closed when required replay metadata or governance state is
missing or unresolved, including:

- missing replay request identity
- missing replay batch identity
- missing source topic
- missing source partition
- missing source offset
- missing target topic
- unresolved policy
- absent required approval
- unavailable durable ledger
- existing reservation for the same canonical replay identity

## Privacy Contract

The ledger is metadata-only.

Protected fields are explicitly rejected, including payload variants,
message values, identity-bearing Kafka keys, beneficiary identifiers,
beneficiary tokens, NIN, account numbers, wallet numbers, exception text and
tracebacks.

The implementation contains no Kafka client and no network or infrastructure
client.

## Validation

Focused replay-ledger validation passed:

- 37 replay-ledger tests passed
- deterministic canonical replay identity verified
- durable SQLite persistence verified
- competing reservation suppression verified
- duplicate-attempt audit persistence verified
- fail-closed request validation verified
- allowed and invalid state transitions verified
- terminal SUCCEEDED protection verified
- prohibited metadata rejection verified
- absence of replay/infrastructure execution capability verified

Existing privacy regression validation is performed separately as part of
Gate 3.6B final acceptance.

## Infrastructure Boundary

Gate 3.6B performs no:

- Kafka production
- Kafka consumption or replay
- consumer offset reset
- topic mutation
- Docker Compose operation
- dbt operation
- MinIO mutation
- Nessie mutation
- Iceberg mutation
- ML execution

The ledger is not wired into the existing manual DLQ replay utility in this
phase.

## POC Limitations

SQLite demonstrates durable local persistence, transaction-based reservation
and uniqueness semantics suitable for this POC.

It does not establish distributed exactly-once replay semantics.

A production implementation would require an approved durable service,
production access control, availability and backup controls, operational
ownership, monitoring, and transaction semantics appropriate to the deployed
replay architecture.

Any live replay integration requires a separate controlled phase and explicit
authorisation.

## Gate 3.6B Boundary

Gate 3.6B proves the replay-control and durable-ledger layer in isolation.

Gate 3.6C may perform synthetic/offline decision and state-machine
demonstration only.

Live Kafka replay is outside this acceptance boundary.
