# Gate 3.6C — Offline Replay-Control Dry Run

## Status

Gate 3.6C demonstrates the Gate 3.6B replay-control ledger using synthetic
metadata only.

No Kafka replay or live infrastructure interaction is performed.

## Purpose

The dry run demonstrates that replay governance can be evaluated before any
future replay executor is permitted to act.

The demonstration uses the existing Gate 3.6B replay ledger rather than
reimplementing replay decision logic.

## Implementation

Demonstration:

`services/shared/replay_ledger_demo.py`

Focused tests:

`tests/test_replay_ledger_demo.py`

The demonstration is executed from the repository root as:

`.venv/bin/python -m services.shared.replay_ledger_demo`

The earlier direct-script invocation failed because the repository root was
not on the Python module import path. No replay-ledger defect or infrastructure
mutation resulted from that failed invocation.

## Synthetic Scenarios

Ten synthetic scenarios were demonstrated:

1. valid replay reservation
2. approval state transition
3. transition to IN_PROGRESS
4. synthetic SUCCEEDED transition
5. duplicate reservation suppression
6. durable duplicate-attempt audit
7. missing approval blocked
8. unresolved policy blocked
9. protected metadata blocked
10. terminal SUCCEEDED replay protected from reopening

All ten scenarios passed.

## Simulation Result

Observed result:

- TOTAL_SCENARIOS=10
- PASSED=10
- FAILED=0
- KAFKA_MESSAGES_REPLAYED=0
- OFFSETS_RESET=0
- LIVE_INFRASTRUCTURE_CONNECTIONS=0
- DURABLE_LIVE_LEDGER_CREATED=0

The SQLite database used by the demonstration exists only inside a temporary
directory created for the synthetic run.

## Focused Test Evidence

Gate 3.6B replay-ledger tests and Gate 3.6C demonstration tests were executed
together.

Result:

- 47 tests passed
- 0 tests failed

## Safety Boundary

This demonstration does not:

- produce Kafka messages
- consume or replay DLQ messages
- reset Kafka offsets
- alter consumer groups
- change topic configuration
- connect to MinIO
- connect to Nessie
- connect to Trino
- connect to DuckDB
- execute dbt
- execute Docker Compose
- execute ML
- expire Iceberg snapshots
- remove orphan files
- perform lifecycle GC

## Interpretation

DRY RUN != REPLAY

APPROVAL != EXECUTION

LEDGER RESERVATION != KAFKA REPLAY

SUPPRESSED_DUPLICATE demonstrates replay-control suppression and durable
attempt auditing. It does not claim distributed exactly-once processing.

SUCCEEDED in this demonstration means that synthetic control flow explicitly
told the ledger that execution succeeded. The ledger did not execute a Kafka
replay.

## POC Limitation

Gate 3.6C validates replay-control behaviour offline.

The existing manual DLQ replay utility remains outside this implementation
boundary and is not wired to the ledger in this phase.

Any live replay integration or demonstration requires separate explicit
authorisation and additional operational safeguards.
