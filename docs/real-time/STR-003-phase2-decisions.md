# STR-003 Phase 2 Decisions

- Status: Recorded
- Date: 2026-10-14
- Ticket: STR-003
- Related: STR-003-implementation-decisions.md, STR-003-output-contract.md

## Decision A — Savepoint compatibility

Value: A1 — clean restart. Phase 2 extends the `last_seen` ValueState from four
fields to the fifteen the LifecycleState schema requires. This breaks restore
from any savepoint taken with the old shape.

Action: No restore from pre-Phase-2 savepoints. Take a fresh savepoint after
Phase 2 is verified.

## Decision B — Stage and status casing

Value: B1 — leave as-is. Stages are lowercase (e.g. `payment-routing`);
statuses are uppercase (e.g. `PENDING`). Both validate against the registered
schema. Record the convention; do not normalize silently.

## Decision C — Two-record output tagging

Value: C1 — tag with `__type`. Phase 2 emits two JSON records per accepted
event, tagged `"__type": "state"` and `"__type": "transition"`. Phase 3
filters by tag before routing to the two Kafka sinks.
