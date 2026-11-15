# STR-003 Implementation Decisions

- Status: Recorded
- Date: 2026-10-14
- Ticket: STR-003
- Related: STR-003-output-contract.md §12

## Decision 1 — Watermark delay and late-event policy

Value: bounded out-of-orderness of 5 minutes.
Late events: routed to transitions only, not applied to state.
Counted by: `flink_j1_late_events_total`.
Rationale: provisional pending real lateness data.

## Decision 2 — Deterministic event-order comparator

Order: ascending (`event_timestamp`, `event_id`).
Tie-break: lexicographic on `event_id` string.
Rationale: matches contract §5 requirement.

## Decision 3 — Final dedup TTL

Value: 24 hours (current default, provisional).
Expiry condition: STR-002A-PROD confirms max lifecycle duration.
Rationale: matches current `J1_DEDUP_TTL_HOURS`.

## Decision 4 — Delivery guarantee

Value: at-least-once with deterministic `transition_id`.
Rationale: simpler than exactly-once for a development fixture;
downstream dedup by `transition_id` handles the retry case.

## Decision 5 — State/transition atomicity

Value: checkpoint-coordinated independent at-least-once writes. Both outputs
are derived from the same accepted event decision, and any serialization or
Kafka write failure fails the job. Recovery may repeat either write; the state
topic converges by `correlation_id`, and transition consumers deduplicate by
deterministic `transition_id`.

Atomicity guarantee: no strict both-or-neither guarantee is claimed. A failure
can occur after one topic acknowledges a record and before the other does.
Strict cross-topic atomicity requires a shared Kafka transaction or an
equivalent transactional publication design and is not provided by independent
at-least-once sinks.

Rationale: the proposed ordering of a transition acknowledgement before a
state write does not satisfy both-or-neither semantics; it permits a committed
transition without the corresponding state update. This decision instead
states the implementable development guarantee explicitly and relies on
failure-driven restart plus deterministic convergence, consistent with the
at-least-once choice in Decision 4 and the failure-evidence allowance in
contract §9.
