# STR-003 Output Contract — Lifecycle State Topic

- Status: Approved 2026-10-14
- Date: 2026-10-14
- Ticket: STR-003
- Related: ADR-STR-001-003, STR-002A-DEV-event-contract.md,
  STR-002A-checkpoint-A.md, STR-002B-smoke-test.md, plan-status.md,
  file-plan.md
- Approvers: streaming architect; payment platform owner
- Source note: no committed fuller draft was recovered. This document expands
  the existing 143-line draft using repository evidence, the live read-only
  Schema Registry inventory, and supplied acceptance expectations. It does not
  claim to reproduce an original historical draft.

## 1. Purpose and scope

This document governs the output boundary for J1, currently implemented as the
development skeleton at
`platform/flink/jobs/j1_lifecycle_state_skeleton.py`. J1 consumes the two raw
technical-event topics fixed by STR-002A-DEV §8.1, re-keys decoded events by
`correlation_id`, maintains keyed lifecycle state, suppresses repeated
`event_id` values within each key, and will publish two governed Kafka outputs:

- `payments.lifecycle.state.v1`: the latest accepted lifecycle state per
  correlation identifier.
- `payments.lifecycle.transitions.v1`: the append-only history of accepted
  distinct lifecycle events, including post-terminal observations.

This is the Kafka → Flink → Kafka operational route. Kafka retained history and
the governed derived topics preserve the Kappa responsibility: state can be
recovered from checkpoints and rebuilt from replayable input. The Lambda
responsibility remains the separate historical lakehouse and downstream
analytical path. J1 must not synchronously query or write the lakehouse, Trino,
Nessie, Superset, or a service-owned database.

The contract closes the design gate for STR-002B/B2 item 3 but does not close
the item itself. The sinks are not implemented, the approval gate is open, and
STR-003 still owns the transition state machine, watermarks, late-event policy,
deduplication TTL approval, J2 gap detection, and unit tests.

This contract is DEVELOPMENT-ONLY under STR-002A-DEV. Its source is synthetic;
surfaces built from it must not be presented as production telemetry.

## 2. Authoritative inputs and boundaries

J1's input contract remains STR-002A-DEV §8.1:

- Topics: `icmn.pmn.pain001` and `cpo.plm.pain002`.
- Consumer group: `flink-lifecycle-consumer`, configurable via `J1_GROUP_ID`.
- Wire format: Confluent-framed Avro decoded by the Table API
  `avro-confluent` format and Schema Registry.
- Canonical keyed-state identifier: non-empty `correlation_id`; the literal
  `unknown` is rejected.
- Deduplication identity: `event_id` within `correlation_id`.

The current skeleton parses the JSON-encoded `parsed_event_data` field inside
the decoded Avro envelope. That inner JSON parse is not wire deserialization.
The skeleton writes only to stdout today; this contract authorizes no runtime
change by itself.

## 3. Kafka output topics

### 3.1 Current-state topic

| Attribute | Contract |
|---|---|
| Topic | `payments.lifecycle.state.v1` |
| Partitions | 6 |
| Replication | 1 in the current development manifest |
| Cleanup | `compact` |
| Retention | `-1` (infinite) |
| Kafka key | UTF-8 `correlation_id` |
| Value subject | `payments.lifecycle.state.v1-value` |
| Value record | `com.payments.platform.LifecycleState` |

The topic is an upsert projection of the latest governed state. Compaction
retains the latest value eventually; it does not mean J1 produces only one
physical Kafka record per key, nor does it guarantee that consumers never see
older records before compaction. Consumers that need a current view must
resolve records by key and accepted event order. Tombstones are not defined
here and remain STR-004 scope.

All writes for one `correlation_id` use the same Kafka key. With the current
six-partition manifest, Kafka's key partitioning keeps records for a key in one
partition. Consumers must not infer ordering across different keys or across
partitions.

### 3.2 Transitions topic

| Attribute | Contract |
|---|---|
| Topic | `payments.lifecycle.transitions.v1` |
| Partitions | 6 |
| Replication | 1 in the current development manifest |
| Cleanup | `delete` (manifest default) |
| Retention | 2,592,000,000 ms (30 days) |
| Kafka key | UTF-8 `correlation_id` |
| Value subject | `payments.lifecycle.transitions.v1-value` |
| Value record | `com.payments.platform.LifecycleTransition` |

The transitions topic is append-only within its retention window. It preserves
one logical transition observation for each distinct accepted input event.
`transition_id` must be deterministic from `(correlation_id, event_id)` (UUID
v5 or an equivalently stable derivation) so downstream consumers can suppress
duplicates caused by retry, replay, or an at-least-once sink.

Consumers must use `event_timestamp` plus a deterministic tie-breaker such as
`event_id` when reconstructing event order. Kafka partition order is arrival
order for the key, not proof of event-time order.

## 4. Avro and Schema Registry contracts

Read-only registry verification on 2026-10-14 found:

| Subject | Subject version | Registry schema ID | Record |
|---|---:|---:|---|
| `payments.lifecycle.state.v1-value` | 1 | 2 | `LifecycleState` |
| `payments.lifecycle.transitions.v1-value` | 1 | 1 | `LifecycleTransition` |

The numeric registry ID identifies registered schema content in this registry;
it is not the subject version and is not the value of the record's
`schema_version` field. Producers use the subject and registered schema;
consumers must decode the Confluent wire schema ID rather than hard-code IDs 1
or 2 as portable constants.

The registry's global compatibility mode was read-only verified as
`BACKWARD_TRANSITIVE`. Subject evolution must therefore remain readable by all
earlier compatible readers. Additive optional fields require a union beginning
with `null` and a default of `null`; other additive fields require a valid Avro
default. Removing, renaming, or incompatibly retyping an existing field is not
permitted without a new governed contract and migration plan. Compatibility
must be checked before registration, and serialized values must be validated
against the intended subject in tests.

### 4.1 `LifecycleState`

| Field | Type | Meaning |
|---|---|---|
| `correlation_id` | string | Logical state key; must equal the Kafka key. |
| `current_stage` | string | Latest accepted governed stage. |
| `current_status` | string | Status of the latest accepted state event. |
| `last_event_id` | string | Event that produced the current state. |
| `last_event_timestamp` | string | Producer event-time for that event. |
| `last_processing_timestamp` | string | J1 wall-clock processing time. |
| `first_seen_at` | string | J1 wall-clock time when this state key was first created. |
| `updated_at` | string | J1 wall-clock time of the current state write. |
| `stage_history` | array<string> | Order-preserving distinct accepted stages. |
| `retry_count` | int | Monotonic count of accepted `retrying` events. |
| `sla_deadline` | null|string, default null | Uncomputed until STR-005. |
| `sla_breached` | boolean, default false | Uncomputed until STR-005. |
| `end_to_end_id` | null|string, default null | Optional source identifier. |
| `instruction_id` | null|string, default null | Optional source identifier. |
| `schema_version` | string, default `1.0.0` | Application contract version, not registry ID. |

The current skeleton does not yet retain or emit every required field. Sink
implementation must extend governed state and mapping before publication.

### 4.2 `LifecycleTransition`

| Field | Type | Meaning |
|---|---|---|
| `transition_id` | string | Stable identity derived from correlation and event IDs. |
| `correlation_id` | string | Logical key; must equal the Kafka key. |
| `event_id` | string | Source event identity. |
| `from_stage`, `from_status` | null|string | Prior accepted state; null on first event. |
| `to_stage`, `to_status` | string | Stage and status observed on the event. |
| `event_timestamp` | string | Producer event-time. |
| `processing_timestamp` | string | J1 wall-clock processing time. |
| `source_topic` | string | Input provenance. |
| `source_partition` | int | Input provenance. |
| `source_offset` | long | Input provenance. |
| `job_id` | string | Flink job identity for operational traceability. |
| `schema_version` | string, default `1.0.0` | Application contract version. |

The current Table projection does not carry Kafka topic, partition, or offset,
so implementation must add governed metadata extraction before it can satisfy
this schema. This is an implementation gate, not permission to invent values.

## 5. Time, ordering, and late events

`last_event_timestamp` and transition `event_timestamp` are event-time asserted
by the source and are replay-stable for the same input. `first_seen_at`,
`updated_at`, `last_processing_timestamp`, and transition
`processing_timestamp` are processing-time values generated by J1 and are not
inherently replay-stable.

All timestamp strings must be normalized to RFC 3339 / ISO 8601 UTC before
serialization. Downstream ordering, comparison, and replay acceptance must use
event-time plus a deterministic tie-breaker. Tests must compare deterministic
business fields separately from processing timestamps; byte-for-byte state
values are not expected to match after a clean replay.

This document does not approve a watermark delay or a general late-event merge
policy. Until those remaining STR-003 decisions are approved, the sink must not
silently let an older event overwrite a newer state merely because it arrived
later. The event-order comparator and late-event route require explicit tests.
Transitions may preserve the observation even when it is not allowed to change
current state.

## 6. Idempotency and duplicate handling

J1 currently uses `MapState<String, Boolean>` named `seen_event_ids`, keyed by
`correlation_id`, with a configurable 24-hour TTL and
`OnCreateAndWrite`/`NeverReturnExpired` semantics. A contained event ID is
counted by `flink_j1_duplicate_events_total` and produces neither a state
change nor an output record.

The 24-hour value is provisional until the production maximum lifecycle and
replay horizons are approved. After expiry, the same event ID can be accepted
again, so the output contracts also require stable identifiers and downstream
idempotency. State upserts use the correlation key; transition consumers dedup
by `transition_id`.

## 7. Terminal-state behavior

The evidenced terminal statuses are `failed` and `timed_out`. A completed
`agent-cashout` stage is also a terminal lifecycle condition under
STR-002A-DEV §7.7.

The first accepted event that makes a key terminal is written to both topics.
Its state value remains the authoritative state-topic value. A later distinct
event for an already terminal key must not overwrite `current_stage`,
`current_status`, `stage_history`, or the terminal event identity in the state
topic. It is recorded in transitions as an observation and increments
`flink_j1_terminal_state_events_total`.

An exact duplicate `event_id` remains suppressed before terminal handling. A
late or out-of-order distinct event is preserved in transitions but may change
state only under the separately approved event-order and late-event policy.
This contract introduces no additional precedence between different terminal
statuses because repository evidence does not support one.

## 8. Flink state and savepoint compatibility

The current state descriptors are part of J1's durable compatibility contract:

- `ValueState<String>` named `last_seen`.
- `MapState<String, Boolean>` named `seen_event_ids`, with TTL metadata.

Changing descriptor names, key/value serializers, state type, operator UID, or
state topology can make existing checkpoints and savepoints unrestorable.
Such a change requires a reviewed migration plan, compatible serializer
evolution or State Processor API migration, and a recovery test. Discarding
state and rebuilding from retained Kafka input is a separate governed decision.

The supplied 2026-10-14 incident record reports that job `c5f16ba...` failed
with `StateMigrationException` against
`savepoint-acd4d3-c410490ac202`. That incident is not present in committed
repository evidence and remains externally supplied, but it is consistent with
the serializer risk. The savepoint is explicitly prohibited for restore. This
task performs no restore. A future recovery test must use a fresh savepoint
created by the approved serializer version.

## 9. Delivery, checkpoint, and failure semantics

Flink's configured `CheckpointingMode.EXACTLY_ONCE` provides exactly-once
managed-state semantics; it does not by itself make Kafka output exactly-once.
No Kafka sink exists in J1 today, so no end-to-end delivery guarantee is
currently implemented.

Flink 1.18's Kafka sink technically supports transactional exactly-once
delivery. The implementation must explicitly select
`DeliveryGuarantee.EXACTLY_ONCE` (or Table sink
`sink.delivery-guarantee=exactly-once`), configure a stable transactional ID
prefix unique to the running job, and keep checkpointing enabled. Kafka
transaction timeout must exceed worst-case checkpoint plus restart duration,
and consumers that require committed-only visibility must use
`isolation.level=read_committed`.

If any prerequisite cannot be demonstrated, the declared guarantee is
at-least-once. Under at-least-once delivery, retries can physically duplicate
records: state remains convergent by key and event ordering, while transition
consumers must suppress repeated deterministic `transition_id` values.

State and transition outputs belong to one logical event decision. The sink
design must prove whether both writes share one checkpoint-coordinated atomic
boundary. Partial publication must fail the job or emit bounded failure
evidence; it must not be silently treated as success. Serialization errors and
asynchronous Kafka send failures must be observable and cause the configured
restart/failure policy rather than dropping a record.

## 10. Observability contract

Implementation must expose bounded, low-cardinality metrics consistent with
the existing `flink_j1_*` prefix:

| Metric | Requirement |
|---|---|
| `flink_j1_envelopes_seen_total` | Existing raw-envelope count. |
| `flink_j1_events_seen_total` | Existing accepted distinct-event count. |
| `flink_j1_duplicate_events_total` | Existing duplicate suppression count. |
| `flink_j1_state_records_total` | Existing count of keys first observed; not a sink-write count. |
| `flink_j1_state_writes_total` | Successful state records acknowledged under the chosen guarantee. |
| `flink_j1_transition_writes_total` | Successful transition records acknowledged. |
| `flink_j1_kafka_write_failures_total` | Kafka publication failures, with bounded topic label if needed. |
| `flink_j1_serialization_failures_total` | Avro/key serialization failures. |
| `flink_j1_terminal_state_events_total` | Distinct post-terminal observations not applied to state. |

Flink checkpoint metrics must also demonstrate completed/failed checkpoints,
duration, age of the latest completed checkpoint, restart count, and recovery
status. Kafka sink transaction and producer error metrics should be retained
where the connector exposes them. Identifiers such as correlation ID, event ID,
job ID, partition, or offset must not be metric labels.

## 11. Frozen acceptance baseline

The following figures are supplied expectations, not independently verified by
this documentation task:

| Measurement | Expected | Contract interpretation |
|---|---:|---|
| Raw envelopes | 1,428 | Input rows observed before inner-event dedup. |
| Logical events | 476 | Distinct accepted `(correlation_id, event_id)` values. |
| Distinct state keys | 238 | Correlation IDs first observed. |
| Duplicate envelopes | 952 | Envelopes suppressed as repeated event IDs. |
| Transition emissions | 476 | One logical transition per distinct accepted event. |
| Kafka write failures | 0 | Required result once sinks exist. |

`238` is not the number of state-topic writes implied by the present skeleton.
Two accepted events per key can produce up to `476` logical state updates before
terminal/order rules, while compaction eventually retains one current value per
key. Physical writes can differ again under retry or at-least-once delivery.
Therefore the earlier shorthand “238 state-topic records” is ambiguous. The
sink acceptance plan must separately measure distinct keys, logical state
updates, producer acknowledgements, records visible before compaction, and the
compacted current view.

The current skeleton's `flink_j1_state_records_total` increments only when a
key has no prior `ValueState`; it measures first-seen keys, not successful Kafka
writes. No sink acceptance test is run in this documentation session.

## 12. Implementation and acceptance gates

Before sink implementation can be accepted:

1. Both approvers accept this contract or record explicit conditions.
2. Event ordering, watermark, and late-event policies are approved and tested.
3. J1 state is extended without violating the migration contract.
4. Source topic/partition/offset metadata is available for transition values.
5. Both Avro mappings pass local schema validation and Registry compatibility.
6. Delivery guarantee and transactional configuration are explicit and tested
   through failure and restart scenarios.
7. State/transition publication consistency is demonstrated.
8. The frozen baseline is reported using the distinct measurements in §11.
9. Checkpoint restore uses a newly compatible savepoint, never the prohibited
   savepoint in §8.
10. Unit and integration evidence is added without mutating frozen source data.

## 13. Approvals

| Role | Approval decision | Approval date | Outstanding conditions |
|---|---|---|---|
| Streaming architect | Approved | 2026-10-14 | Ordering, state compatibility, delivery, and acceptance gates reviewed and accepted as written. |
| Payment platform owner | Approved | 2026-10-14 | Development output semantics and production-boundary caveats confirmed as written. |

**Approval note.** Approval of this contract closes the design gate
for STR-002B/B2 item 3. It does not close the item itself. The ten
implementation and acceptance gates in §12 remain open and must be
satisfied with evidence before sink implementation is accepted. The
savepoint `savepoint-acd4d3-c410490ac202` remains prohibited for
restore per §8.

The contract is not binding for sink implementation until both required
approvers record a decision. Approval of this document does not close
STR-002B/B2 item 3; implementation and verification remain required.

## 14. Change history

| Date | Change | Sections | Approval impact |
|---|---|---|---|
| 2026-10-14 | Initial short draft created. | all | Draft; no approvals recorded. |
| 2026-10-14 | Reconstructed and completed from repository evidence; added delivery, state-compatibility, observability, baseline reconciliation, and approval gates. | all | Full review required. |
| 2026-10-14 | Approval recorded. | 13, 14 | Closed. |

## 15. Implementation verification — 2026-10-15

This section records runtime evidence obtained after the contract approval.
It does not supersede the approved requirements or constitute approval of
all ten implementation acceptance gates in section 12.

### Kafka output binding

J1 publishes to:

- `payments.lifecycle.state.v1`, Confluent Avro schema ID 2.
- `payments.lifecycle.transitions.v1`, Confluent Avro schema ID 1.

Both outputs use the UTF-8 `correlation_id` Kafka key. The observed
output snapshot contained 476 state records across 238 distinct keys
and 476 transition records.

### Source-to-output reconciliation

A read-only reconciliation compared transition provenance against
the original Kafka records in `icmn.pmn.pain001` and
`cpo.plm.pain002`, using source topic, partition and offset.

Verified results:

| Measurement | Actual |
|---|---:|
| Source physical envelopes | 1,428 |
| Distinct logical source events | 476 |
| Published transitions | 476 |
| Distinct published transition identities | 476 |
| Missing transitions | 0 |
| Unexpected transitions | 0 |
| Duplicate logical transitions | 0 |
| Source/output field mismatches | 0 |

The comparison verified source `event_id`, inner `correlation_id`,
technical stage, technical status, event timestamp and output Kafka key.

Result: `SOURCE_TO_OUTPUT_RECONCILIATION=PASS`.

### Regression verification

The post-Row-change J1 schema-binding regression runner completed
17 tests with zero failures or errors and process exit code 0.

### Acceptance boundary

This evidence verifies the implemented development Kafka output binding,
schema serialization, key mapping, transition identity and source
provenance for the observed snapshot.

It does not establish completion of STR-003 section 12 as a whole.
Ordering and late-event policies, terminal-state handling, delivery
guarantees, failure/restart behavior, publication atomicity and
compatible recovery remain subject to their respective acceptance
requirements. The development-only boundary remains unchanged.
