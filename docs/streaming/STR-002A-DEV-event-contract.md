# STR-002A-DEV Development Event Contract

- Status: Approved 2026-10-14
- Date: 2026-10-12
- Last revised: 2026-10-13 (§8.1 J1 input contract decided)
- Ticket: STR-002A-DEV
- Related: ADR-STR-001-003, STR-002A Checkpoint A report,
  Kafka -> Flink -> Kafka directive, STR-001 discovery report
- Approver(s): platform architecture owner; payment platform owner
- Supersedes: none
- Superseded by: none (this contract remains in force until STR-002A-PROD
  produces the production event contract)

## 1. Purpose and scope

This document defines the development-time event contract that the
streaming route will consume and process from STR-002B/B2 through
STR-009. It covers the source topics, consumer group, schema subject,
canonical correlation key, stage vocabulary, status vocabulary, and
transition rules that the Flink jobs (J1 through J5) and the projection
writer will operate against.

This contract is deliberately DEVELOPMENT-ONLY. It is based on synthetic
events produced by the existing `payment-xml-generator` services. It is
not a production event contract. STR-002A-PROD produces the production
contract once real emitters are confirmed. Until then, every consumer of
this contract must treat its inputs as synthetic and must label them as
such wherever they appear in dashboards, alerts, or reports.

The contract does not authorize the creation of any new Kafka topic,
schema subject, PostgreSQL table, Flink job, or Docker service. It
records the interface that later tickets (STR-002B/B2, STR-003 onward)
will implement against.

The J1 input contract — which topics J1 reads, under what consumer
identity, and for how long the raw topics are treated as a development
fixture — is decided and recorded in §8.1. Read §8 before reading §3 or
§5, because §8.1 governs both.

## 2. Source topics

Flink consumes the two existing technical-lifecycle topics. No new input
topic is created by this contract.

| Topic | Partitions | Replication | Retention | Cleanup policy |
|---|---|---|---|---|
| `icmn.pmn.pain001` | 6 | 1 | 7 days | delete |
| `cpo.plm.pain002` | 6 | 1 | 7 days | delete |

Both are declared in `platform/kafka/topics.yaml`. Both carry the same
Avro record type (`TechnicalPaymentEvent`) and are produced by the same
producer service (`services/payment-producer/kafka_producer.py`).

The two topics differ in producer system only:

- `icmn.pmn.pain001` -- produced by the ICMN/PMN generator, describes
  PMN technical lifecycle events.
- `cpo.plm.pain002` -- produced by the CPO/PLM generator, describes PLM
  technical lifecycle events.

Both topic families together cover the two stages of the synthetic
payment journey. They must be consumed together; neither alone carries
the full lifecycle.

## 3. Consumer contract

Flink consumes both source topics under a single, isolated consumer
group.

| Attribute | Value |
|---|---|
| Consumer group ID | `flink-lifecycle-consumer` (configurable via `J1_GROUP_ID`) |
| Starting offsets | `earliest` on first start |
| Offset commit mode | Flink checkpointed offsets only; no auto-commit |
| Isolation from existing consumers | Distinct group ID from `payment-events-consumer` |
| Topic subscription | Both topics, single subscription |
| Assignment strategy | Standard Kafka range assignment; no custom partitioner |
| Access mode | Read-only on both source topics; no produce, no truncate, no clear |

**Isolation rule.** The Flink consumer group must never reuse the group
ID of any existing consumer. The existing `payment-events-consumer`
group that writes to Raw storage is a separate, historical pipeline. The
two must not compete for partition assignment, and neither must reset
the other's committed offsets.

**Offset policy.** Flink commits offsets through its own checkpoints,
not through Kafka's `enable.auto.commit`. This means:

- On first start, Flink reads from `earliest` and processes the retained
  history.
- On restart from a checkpoint, Flink resumes from the offsets recorded
  in the checkpoint, not from Kafka's committed offsets.
- Kafka's committed offsets for this group are informational only.
- Replay is achieved by restoring from an earlier checkpoint, not by
  resetting group offsets.

**Re-consumption.** If Flink is reset to an earlier checkpoint, it will
re-consume events from the input topics. Idempotency (Section 5.5)
ensures that duplicates do not corrupt state.

**Read-only rule.** J1 never writes to `icmn.pmn.pain001` or
`cpo.plm.pain002`. It never truncates them, never deletes records, and
never clears them as part of a test or reset. Clearing the raw topics is
a frozen-set exception; see §8.1 for the fixture designation that this
rule protects.

## 4. Schema contract

Flink consumes Avro records registered under the Schema Registry subject
derived from the topic name. Schema Registry uses the default
TopicNameStrategy unless an explicit subject override is registered.

| Attribute | Value |
|---|---|
| Avro record type | `com.payments.platform.TechnicalPaymentEvent` |
| Schema file | `contracts/kafka/technical_payment_event.avsc` |
| Subject name | `icmn.pmn.pain001-value` and `cpo.plm.pain002-value` (TopicNameStrategy) |
| Compatibility mode | `BACKWARD_TRANSITIVE` |
| Deserializer | Schema Registry Avro deserializer for Flink, resolved per topic |
| Message key | Kafka record key is a plain string identifier (see Section 5) |

**Compatibility.** New schema revisions are subject to
`BACKWARD_TRANSITIVE` compatibility. No breaking change to the existing
contract is permitted. Additive optional fields with default `null` are
compatible; renaming, retyping, or removing fields is not.

**Deserialization.** The Flink job must use a Schema Registry-aware Avro
deserializer configured with the Schema Registry endpoint. The current
skeleton reads the raw topics through the Flink Table API with the
`avro-confluent` format. Its `json.loads` call parses only the JSON-encoded
`parsed_event_data` field inside the decoded Avro envelope; it does not parse
the wire format. The proposed output subjects, Avro mappings, time semantics,
and delivery gates are defined in `docs/streaming/STR-003-output-contract.md`,
which remains a draft pending approval and sink implementation.

**Schema Registry endpoint.** The Flink job reads the endpoint from an
environment variable. The value is the existing `schema-registry`
service on the Compose network. No credentials are embedded in the job
or committed to the repository; if Schema Registry authentication is
enabled, credentials are supplied through the platform's secret
provider.

## 5. Correlation contract

The canonical development lifecycle key is `correlation_id`, a required
string field in the Avro schema.

### 5.1 Canonical key

| Attribute | Value |
|---|---|
| Canonical key field | `correlation_id` |
| Key type | string |
| Required | yes (non-null in Avro schema) |
| Scope | journey-level: one value per synthetic payment journey |
| Provision | provisional; subject to confirmation by STR-002A-PROD |
| Input contract | decided; see §8.1 |
| Never permitted | `"unknown"`, empty string, or any placeholder as a shared key |

### 5.2 Fallback and quarantine

If `correlation_id` is empty or missing in a consumed event:

1. The event must NOT be added to keyed lifecycle state.
2. The event must be routed to the streaming failure output with:
   - Source topic
   - Source partition
   - Source offset
   - Raw event bytes (or a governed reference)
   - Reason code: `missing_correlation_key`
3. A bounded counter `lifecycle_events_missing_key_total` is
   incremented. The counter is labelled by source topic only -- never by
   any identifier value.

The literal `"unknown"` is never used as a shared lifecycle key. Using
it merges unrelated payments into one state entry. This rule is
non-negotiable.

### 5.3 Other identifiers

The following identifiers are carried in the event and may be used for
enrichment, joins, or provenance -- but they are NOT the canonical key:

| Identifier | Field | Use |
|---|---|---|
| `end_to_end_id` | required | Fallback candidate for STR-002A-PROD investigation; not used as the development key |
| `instruction_id` | optional | Payment instruction scope; may join to business messages |
| `transaction_id` | optional | Transaction scope; may join to settlement events |
| `uetr` | optional | ISO 20022 universal transaction reference; not used as the development key |
| `business_reference` | required | Business-domain reference; not unique per journey |
| `event_id` | required | Event identity only; not a journey key |

### 5.4 Partitioning vs. correlation

Kafka's producer selects the record key using a priority order that may
differ from the canonical key. The producer currently prefers
`end_to_end_id`, then `instruction_id`, then `correlation_id`, then
`transaction_id`.

Therefore:

- Source partition ordering cannot be assumed to align with canonical
  key ordering.
- Flink must re-key the stream by `correlation_id` after
  deserialization. This re-key is a required step, not optional.
- Derived output topics (compacted state, transitions, exceptions) use
  `correlation_id` as the Kafka record key so that per-journey ordering
  and compaction are correct.

### 5.5 Idempotency

Re-consumption of an event (from checkpoint restore or replay) must not
change state. Idempotency is achieved by treating each lifecycle
transition as a function of `(correlation_id, stage, event_timestamp,
event_id)` and applying it only once.

- Current-state updates are upserts guarded by an event-ordering
  comparator.
- Append-only transition outputs use a unique event-identity constraint
  so re-insertion is a no-op.

## 6. Stage and status vocabulary

The `stage` and `status` fields together describe what happened. They
are separate fields with separate vocabularies. Stage names the
process step; status names the outcome of that step.

### 6.1 Stage vocabulary

The generator emits four technical stages in the current synthetic
dataset. Each is defined in
`services/payment-xml-generator/generator_common.py`.

| Stage | Meaning | Source line |
|---|---|---|
| `payment-validation` | Payment has entered validation | `services/payment-xml-generator/generator_common.py:401` |
| `payment-routing` | Payment is being routed to a target institution | `services/payment-xml-generator/generator_common.py:379` |
| `mobile-money-credit` | Payment is being credited to a mobile-money wallet | `services/payment-xml-generator/generator_common.py:384` |
| `agent-cashout` | Payment is being cashed out through an agent | `services/payment-xml-generator/generator_common.py:406` |

**A second vocabulary exists in the generator.** The same file defines
`LIFECYCLE_STAGE_FIELDS` at line 170 with five PDMIS loan-lifecycle
stages: `application`, `approval`, `verification`, `disbursement`,
`cashout`. Those describe the loan journey, not the technical payment
process. The `stage` field of `TechnicalPaymentEvent` carries the
technical vocabulary from the table above, not the PDMIS vocabulary.
Consumers of this contract must not confuse the two.

### 6.2 Status vocabulary

The generator emits the following statuses alongside the stages above.
The Avro schema carries these in the `status` field.

| Status | Meaning |
|---|---|
| `completed` | Stage completed successfully |
| `pending` | Stage has started but not yet completed |
| `delayed` | Stage is behind its expected completion window |
| `failed` | Stage has failed |
| `retrying` | Stage is under retry |
| `timed_out` | Stage has exceeded its expected completion window |

**Terminal statuses.** `failed` and `timed_out` are terminal. Once a
payment reaches a terminal status, subsequent events for the same
`correlation_id` are processed for audit and exception purposes only;
they do not change the primary state.

**Transient statuses.** `pending`, `delayed`, and `retrying` are
transient. A payment in a transient status may transition to any
subsequent stage or to a terminal status.

**Unknown stages or statuses.** If a consumed event carries a `stage`
value not in Section 6.1 or a `status` value not in Section 6.2, it is
routed to the streaming failure output with reason code `unknown_stage`
or `unknown_status` respectively. It does NOT affect keyed state.

## 7. Transition rules

The technical stages do not form a single linear sequence. The canonical
payment flow in the synthetic data is:

    payment-validation
        -> payment-routing
            -> mobile-money-credit
                -> agent-cashout

But not every payment passes through every stage. Mobile-money and
agent-based payments take different paths, and the generator produces
events for both. The state machine therefore tracks both the current
stage and the set of stages observed so far, rather than assuming a
fixed linear order.

### 7.1 Valid forward transitions

| From | Valid next stages |
|---|---|
| (none) | `payment-validation` |
| `payment-validation` | `payment-routing` |
| `payment-routing` | `mobile-money-credit` or `agent-cashout` |
| `mobile-money-credit` | `agent-cashout` |
| `agent-cashout` | (none -- terminal) |

### 7.2 Failure and timeout

An event with `status` in `{failed, timed_out}` is terminal regardless
of its stage. Once a payment reaches a terminal status, subsequent
events for the same `correlation_id` are processed for audit and
exception purposes only. They do not change the primary state.

### 7.3 Retry

An event with `status = retrying` is transient. It does not change the
current stage. It is recorded in the transition log for audit purposes
and a retry counter is incremented for the correlation key.

### 7.4 Repeated stage

An event whose `stage` equals the current stage is a duplicate or
re-emission. It is processed for audit only. The current-state row does
not change stage. This handles the retry pattern where the producer
re-emits the same stage with an updated status.

### 7.5 Backward transition

A transition to an earlier stage in the flow above is invalid unless
the current stage is in a transient status (`pending`, `delayed`, or
`retrying`). From any non-transient status, a backward transition is
invalid and results in the event being routed to the streaming failure
output with reason code `invalid_transition`.

### 7.6 Missing stage

If a payment reaches `mobile-money-credit` or `agent-cashout` without a
prior `payment-routing` event, the gap detector (J2) emits an
operational exception. The state machine (J1) processes the later event
normally -- the gap is an observation, not a state-machine failure.

### 7.7 Terminal-stage detection

A payment is considered complete when it reaches `status = completed`
at `stage = agent-cashout`, or when it reaches any event with
`status = failed` or `status = timed_out` at any stage. These are the
terminal conditions that close a lifecycle in the current-state topic.

## 8. Development vs. production boundary

This contract is DEVELOPMENT-ONLY. It is not a production contract.

**Development (this contract):**

- Source events are produced by the synthetic generators in
  `services/payment-xml-generator`.
- The events are deterministic and derived from a loan journey model.
- The values of `correlation_id`, `end_to_end_id`, `uetr`, and other
  identifiers are stable within the generator's run but do not
  correspond to real payments.
- The contract is approved for developing and testing the streaming
  route end to end.

**Production (STR-002A-PROD):**

- Source events come from real payment platform emitters.
- Identifiers must be proven populated, unique at the required scope,
  immutable across stages, and stable across the full journey.
- The stage and status vocabulary must be validated against the real
  lifecycle, not assumed from the synthetic generator.
- The contract becomes authoritative only after the payment platform
  owner confirms the producer-side guarantee.

**Rule for consumers.** Any consumer that surfaces this data must
clearly label it as synthetic in development. Dashboards, alerts, and
reports must not present synthetic lifecycle data as production
telemetry. This is a hard requirement from the consolidated directive.

### 8.1 J1 input contract — decision (2026-10-13)

**Decision.** J1 reads the raw-route topics `icmn.pmn.pain001` (PMN) and
`cpo.plm.pain002` (PLM) as an explicit **development fixture**. It does
not read a lifecycle topic, and no producer writes to a lifecycle topic
for J1.

**Consumer identity.** J1 reads through a dedicated read-only consumer
group (`flink-lifecycle-consumer`, group id configurable via
`J1_GROUP_ID`). It does not join any producer-side group, and it commits
offsets only under its own group id. The raw topics are not modified,
truncated, or cleared by J1.

**Rationale.** §1 of this contract records that no real emitter is
present in the repository. The synthetic `TechnicalPaymentEvent`
generator is the only source. Building J1 against a lifecycle topic
would require inventing a producer for a topic whose consumer does not
yet exist, which inverts the Kappa direction of the route. The raw
topics are the actual input the production route will see once
STR-002A-PROD confirms a real emitter, so J1's consumption logic is
exercised against the real shape from day one.

**Expiry condition.** This fixture designation ends at the earlier of:

1. STR-002A-PROD confirming a real production emitter and its topic, at
   which point J1's input is re-evaluated against that emitter's
   contract; or
2. The STR-003 output contract being signed off, at which point J1's
   input and output bindings are reviewed together as one contract.

Whichever comes first, this subsection is superseded and the input
contract is restated against the production emitter or the signed-off
STR-003 contract.

**What this decision does not settle.** The dedup TTL, the event-time
and watermark policy, the late-event policy, and the output binding to
`payments.lifecycle.state.v1` all remain STR-003 deliverables. This
decision fixes only the input side: which topics J1 reads, under what
consumer identity, and until when the raw topics are treated as a
fixture.

**Consistency with §3.** §3 already states that J1 reads both topics
under a single isolated consumer group, that offsets are committed
through Flink checkpoints only, and that re-consumption is handled by
idempotency rather than offset resets. §8.1 confirms those rules and
adds the read-only and fixture-designation clauses. No conflict; §8.1 is
the authoritative statement of the input contract, and §3 is its
consumer-level detail.

## 9. Open questions for STR-002A-PROD

The following must be resolved by STR-002A-PROD before this contract can
be replaced by a production contract:

1. Which real systems emit lifecycle telemetry at each stage?
2. Does the real producer populate `correlation_id` for every event?
3. Is `correlation_id` unique at the required scope? What is the
   required scope -- journey, instruction, transaction, or message?
4. Is `correlation_id` immutable across all stages of a journey?
5. Is `correlation_id` stable across retries and re-emissions?
6. Is `end_to_end_id` a better canonical key for production? Under what
   conditions?
7. Does the production stage vocabulary match the four technical stages
   listed in Section 6.1?
8. Does the production status vocabulary match the six statuses listed
   in Section 6.2?
9. Are the transition rules in Section 7 consistent with the real
   lifecycle?
10. Are there stages or statuses missing from the development vocabulary
    that the production lifecycle produces?
11. Is there a separate identifier that identifies the journey but is
    not any of the fields in the current Avro schema?
12. What is the required retention for lifecycle events in production?
13. Is there a governance requirement to encrypt or tokenise any
    identifier before it enters the streaming route?

Until these questions are answered, any Flink job, projection writer, or
dashboard built on this contract must be treated as development-only.

## 10. Files referenced

- `contracts/kafka/technical_payment_event.avsc` -- Avro schema.
- `platform/kafka/topics.yaml` -- topic declarations for both source
  topics.
- `platform/kafka/configure-schema-registry.sh` -- global compatibility
  mode.
- `services/payment-producer/kafka_producer.py` -- producer topic
  mapping and key selection.
- `services/payment-xml-generator/generator_common.py` -- synthetic
  event generation and the source of the stage and status vocabularies
  (lines 170 and 379-406 for stage definitions).
- `docs/streaming/STR-002A-checkpoint-A.md` -- the design report this
  contract formalises.
- `docs/streaming/STR-006-postgres-provisioning-evidence.md` -- the
  Postgres boundary this contract's outputs will populate.

## 11. Approvals

| Role | Name | Date | Status |
|---|---|---|---|
| Platform architecture owner | Herrick OCHOM | 2026-10-14 | approved |
| Payment platform owner | Herrick OCHOM | 2026-10-14 | approved |

**Approval note.** The §4 change of 2026-10-14 (Table API
`avro-confluent` input decoding correction and STR-003 output contract
link) is ratified by this approval. The contract becomes binding on
STR-002B/B2 and STR-003.

The contract becomes binding on STR-002B/B2 and STR-003 when both
approvers sign this section. Until then it is a draft.

## 12. Change history

This contract is a development artifact. Any change to its content
requires:

1. An updated section in this document.
2. A commit message referencing this ticket (STR-002A-DEV).
3. Re-approval by both signatories if the change affects Sections 3, 4,
   5, 6, or 7.

Changes to Sections 1, 2, 8, 9, 10, 11, or 12 do not require
re-approval.

### Change log

| Date | Change | Sections | Re-approval required |
|---|---|---|---|
| 2026-10-12 | Initial draft | all | yes (draft) |
| 2026-10-13 | J1 input contract decided; added §8.1; added read-only rule to §3; added input-contract row to §5.1; added pointer in §1 | 1, 3, 5.1, 8 | no (§8 is not a re-approval section) |
| 2026-10-14 | Corrected §4 to reflect Table API `avro-confluent` input decoding and linked the draft STR-003 output contract and approval gate | 4 | yes (§4 change; approvals remain pending) |
| 2026-10-14 | Approval recorded. §4 change of 2026-10-14 ratified. | 4, 11, 12 | closed |
