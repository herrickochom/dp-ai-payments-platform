# ADR-STR-001-003: Lambda and Kappa in the Streaming Route

- Status: Proposed
- Date: 2026-10-11
- Ticket: STR-001
- Deliverable: #3 (Architecture Decision Record)
- Supersedes: —
- Superseded by: —
- Approver(s): platform architecture owner; payment platform owner
- Approval date: —
- Approval evidence: —

## 1. Context

The platform currently ingests generated payment business messages and generated technical payment events through Kafka into immutable Raw storage. The new requirement is operational monitoring of payment lifecycle events: low-latency state transitions, SLA and gap detection, live counters, recovery by replay, failure reprocessing, and an auditable streaming history. Existing topic and runtime evidence is in `platform/kafka/topics.yaml:8-191`, `services/payment-producer/kafka_producer.py:80-154`, and `services/kafka-consumer-events/kafka_consumer_events.py:73-113`.

The following requester-supplied STR-001 constraints are binding for this ADR:

- Lambda and Kappa are streaming-processing patterns, not a choice between batch and streaming platforms.
- The new streaming route assigns distinct responsibilities to both patterns.
- Its serving layer is PostgreSQL plus Grafana, subject to the boundaries in section 4.
- The existing lakehouse route—Kafka → Iceberg → Nessie → Trino → Superset—is a separate concern and is out of scope. This ADR neither treats it as a Lambda batch path nor proposes changes to it.

The named ticket file, `STR-001 — Streaming Architecture Discovery and.txt`, was not present in the repository during reconnaissance. The ticket text was subsequently provided to the design process, and its constraints were reviewed against the requester-supplied constraints above. That review confirmed that the constraints in this section are consistent with the ticket: Lambda and Kappa are scoped to the streaming route, the serving layer is PostgreSQL plus Grafana under the boundaries in section 4, and the lakehouse route remains a separate concern. The ticket text remains the authoritative scope source. Where this ADR and the ticket are ever found to diverge, the ticket prevails and this ADR is amended.

## 2. Definitions

**Lambda, as used here:** the low-latency operational-processing responsibility within the streaming route. It incrementally evaluates ordered lifecycle events and maintains immediately useful operational state, timers, gap/SLA signals, and live metrics. This is not the classical Lambda architecture with independent speed and batch implementations.

**Kappa, as used here:** the replay and recovery responsibility within the streaming route. An ordered, retained event log is the recoverable input; current state can be rebuilt by replay, failed events can be quarantined and deliberately reprocessed, and an audit history is retained. This is not a claim that every platform workload uses a single classical Kappa pipeline.

Both definitions apply only to the new streaming route, not to the platform as a whole.

## 3. Findings from reconnaissance

### A. Kafka topics, producers, consumers, and schemas

- Payment lifecycle-relevant source topics include business-message topics `icmn.vpm.pain001`, `cpo.psn.pain002`, `wendi.pain001`, `wendi.pain002`, mobile `pacs.008`/`pacs.002` topics, and standalone technical-event topics `icmn.pmn.pain001` and `cpo.plm.pain002` (`platform/kafka/topics.yaml:12-103`).
- Shared retry and DLQ topics exist as `payment-events.retry` and `payment-events.dlq`; both use delete retention, for 7 and 30 days respectively (`platform/kafka/topics.yaml:179-191`). No compacted lifecycle-state topic is declared (`platform/kafka/topics.yaml:5-8`, `platform/kafka/topics.yaml:179-191`).
- The payment producer maps PMN and PLM topics to `TECHNICAL_PAYMENT_EVENT`, selects a business key from actual identifiers, and rejects events without a stable key (`services/payment-producer/kafka_producer.py:80-154`). It uses Schema Registry, Avro serialization, acknowledgements, and optional idempotence (`services/payment-producer/kafka_producer.py:700-735`).
- The consumer subscribes to the configured payment and reference topics under group `payment-events-consumer`, with manual commits and `earliest` reset behavior (`services/kafka-consumer-events/kafka_consumer_events.py:73-109`, `services/kafka-consumer-events/kafka_consumer_events.py:1444-1463`). Its current purpose is Raw Avro persistence, not lifecycle stream processing (`services/kafka-consumer-events/kafka_consumer_events.py:1409-1418`).
- A deliberate DLQ replay tool republishes one exact failed record and leaves the DLQ record as an immutable audit record (`services/kafka-consumer-events/replay_dlq.py:1-19`, `services/kafka-consumer-events/replay_dlq.py:170-240`).
- The technical-event Avro contract contains lifecycle stage/status, event and processing timestamps, latency, retries, timeout and error fields, plus several correlation identifiers (`contracts/kafka/technical_payment_event.avsc:1-35`). The generic business-event envelope does not expose those identifiers as first-class fields (`contracts/kafka/payment_event.avsc:1-22`).

### B. PostgreSQL services, schemas, and ownership

- Compose defines a platform PostgreSQL service and a separate `pdm-source-postgres` service (`docker-compose.yaml:28-55`, `docker-compose.yaml:98-132`).
- Platform PostgreSQL creates separate Superset and Airflow databases (`platform/postgres/initdb/20_create_superset_db.sh:5-19`, `platform/postgres/initdb/30_create_airflow_db.sh:4-17`) and dedicated transform migration/application roles (`platform/postgres/initdb/40_create_transform_ledger_roles.sh:18-66`). Transform-owned tables record runs, batch executions, attempts, events, and locks (`orchestration/transform_runtime/migrations/001_transform_ledger.sql:1-7`).
- Source PostgreSQL owns an `mdm` schema containing source master data, golden records, crosswalks, and identity alerts (`platform/pdm-source-postgres/initdb/10_mdm_schema.sql:1-82`). Only ordinary SACCO source data is published for CDC (`platform/pdm-source-postgres/initdb/10_mdm_schema.sql:84-86`).
- No repository evidence identifies an existing PostgreSQL schema owned by the new streaming route. Reuse of the existing platform, source, Superset, Airflow, transform, or MDM tables for streaming projections is not authorised by this ADR.

### C. Monitoring, alerting, and agent components

- `services/agent-api/observability.py` emits bounded structured operational log events (`services/agent-api/observability.py:1-22`).
- The programme monitoring register is explicitly awaiting evidence-based population and has no records (`programme/registry/operations/monitoring.yaml:1-6`).
- No Prometheus or Grafana service/configuration was found in the repository or Compose service inventory. Therefore, the required Prometheus/Grafana surface is a target contract, not an existing verified capability. This is a known unknown requiring operational confirmation.

### D. `platform/trino/etc/rules.json`

- Trino is configured with file-based access control and points directly to `/etc/trino/rules.json` (`platform/trino/etc/access-control.properties:1-2`); Compose mounts the repository file at that path (`docker-compose.yaml:625-636`).
- The file grants or denies Trino catalog, schema, table, and session-property permissions (`platform/trino/etc/rules.json:1-90`, `platform/trino/etc/rules.json:296-340`). It is a Trino access-control artifact, not payment business rules.

### E. Lifecycle and telemetry events

- The repository does contain a separate `TechnicalPaymentEvent` family, distinct from ISO 20022 business messages. PMN and PLM generators explicitly describe these as separate technical correlation events rather than ISO 20022 XML (`services/payment-xml-generator/icmn_generator.py:111-122`, `services/payment-xml-generator/cpo_generator.py:62-73`).
- These events are synthetic: the shared generator describes them as deterministic, source-owned observability and derives them from generated loan context (`services/payment-xml-generator/generator_common.py:364-430`). Checked-in examples contain populated PMN and PLM stages/statuses and telemetry (`data/icmn/pmn/pain001/pmn_0001.xml:1-2`, `data/cpo/plm/pain002/plm_0001.xml:1-2`).
- **Hard-stop finding:** no evidence of real production lifecycle telemetry was found. The streaming contracts that depend on source telemetry are **provisional pending confirmation**; this ADR does not treat synthetic fixtures as proof of a live source contract.

### F. Correlation identifiers

- The technical-event schema defines required `correlation_id` and `end_to_end_id`, and nullable `instruction_id`, `transaction_id`, and `uetr` (`contracts/kafka/technical_payment_event.avsc:4-8`). It does not declare uniqueness constraints.
- The synthetic generator deterministically populates CorrelationId, PaymentInstructionId, EndToEndId, TransactionId, UETR, BusinessReference, and XTrace from the loan journey (`services/payment-xml-generator/generator_common.py:418-430`). The checked-in PMN and PLM samples share those values (`data/icmn/pmn/pain001/pmn_0001.xml:2`, `data/cpo/plm/pain002/plm_0001.xml:2`).
- In checked-in business-message samples, InstrId, EndToEndId, and UETR are populated in VPM and referenced by PSN; PSN additionally references a transaction ID (`data/icmn/vpm/pain001/vpm_0001.xml:2`, `data/cpo/psn/pain002/psn_0001.xml:2`). CorrelationId is present only in the technical samples, not those ISO 20022 samples.
- **Hard-stop finding:** population and cross-stage stability are demonstrated only for synthetic fixtures; repository contracts do not enforce global uniqueness or prove stability in real sources. Selection of the canonical lifecycle key is **provisional pending confirmation**.

### G. Flink, Kafka Connect, and stream processing

- No Flink component or configuration was found in the repository or Compose service inventory.
- Debezium/Kafka Connect contracts and templates exist for governed database CDC, but their presence does not activate CDC (`platform/cdc/debezium/README.md:22-40`). The consumer's activated CDC topic map is intentionally empty (`services/kafka-consumer-events/kafka_consumer_events.py:180-209`).
- The repository defines Kafka producer, Raw consumer, retry/DLQ, and replay behavior, but no existing lifecycle stream processor or compacted lifecycle-state topic was found.

## 4. Decision

**Provisional pending confirmation of the real telemetry source contract and canonical lifecycle key**, the streaming route will use both Lambda and Kappa responsibilities as follows.

### Lambda layer responsibilities

- Consume lifecycle telemetry keyed by the confirmed canonical journey identifier.
- Evaluate low-latency lifecycle state transitions.
- Detect missing expected stages, out-of-order/late events, and SLA breaches.
- Maintain live operational counters and expose them to Prometheus for Grafana.
- Produce serving projections required for current operational views, without changing the authoritative event history.

### Kappa layer responsibilities

- Retain the replayable lifecycle event log as the streaming route's source of truth.
- Maintain a compacted current-state topic derived from that log.
- Rebuild derived state and serving projections by controlled replay.
- Quarantine failures to a DLQ and support deliberate, auditable reprocessing.
- Preserve the event/audit history and replay provenance.

The serving layer is PostgreSQL plus Grafana. PostgreSQL's permitted role is use-case-dependent:

- **Current-state serving projection:** use when Grafana needs indexed, per-payment drill-down or filtering that is unsuitable for metric labels.
- **Operational exception/case projection:** use when acknowledged, assigned, or otherwise operator-managed exception views require relational querying. Operator workflow state may be authoritative for that workflow only, never for payment lifecycle state.
- **Bounded historical serving projection:** use when Grafana needs a short, explicitly retained operational history not efficiently represented in Prometheus.
- **No PostgreSQL projection:** use when aggregate counters, rates, latency histograms, and alert inputs are fully served by Prometheus.

Boundary rules are mandatory:

- PostgreSQL is never the system of record for payment lifecycle state.
- PostgreSQL is never a Flink state backend.
- Flink never writes streaming projections into tables owned by another service. Any PostgreSQL projection requires a dedicated streaming-owned schema/table contract and owner.
- Grafana reads streaming dashboards from Prometheus and, where justified above, streaming-owned PostgreSQL projections.
- The lakehouse route and its Nessie, Trino, Superset, and access-control artifacts remain unchanged.

## 5. Rationale

Both responsibility sets are required because operational monitoring needs immediate state/timer evaluation and a recoverable event history. Lambda responsibilities alone would give fast signals without a sufficient replay/recovery authority. Kappa responsibilities alone, interpreted as replay-only, would not explicitly contract the low-latency timers, gap detection, and live operational metrics required by STR-001.

The lakehouse route is not the batch half of Lambda. It is an independently governed analytical route whose current consumer persists Kafka events to Raw storage (`services/kafka-consumer-events/kafka_consumer_events.py:1409-1418`) and whose Trino policy governs Iceberg access (`platform/trino/etc/rules.json:1-90`). Conflating it with streaming recovery would couple unrelated ownership, latency, and change boundaries.

PostgreSQL is a disposable/rebuildable serving projection because the retained event log and compacted topic hold the streaming truth. This prevents query-optimised tables from becoming an accidental lifecycle authority and preserves deterministic recovery.

Grafana reads Prometheus for metrics and streaming-owned PostgreSQL for justified drill-downs. It does not query the lakehouse for streaming dashboards because that would couple operational latency and availability to the separate analytical route.

## 6. Consequences

### Positive

- Low-latency detection and replay/recovery have explicit, non-overlapping responsibilities.
- Serving data can be rebuilt without promoting PostgreSQL to lifecycle authority.
- The existing lakehouse route remains isolated.
- DLQ and replay concepts align with existing repository behavior (`platform/kafka/topics.yaml:179-191`, `services/kafka-consumer-events/replay_dlq.py:1-19`).

### Negative

- Two responsibility sets increase contract, operational, and testing scope.
- Event ordering, lateness, idempotency, timer recovery, replay isolation, and projection rebuild semantics must be defined before implementation.
- Prometheus, Grafana, Flink, the compacted state topic, and a streaming-owned PostgreSQL schema are not established repository capabilities.
- The decision cannot be final until real telemetry and a canonical correlation key are confirmed.

### Neutral

- This ADR does not select products, versions, deployment topology, retention durations, schemas, or alert thresholds.
- Existing synthetic technical events are useful discovery evidence but are not accepted as production telemetry.
- Existing lakehouse, CDC, MDM, BI, and service-owned PostgreSQL contracts are unchanged.

### Non-binding implications for follow-on tickets

- **STR-002:** confirm the real lifecycle-event producers, event envelope, stage vocabulary, and canonical journey key.
- **STR-003:** define ordering, event-time, watermark/lateness, deduplication, and transition contracts.
- **STR-004:** define compacted-state keys/values, retention, tombstones, and audit-log contracts.
- **STR-005:** define SLA/gap timers and Prometheus metric cardinality/labels.
- **STR-006:** define streaming-owned PostgreSQL projection use cases, ownership, retention, and rebuild rules.
- **STR-007:** define DLQ classification, operator authorization, replay isolation, and audit evidence.
- **STR-008:** define Grafana data-source boundaries, dashboards, and alert ownership.
- **STR-009:** validate recovery, replay, late/out-of-order events, projection rebuilds, and failure modes end to end.

## 6a. Related STR-001 deliverables

This ADR is STR-001 deliverable #3. The eight STR-001 deliverables and their current status are:

1. **Current-state architecture and repository inventory.** Partially covered by `docs/discovery/STR-001-compose-recon.md`, which inventories the Compose stack, Kafka topics, schema configuration, Postgres init patterns, Dockerfile conventions, secret handling, volumes, network topology, health/dependency graph, existing observability, payment lifecycle evidence, and repository layout for new config. That report is a discovery artifact, not a formal deliverable document; if STR-001 expects the deliverable as a standalone document, the report is its source material and the deliverable remains open.
2. **Gap analysis and risk register.** Partially covered by the "Known unknowns" and "Blockers or surprises" sections of `docs/discovery/STR-001-compose-recon.md`. A formal risk register mapped to STR-002 through STR-009 is not yet produced.
3. **Architecture Decision Record comparing Lambda and Kappa.** This document.
4. **Proposed target streaming architecture.** Not yet produced as a standalone document. The architecture is currently described in section 4 (Decision) and in the draft streaming-route design referenced in conversation; a formal target-architecture document mapping services, topics, schemas, and Flink job contracts to compose service definitions remains open.
5. **Event contract and correlation specification.** Not yet produced. Section 3.F records the raw evidence for correlation identifiers; the canonical key selection and event envelope remain **provisional pending confirmation** and are STR-002 work.
6. **Phased implementation plan mapped to STR-002 through STR-009.** Partially covered by the "Non-binding implications for follow-on tickets" section of section 6. A formal phased plan with sequencing, entry/exit criteria, and dependencies between tickets is not yet produced.
7. **Test and acceptance criteria.** Not yet produced. Section 6 (Negative consequences) and the STR-009 implication list the areas that require acceptance testing; specific criteria remain open.
8. **Exact files proposed for creation or modification.** Partially covered by the discovery report's "Frozen set" and "Proposed next actions" sections. A complete file-level plan mapping each proposed change to a ticket is not yet produced.

The lakehouse route, its Nessie publication authority, and the Trino access-control model (`platform/trino/etc/rules.json`) remain unchanged by this ADR and by the deliverables above.

## 7. Alternatives considered and rejected

### Classical Lambda: two independent paths with batch authority

Rejected because STR-001 assigns Lambda only the low-latency operational responsibility and keeps the lakehouse separate. Making an analytical batch path authoritative would duplicate logic and improperly couple the lakehouse to streaming recovery.

### Classical Kappa: one streaming path with replay only

Rejected because replay is necessary but does not by itself assign or guarantee the required low-latency state-transition, SLA, gap, and live-metric responsibilities. STR-001 explicitly requires both responsibility sets.

### Do nothing: no streaming route

Rejected because the current Raw-ingestion route and synthetic lifecycle models do not provide the required live operational state, gap/SLA detection, Prometheus counters, or Grafana serving contract.

## 8. Known unknowns

The unknowns below are the working backlog that STR-002 inherits from STR-001. Each row must be either resolved, re-owned, or explicitly deferred before STR-002 is marked complete.

| # | Unknown | Why it matters | Resolving action | Owner role | Target ticket |
|---|---|---|---|---|---|
| U1 | Confirm the authoritative STR-001 ticket path and content, and that this ADR's constraints match it. | The ADR's scope is defined by the ticket; a mismatch would invalidate the decision. | Retain the ticket text as the authoritative source; record its path here once confirmed; reconcile any divergence into a superseding ADR. | Platform architecture owner | STR-001 (close-out) |
| U2 | Identify which systems emit real lifecycle telemetry and whether the synthetic `TechnicalPaymentEvent` contract represents them. | The streaming route has no real input until a live emitter is confirmed and contracted. | Source-owner discovery: enumerate real producers, capture their event envelopes, and confirm or amend the contract. | Payment platform owner | STR-002 |
| U3 | Select and govern the canonical lifecycle key; confirm population, uniqueness, immutability, and cross-stage propagation for CorrelationId, InstrId, EndToEndId, UETR, transaction IDs, and any source-native identifier. | Partitioning, keyed state, joining, and dedup all depend on this key; the current evidence is synthetic only. | Contract review with source owners; redacted production sampling; recorded key selection with rationale. | Streaming architect | STR-002 |
| U4 | Confirm ordering guarantees, partition key, duplicate semantics, event-time accuracy, expected stage graph, and late/out-of-order policy. | These define the Flink job's correctness model and the SLA/gap detectors' behaviour. | Contract definition with producers and platform owners; recorded as the streaming event contract. | Streaming architect | STR-003 |
| U5 | Confirm lifecycle event-log, compacted-state, DLQ, and audit retention requirements; current retry/DLQ topics use delete retention and are not a compacted lifecycle state store (`platform/kafka/topics.yaml:179-191`). | Retention determines replay horizon, storage cost, and whether Kappa's replay-based recovery is feasible. | Retention policy decision; new topic declarations if required; recorded in the streaming contract. | Platform operations owner | STR-004 |
| U6 | Confirm the approved stream processor and its state/checkpoint/recovery boundary; no Flink component currently exists in the repository. | The processor and its state backend determine checkpoint storage, recovery semantics, and deployment topology. | Processor selection and state/checkpoint design; recorded in the target streaming architecture. | Streaming architect | STR-003 |
| U7 | Confirm Prometheus and Grafana ownership, deployment location, availability targets, authentication, metric cardinality limits, and alert routing; neither is currently configured in the repository. | Observability is a required capability, not an optional one; its absence blocks operational acceptance. | Observability platform design; alert ownership model; recorded in the target streaming architecture. | Platform operations owner | STR-008 |
| U8 | Confirm whether any use case requires PostgreSQL, then assign a dedicated streaming-owned schema, role, migration owner, retention policy, and rebuild contract. No such schema exists today. | PostgreSQL projections must be streaming-owned; reuse of other services' tables is not authorised. | Use-case review; schema/role design; recorded in the streaming projection contract. | Streaming architect | STR-006 |
| U9 | Confirm production Kafka Connect/CDC activation separately; repository configuration is fail-closed and currently inactive (`platform/cdc/debezium/README.md:22-40`, `services/kafka-consumer-events/kafka_consumer_events.py:180-209`). | CDC may become an additional lifecycle-event source, which would affect the streaming contract and correlation model. | Separately governed activation decision; not part of this ADR. | Platform data owner | STR-002 (triage) |

## 9. References

- STR-001 ticket: `STR-001 — Streaming Architecture Discovery and.txt`; provided to the design process after initial reconnaissance. Its text remains the authoritative scope source for this ADR.
- `docs/discovery/STR-001-compose-recon.md` (discovery report supporting deliverables #1 and #2).
- `platform/kafka/topics.yaml:5-191`
- `contracts/kafka/payment_event.avsc:1-22`
- `contracts/kafka/technical_payment_event.avsc:1-35`
- `services/payment-producer/kafka_producer.py:80-154`, `services/payment-producer/kafka_producer.py:700-735`
- `services/kafka-consumer-events/kafka_consumer_events.py:73-113`, `services/kafka-consumer-events/kafka_consumer_events.py:180-209`, `services/kafka-consumer-events/kafka_consumer_events.py:1409-1463`
- `services/kafka-consumer-events/replay_dlq.py:1-19`, `services/kafka-consumer-events/replay_dlq.py:170-240`
- `services/payment-xml-generator/icmn_generator.py:35-122`
- `services/payment-xml-generator/cpo_generator.py:23-73`
- `services/payment-xml-generator/generator_common.py:356-478`
- `data/icmn/vpm/pain001/vpm_0001.xml:1-2`
- `data/icmn/pmn/pain001/pmn_0001.xml:1-2`
- `data/cpo/psn/pain002/psn_0001.xml:1-2`
- `data/cpo/plm/pain002/plm_0001.xml:1-2`
- `docker-compose.yaml:28-132`, `docker-compose.yaml:228-330`, `docker-compose.yaml:481-581`, `docker-compose.yaml:625-636`
- `platform/pdm-source-postgres/initdb/10_mdm_schema.sql:1-86`
- `platform/postgres/initdb/20_create_superset_db.sh:5-19`
- `platform/postgres/initdb/30_create_airflow_db.sh:4-17`
- `platform/postgres/initdb/40_create_transform_ledger_roles.sh:18-66`
- `orchestration/transform_runtime/migrations/001_transform_ledger.sql:1-7`
- `services/agent-api/observability.py:1-22`
- `programme/registry/operations/monitoring.yaml:1-6`
- `platform/trino/etc/access-control.properties:1-2`
- `platform/trino/etc/rules.json:1-340`
- `platform/cdc/debezium/README.md:1-40`, `platform/cdc/debezium/README.md:122-143`
- `platform/cdc/contracts/debezium_connector_contract.json:1-90`