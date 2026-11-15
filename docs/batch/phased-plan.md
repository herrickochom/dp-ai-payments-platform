# STR-001 Phased Implementation Plan

- Status: Draft
- Date: 2026-10-11
- Ticket: STR-001
- Deliverable: #6 (Phased implementation plan mapped to STR-002 through STR-009)
- Owner: Streaming architect
- Related documents: ADR-STR-001-003, STR-001-compose-recon, risk-register, file-plan

## 1. How to read this plan

This plan sequences the streaming route from STR-002 through STR-009. It is a discovery-ticket output, not authorisation to implement: STR-001 closes with this plan as a baseline, and STR-002 opens against it.

Each ticket section contains: *Why this exists*, *Inputs*, *Work*, *Outputs*, *Exit criteria*. Two tickets have additional sections: STR-002A has *Blockers to escalate*; STR-002B has *Frozen set checks*.

Phases are gates, not waterfalls. A ticket begins only when its entry criteria — the completion of the tickets it depends on — are met. A ticket is complete only when its exit criteria are met; exit criteria are testable, not narrative.

A **frozen set** (section 15) names paths and state that no ticket in this plan may modify. It is repeated as a check in each ticket where a change could otherwise touch it.

## 2. Plan at a glance

| Ticket | Title | Depends on | Produces | Blocks |
|---|---|---|---|---|
| STR-001 | Discovery and ADR | — | ADR-STR-001-003, discovery report, phased plan, risk register, file plan | STR-002A, STR-002B |
| STR-002A | Real emitter and event contract | STR-001 | Confirmed producers, envelope, stage vocabulary, canonical key | STR-002B (contract portions), STR-003 |
| STR-002B | Substrate build-out (topics, schemas, Flink service) | STR-001 | Topic declarations, subject registrations, Flink service in compose, skeleton job | STR-003 |
| STR-003 | Event-time, ordering, dedup, transition contracts | STR-002A, STR-002B | Event-time semantics, watermark policy, dedup and transition contracts, J1/J2 | STR-004, STR-005 |
| STR-004 | Compacted state topic and audit log contracts | STR-003 | Compacted topic declaration, key/value schema, retention, J3/J4 | STR-005, STR-006 |
| STR-005 | SLA/gap timers and Prometheus metric contracts | STR-003, STR-004 | Timers, metric names, labels, cardinality budget | STR-007, STR-008 |
| STR-006 | Streaming-owned PostgreSQL projections | STR-004, STR-005 | Schema, roles, migrations, rebuild procedure | STR-008, STR-009 |
| STR-007 | DLQ classification, replay isolation, audit evidence | STR-003, STR-004 | DLQ contract, replay runbook, audit trail | STR-009 |
| STR-008 | Prometheus, Kafka Exporter, Grafana | STR-005, STR-006 | Deployed Prometheus, Kafka Exporter, Grafana, dashboards, alert rules | STR-009 |
| STR-009 | End-to-end validation | STR-006, STR-007, STR-008 | Test evidence, acceptance sign-off | — |

## 3. Cross-cutting workstreams

Two concerns run across multiple tickets and are named here so they do not get lost.

**WS-1 Governance and sign-off.** Every ticket that introduces a new contract (STR-002A, STR-003, STR-004, STR-005, STR-007) produces a contract document that requires sign-off by the payment platform owner. Contracts are the interface; without them, later tickets drift.

**WS-2 Observability self-instrumentation.** From STR-002B onward, every new service emits Prometheus metrics from day one. This is not deferred to STR-008. The alternative — building services with no metrics and retrofitting them — is what STR-008 exists to avoid.

## 4. STR-001 — Discovery (closing)

**Status:** Complete at the document level; awaiting human approval of the ADR and this plan.

**Produces:**
- `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md` (deliverable #3).
- `docs/discovery/STR-001-compose-recon.md` (supporting evidence for deliverables #1 and #2).
- `docs/streaming/risk-register.md` (deliverable #2).
- `docs/streaming/file-plan.md` (deliverable #8).
- This plan (deliverable #6).

**Deliverable status:**
- #1 Current-state inventory: partially complete (compose-focused; lakehouse integration detail deferred to STR-002B and STR-008).
- #2 Gap analysis and risk register: complete.
- #3 ADR: complete; awaiting approval.
- #4 Target architecture: deferred to STR-002B.
- #5 Event contract and correlation specification: deferred to STR-002A.
- #6 Phased plan: complete; awaiting approval.
- #7 Test and acceptance criteria: deferred to STR-009.
- #8 File plan: complete.

**Exit criteria:**
- ADR status moved from `Proposed` to `Accepted` by the named approvers.
- This plan reviewed by the payment platform owner.
- No open blocker in the risk register that would invalidate STR-002A or STR-002B.

## 5. STR-002 — The foundation ticket

STR-002 is split into two tracks because they have different owners and different risk profiles. **Both must complete before STR-003 starts.**

### STR-002A — Real emitter and event contract

**Why this exists.** The discovery report found synthetic `TechnicalPaymentEvent` telemetry but no evidence of a real emitter. Without this track, the streaming route has no real input. The ADR marks its decision "provisional pending confirmation of the real telemetry source contract and canonical lifecycle key."

**Inputs.** ADR §3.E, §3.F, §8 (U2, U3, U9); risk register U2, U3, U9, R2; discovery report T14.

**Work.**
1. Enumerate real emitters. Identify which payment services emit lifecycle telemetry today and which should. Produce a table: service, stage(s), topic, owner.
2. Capture event envelopes. For each confirmed emitter, capture a redacted sample. Compare against `contracts/kafka/technical_payment_event.avsc`.
3. Confirm stage vocabulary. Produce the canonical list of lifecycle stages and valid transitions. This is the vocabulary the gap detector and SLA evaluator will use.
4. Select the canonical lifecycle key. Evaluate `CorrelationId`, `InstrId`, `EndToEndId`, `UETR`, `transaction_id` against population, uniqueness, immutability, and cross-stage propagation. Select one primary key and at most one fallback. Record the rationale.
5. Amend the ADR's §4 decision if the contract differs materially from the synthetic one, replacing "provisional" with the confirmed contract. This amendment is governed: it requires approver sign-off.

**Outputs.**
- `docs/streaming/` — event contract and correlation specification (filename fixed by STR-002A).
- `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md` — governed amendment if the contract is confirmed (see file plan, STR-002A).

**Exit criteria.**
- At least one real emitter is confirmed, or a documented decision that no real emitter exists and the route is built against synthetic events as a development fixture only.
- Canonical key selected and signed off by the payment platform owner.
- Stage vocabulary signed off.

**Blockers to escalate.**
- If no real emitter exists and none is planned, escalate to the programme owner. The streaming route becomes a development-only capability and STR-009's acceptance criteria are reduced accordingly.

### STR-002B — Substrate build-out

**Why this exists.** Flink, Prometheus, Kafka Exporter, and Grafana do not exist in the repository. Kafka auto-create is off, and no compacted topic exists. This track builds the substrate against the confirmed contract.

**Inputs.** Discovery report T1, T3, T4, T7, T8, T9, T10, T11, T12, T15; ADR §4; risk register R1, R6, R8, U10, U11, U12; STR-002A outputs where available.

**Work.**
1. Declare topics. Add lifecycle topic declarations to `platform/kafka/topics.yaml`. Reuse `payment-events.dlq` for DLQ unless a separate topic is justified by STR-007. Add the compacted state topic only if STR-004 has scoped it by now; otherwise defer it to STR-004 and add only the input topic and derived-output topics here.
2. Register schemas. Extend `platform/kafka/configure-schema-registry.sh` or add a sibling `register-schemas.sh` to explicitly register the lifecycle envelope subject(s), rather than relying on producer auto-registration. This matches the repository's "declare everything" posture.
3. Add `Dockerfile.flink` under `platform/docker/dockerfiles/`, following the Superset and Kafka conventions from discovery T8: explicit version pin, deliberate user, no multi-stage unless justified.
4. Add Flink services to `docker-compose.yaml`: `flink-jobmanager` and `flink-taskmanager`, session mode for Day-0. Mount `platform/flink/conf/` read-only. Named volume `flink-data` for RocksDB. Checkpoint storage in MinIO under a dedicated prefix, not under raw/warehouse.
5. Add a `streaming` profile. Register in `docker-compose.yaml` and document activation.
6. Update `programme/tooling/programme_control/collectors/compose_inventory.py` if the inventory is meant to enumerate all services. Otherwise the new services will be invisible to programme-level tooling.
7. Add a skeleton Flink job (J1) that consumes the lifecycle topic, keys by the canonical key, and writes a minimal current-state record. This is a smoke-test job, not the full state machine.
8. Instrument the Flink job with Prometheus metrics from day one (WS-2). Expose the Flink metric reporter to Prometheus.
9. Add a Flink scrape target placeholder to the not-yet-existing Prometheus configuration (the actual Prometheus service lands in STR-008).

**Outputs.**
- `platform/kafka/topics.yaml` — modified.
- `platform/kafka/configure-schema-registry.sh` or sibling — modified or new.
- `platform/docker/dockerfiles/Dockerfile.flink` — new.
- `platform/flink/conf/flink-conf.yaml`, `platform/flink/conf/log4j.properties` — new.
- `platform/flink/jobs/` — new, with the skeleton job.
- `docker-compose.yaml` — modified with `flink-jobmanager`, `flink-taskmanager`, and the `streaming` profile.
- `programme/tooling/programme_control/collectors/compose_inventory.py` — modified if applicable.

**Exit criteria.**
- `docker compose --profile streaming up` brings up Flink and the Flink UI is reachable.
- The skeleton job submits, runs, and produces to the lifecycle topic.
- Prometheus-format metrics are exposed by the Flink job, even if nothing scrapes them yet.
- No existing service is affected: `docker compose --profile analytics,superset,dashboard up` still works unchanged.
- `.env` gains only the variables required for STR-002B, and they are placeholders pending the rotation pass.

**Frozen set checks.**
- Nessie, Trino, Superset, `rules.json`, and existing Postgres schemas untouched.
- No Kafka offset reset. No volume deletion. No topic redefinition of existing topics.
- Existing `KAFKA_TOPICS` list for `payment-consumer-events` and `platform-job-runner` unchanged.

**Status:** B1 (substrate infrastructure) complete. B2 (contract-bound topic
declarations and schema registration) blocked on STR-002A's event contract.
Evidence: `docs/streaming/STR-002B-smoke-test.md`.

## 6. STR-003 — Event-time, ordering, dedup, transition contracts

**Why this exists.** The Flink job's correctness model depends on event-time semantics that the ADR explicitly defers.

**Inputs.** STR-002A event contract; STR-002B Flink skeleton; risk register R3, U4, U6.

**Work.**
1. Define event time. Confirm the field used as event time (`occurred_at`, `processing_timestamp`, or other). Confirm its source and accuracy.
2. Define watermarks. Bounded out-of-orderness value, empirically derived from observed lateness in real or representative events. Make it configurable.
3. Define dedup. Dedup key and TTL. Confirm against the canonical key from STR-002A.
4. Define state TTL. Maximum plausible lifecycle duration; configurable.
5. Define transition rules. Valid stage transitions, terminal states, and behaviour on invalid transitions (route to DLQ, emit warning, or both).
6. Define late-event policy. Events arriving after watermark: drop, DLQ, or late-merge with audit.
7. Implement J1 (state machine) and J2 (gap detector) against these contracts.
8. Add unit tests for transition, dedup, late-event, and watermark behaviour.

**Outputs.**
- `docs/streaming/` — event-time and ordering contract.
- J1 and J2 implementations under `platform/flink/`.
- Unit test suite.

**Exit criteria.**
- Contracts signed off by the streaming architect.
- J1 and J2 pass unit tests for transition, dedup, late-event, and watermark cases.
- J1 and J2 expose Prometheus metrics for events processed, late events, DLQ routes, and transition failures.

## 7. STR-004 — Compacted state topic and audit log contracts

**Why this exists.** The Kappa responsibility depends on a replayable log and a compacted current-state topic. Neither exists today. This is the ticket that makes replay possible.

**Inputs.** STR-003 contracts; risk register R1, R4, U5.

**Work.**
1. Declare the compacted state topic in `platform/kafka/topics.yaml`, overriding the `defaults: cleanup_policy: delete` for this topic only. Confirm `kafka-init`'s tooling supports per-topic override (discovery U3).
2. Declare the audit log topic if a separate one is required, or confirm reuse of an existing topic with adjusted retention.
3. Define key and value schemas for the compacted topic. Register subjects explicitly.
4. Define retention and tombstone policy for the compacted topic and the audit topic.
5. Implement J3 (SLA evaluator) and J4 (business-telemetry joiner) if not already covered, and implement the state-topic producer.
6. Define the rebuild procedure for the compacted topic from the input log.
7. Test replay. Stop the job, delete the compacted topic's contents in a test environment, restart, and confirm state rebuilds.

**Outputs.**
- `platform/kafka/topics.yaml` — modified with the compacted topic declaration.
- `docs/streaming/` — state and audit contract.
- J3 and J4 implementations.
- Replay test evidence.

**Exit criteria.**
- Compacted topic declared and created by `kafka-init`.
- Replay from input log rebuilds compacted state correctly in a test environment.
- Tombstone policy tested.
- Audit log captures events with sufficient fidelity for governance review.

## 8. STR-005 — SLA/gap timers and Prometheus metric contracts

**Why this exists.** Operational monitoring is the point of the streaming route. This ticket defines what "operational" means in measurable terms.

**Inputs.** STR-003 event-time contracts; STR-004 state contracts; risk register R5, R12.

**Work.**
1. Define SLA windows per stage. Confirm the numbers with the payment platform owner; do not invent.
2. Define gap rules. Expected stage order, allowed skips, and how "missing stage" is distinguished from "delayed stage".
3. Define metric names and labels for Prometheus. Cap cardinality: no `correlation_id` in metric labels. Use stage, status, and topic as labels; use aggregate counters for volumes.
4. Define alert inputs: which metrics, what thresholds, what windows.
5. Implement SLA timer in J3 using Flink timers.
6. Implement gap detector in J2.
7. Expose metrics from J1–J4 in Prometheus format.
8. Load-test with representative event volumes and confirm the cardinality budget is respected.

**Outputs.**
- `docs/streaming/` — SLA and metrics contract.
- J2 and J3 finalised.
- Metric catalogue with cardinality budget.
- Load-test results.

**Exit criteria.**
- SLA thresholds signed off.
- Metric catalogue published.
- Cardinality under the agreed budget at peak load.
- Alerts specified, even if not yet wired. Wiring is STR-008.

## 9. STR-006 — Streaming-owned PostgreSQL projections

**Why this exists.** Grafana may need per-payment drill-down that Prometheus cannot serve. The ADR permits this but only under strict boundary rules.

**Inputs.** STR-004 state contracts; STR-005 metric contracts; ADR §4 boundary rules; risk register R6, R7, U8.

**Work.**
1. Confirm use cases. For each proposed Grafana panel, decide: Prometheus-only, or PostgreSQL projection. If Prometheus-only, do not build a projection. Only build projections for panels that genuinely require them.
2. Design the schema. Streaming-owned, dedicated schema (for example `streaming`), dedicated role (for example `streaming_app`), dedicated migration role (`streaming_migration`), following the transform-ledger pattern.
3. Write migrations under a streaming-owned migration path.
4. Implement the sink. Flink writes to the streaming schema only. Never to service-owned tables.
5. Define the rebuild procedure. Drop and rebuild from the compacted state topic or the input log.
6. Grant least privilege to Grafana's read identity.

**Outputs.**
- `platform/postgres/initdb/` — new numbered script, if STR-006 selects PostgreSQL.
- Streaming-owned migrations.
- Flink sink implementation.
- `docs/streaming/` — Postgres projection contract.
- Rebuild runbook.

**Exit criteria.**
- Streaming schema exists with least-privilege roles.
- Rebuild procedure tested: drop schema, replay from compacted topic, verify.
- Grafana read identity can query the projection but cannot write it.
- No service-owned table is written by Flink.

## 10. STR-007 — DLQ classification, replay isolation, audit evidence

**Why this exists.** The ADR mandates quarantine and deliberate, auditable reprocessing. The repository already has `payment-events.dlq` and `replay_dlq.py`; this ticket extends existing patterns rather than inventing new ones.

**Inputs.** STR-003 contracts; STR-004 state contracts; risk register R9.

**Work.**
1. Define DLQ classification. Reason codes for parse failure, late event, invalid transition, unjoinable event, schema mismatch.
2. Extend the DLQ message envelope with classification metadata: reason, source topic, offset, original payload.
3. Define replay isolation. A replayed event must not double-count or corrupt state. Test this explicitly.
4. Operator authorisation. Who can trigger replay, and what audit evidence is produced? Align with `replay_dlq.py`'s existing pattern.
5. Implement the DLQ sink across J1–J4.
6. Write the replay runbook.
7. Test failure modes: malformed event, missing schema, late-by-hours, duplicate, unjoinable.

**Outputs.**
- `docs/streaming/` — DLQ and replay contract.
- Extended `replay_dlq.py` or a streaming-specific equivalent.
- Replay runbook.
- Failure-mode test evidence.

**Exit criteria.**
- Every DLQ reason code is tested.
- Replay does not double-count; verified by comparing state before and after.
- Replay audit trail captures who, when, what, and outcome.
- Operators can classify and replay without engineer involvement.

## 11. STR-008 — Prometheus, Kafka Exporter, Grafana

**Why this exists.** Without this, the streaming route is unobservable. The discovery report confirms none of this exists today.

**Inputs.** STR-005 metric contract; STR-006 projection contract; STR-002B Flink metrics endpoint; risk register R5, R8, R10, U7.

**Work.**
1. Add a `kafka-exporter` service to `docker-compose.yaml`. It exposes consumer-group lag and broker metrics in Prometheus format. Minimal footprint, no broker restart.
2. Add a `prometheus` service to `docker-compose.yaml`. Scrape targets: Flink, Kafka Exporter, and, subject to discovery U11, Schema Registry, Trino, and MinIO if they expose Prometheus endpoints.
3. Add a `grafana` service to `docker-compose.yaml`. Postgres-backed metadata in a dedicated `grafana` schema following the discovery T7 pattern. Provisioning directory mounted read-only. Named volume `grafana-data`.
4. Add an `observability` profile. Register and document.
5. Add Grafana provisioning: Prometheus and Postgres datasources, dashboard provider configuration.
6. Author starter dashboards: Flink health, Kafka lag, lifecycle events per stage, SLA breaches, gap detections, DLQ rate.
7. Add alert rules in Prometheus for the thresholds defined in STR-005.
8. Add a read-only Grafana Postgres identity with access to the streaming schema only.
9. Verify the full chain: Flink → Prometheus → Grafana panel shows live data.

**Outputs.**
- `platform/prometheus/prometheus.yml` and `platform/prometheus/rules/`.
- `platform/grafana/provisioning/` and `platform/grafana/dashboards/`.
- `docker-compose.yaml` — modified with `kafka-exporter`, `prometheus`, `grafana`, and the `observability` profile.
- `platform/postgres/initdb/` — new numbered script for the Grafana schema if required.
- `programme/tooling/programme_control/collectors/compose_inventory.py` — modified to enumerate the new observability services.

**Exit criteria.**
- `docker compose --profile observability up` brings up Prometheus, Kafka Exporter, Grafana.
- Prometheus scrapes all targets successfully.
- Grafana loads dashboards from provisioning.
- A live Flink job's metrics appear in a Grafana panel.
- Kafka consumer lag for the Flink consumer group appears in a Grafana panel.
- Alert rules fire correctly in a controlled test.
- No existing service is affected.

## 12. STR-009 — End-to-end validation

**Why this exists.** Every prior ticket has its own acceptance criteria. This ticket validates them together, under failure conditions, and produces the sign-off evidence.

**Inputs.** STR-006, STR-007, STR-008 complete; risk register R12.

**Work.**
1. End-to-end happy path. Real, or confirmed-representative, events flow from Kafka through Flink to compacted state, derived outputs, Prometheus metrics, and Grafana panels.
2. Recovery test. Kill Flink, restart, verify state restores from checkpoint and no events are lost.
3. Replay test. Replay from input log, verify compacted state rebuilds and audit trail is intact.
4. Late event test. Inject late events, verify they are routed per contract.
5. Duplicate test. Inject duplicates, verify dedup.
6. DLQ test. Inject malformed events, verify classification and operator-visible replay.
7. SLA test. Inject events that exceed SLA, verify timers fire and alerts route.
8. Projection rebuild test. Drop the Postgres projection, rebuild from compacted state, verify Grafana panels recover.
9. Load test. Sustained peak volume, verify cardinality budget, latency, and no resource exhaustion.
10. Governance review. Audit evidence, DLQ contents, replay logs.

**Outputs.**
- `docs/streaming/` — acceptance evidence.
- ADR metadata updated on validation completion (governed).
- Sign-off by the payment platform owner and platform architecture owner.

**Exit criteria.**
- All tests pass, or documented exceptions with owner and remediation.
- Sign-off recorded in the ADR's approval block.
- ADR status updated to reflect validation.

## 13. Sequencing and parallelism

```
STR-001 (close)
    │
    ▼
STR-002A ──┐
           ├──► STR-003 ──► STR-004 ──► STR-005 ──┐
STR-002B ──┘                           │           │
                                       │           ▼
                                       ├──► STR-006 ──► STR-008 ──► STR-009
                                       │           ▲
                                       └──► STR-007 ┘
```

- STR-002A and STR-002B run in parallel. A is governance-heavy; B is build-heavy. Different owners.
- STR-003 waits for both 002 tracks. Event-time semantics depend on the contract, and the Flink skeleton must exist to implement against.
- STR-004 through STR-007 are largely sequential because each defines contracts the next depends on. STR-006 and STR-007 can run in parallel once STR-004 and STR-005 are done.
- STR-008 waits for STR-005 (metric contract) and STR-006 (projection contract). It does not strictly need STR-007, but DLQ metrics are valuable in dashboards, so starting after STR-007 is cleaner.
- STR-009 is last.

## 14. Cross-ticket risks

| Risk | Impact | Mitigation |
|---|---|---|
| No real emitter exists | Streaming route becomes development-only | STR-002A escalates early; STR-009 acceptance criteria reduced if confirmed |
| Canonical key unstable in production | State, joins, and dedup all wrong | STR-002A evidence-based selection; STR-003 tests key stability with real data |
| Flink state outgrows memory | Job instability | RocksDB from day one in STR-002B; state TTL bounded in STR-003 |
| Compacted topic retention too short | Replay horizon insufficient | STR-004 explicitly defines retention with the ops owner |
| Postgres projection drifts from compacted state | Grafana shows stale data | STR-006 rebuild procedure; STR-009 verifies |
| Cardinality explosion in Prometheus | Scrape failures, storage cost | STR-005 defines and tests cardinality budget |
| DLQ replay double-counts | State corruption | STR-007 explicit replay isolation test |
| Existing stack broken by new services | Regression | Every phase includes a "no existing service affected" exit criterion |

## 15. Frozen set (never modified by any ticket)

- `platform/trino/etc/rules.json` and Trino access-control files.
- Nessie configuration and publication authority.
- Superset configuration and access model.
- Existing service-owned Postgres schemas and tables.
- Existing Kafka offsets, volumes, DLQ records, and historical data.
- Existing topic definitions and producer/consumer contracts; extensions only.
- `.env` and `.secrets/` values; only variable additions, no value changes without rotation.
- MinIO, Iceberg, and Raw ingestion pipelines.

The governed exceptions to this list — the ADR's own §4 amendment and metadata update — are listed in `docs/streaming/file-plan.md` §3 under STR-002A and STR-009.

## 16. What STR-001 hands to STR-002

STR-001's outputs for STR-002:

1. ADR-STR-001-003 — the streaming-route decision: Lambda and Kappa as complementary responsibilities, Postgres and Grafana as the serving layer, lakehouse out of scope.
2. Discovery report — the current-state inventory, gap analysis, frozen set, and known unknowns.
3. Risk register — U1–U12 and R1–R13, with owners and target tickets.
4. File plan — verified dependencies, frozen paths, and the smallest presently supportable set of proposed file locations.
5. This plan — the sequence, dependencies, and exit criteria for STR-002 through STR-009.
6. Nine unknowns (U1–U9) in ADR §8 — the working backlog STR-002 inherits.

STR-002 begins with STR-002A (emitter and contract) and STR-002B (substrate) in parallel. Both are prerequisites for STR-003, and both have signed-off exit criteria before STR-003 starts.

## 17. Deliverable #6 status

This document is STR-001 deliverable #6: "Phased implementation plan mapped to STR-002 through STR-009." It is complete as a Draft. It becomes binding on STR-002A and STR-002B once approved by the platform architecture owner and the payment platform owner.

Deliverables #4, #5, #7, and #8 are formally completed by STR-002B (target architecture), STR-002A (event contract), and STR-009 (test and acceptance criteria). Deliverable #8 is completed by `docs/streaming/file-plan.md`, which this plan cross-references.
