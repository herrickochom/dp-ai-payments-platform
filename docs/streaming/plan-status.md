# STR-001 Phased Plan — Status Report

- Status: Living document
- Date: 2026-10-13
- Related: phased-plan.md, file-plan.md, risk-register.md, ADR-STR-001-003, STR-002A-DEV-event-contract.md
- Purpose: Track the current state of each ticket in the phased plan against the plan's own exit criteria.

## How to read this report

Each ticket section reproduces the plan's exit criteria and marks each one:

- **DONE**: met, with evidence.
- **PARTIAL**: partially met; the unmet part is named.
- **IN PROGRESS**: actively being worked; blocker named.
- **BLOCKED**: cannot proceed; dependency named.
- **NOT STARTED**: no work begun.

This report is refreshed whenever a ticket closes or the plan is revised.

---

## STR-001: Discovery and ADR

**Plan exit criteria:**
1. ADR status moved from `Proposed` to `Accepted` by the named approvers.
2. This plan reviewed by the payment platform owner.
3. No open blocker in the risk register that would invalidate STR-002A or STR-002B.

**Status:**
1. **PARTIAL.** The ADR exists at `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md` and is committed. Status remains `Proposed`; approver sign-off is not yet recorded.
2. **PARTIAL.** The plan exists at `docs/streaming/phased-plan.md` and is committed. Owner review is not yet recorded.
3. **DONE.** No risk-register entry blocks STR-002A or STR-002B.

**Notes:** STR-001 is complete at the document level. The two PARTIAL items are governance actions, not technical ones.

---

## STR-002A: Real emitter and event contract

**Plan exit criteria:**
1. At least one real emitter is confirmed, OR a documented decision that no real emitter exists and the route is built against synthetic events as a development fixture only.
2. Canonical key selected and signed off by the payment platform owner.
3. Stage vocabulary signed off.

**Status:**
1. **DONE.** `docs/streaming/STR-002A-DEV-event-contract.md` documents that no real emitter is present in the repository, that the route is built against the synthetic `TechnicalPaymentEvent` generator, and that STR-002A-PROD remains open for production emitter confirmation. §8 of that document records the boundary.
2. **PARTIAL.** `correlation_id` is named as the canonical development key in STR-002A-DEV §5.1. Owner sign-off is not yet recorded. The registered `payments.lifecycle.state.v1` schema also uses `correlation_id` as its key field.
3. **DONE.** The stage vocabulary is defined in STR-002A-DEV §6: `payment-validation`, `payment-routing`, `mobile-money-credit`, `agent-cashout`. Sourced from `services/payment-xml-generator/generator_common.py`. The live raw topics confirmed this vocabulary: PMN carries `payment-routing` and `payment-validation`, and PLM carries `mobile-money-credit` and `payment-validation` (see STR-002B/B2 item 3).

**Notes:** The consolidated directive of 2026-10-12 split STR-002A into DEV and PROD. The DEV half is drafted. The PROD half remains open and is tracked separately.

---

## STR-002B: Substrate build-out

The plan does not name B1 and B2 as sub-tickets; this report does, because the work has been sequenced that way in practice.

### STR-002B / B1: Substrate infrastructure

**Plan exit criteria (from §5):**
1. `docker compose --profile streaming up` brings up Flink and the Flink UI is reachable.
2. The skeleton job submits, runs, and produces to the lifecycle topic.
3. Prometheus-format metrics are exposed by the Flink job.
4. No existing service is affected.
5. `.env` gains only the variables required for STR-002B.

**Status:**
1. **DONE.** JobManager and TaskManager both reach `healthy`. The UI is on `127.0.0.1:8082`. One TaskManager is registered with 2 slots.
2. **PARTIAL.** The skeleton job submits and runs. Job `5866dafb939974cefc34783809e56c20` is `RUNNING` with no exceptions. It does not yet produce to the lifecycle topic; that clause is deferred to STR-003 per plan §6 (J1/J2).
3. **DONE.** Flink exposes Prometheus metrics on port 9249. The `flink_j1_*` counters are visible there.
4. **DONE.** Verified in `docs/streaming/STR-002B-smoke-test.md`.
5. **DONE.** `.env` gained `FLINK_CHECKPOINT_S3_ACCESS_KEY_ID`, `FLINK_CHECKPOINT_S3_SECRET_ACCESS_KEY`, `FLINK_JOBMANAGER_UI_PORT`.

**Evidence:** `docs/streaming/STR-002B-smoke-test.md`, `docs/streaming/STR-002B-preflight.md`, `docs/streaming/STR-002B-minio-iam-discovery.md`, `docs/streaming/STR-002B-pyflink-java17-investigation.md`.

### STR-002B / B2: Contract-bound topic declarations and Flink input binding

**Plan exit criteria (from §5):**
1. Derived topics declared and created.
2. Schema subjects registered.
3. Flink skeleton consumes the source topics.
4. No existing service affected.

**Status:**
1. **DONE.** Five topics are declared in `platform/kafka/topics.yaml`. `payments.lifecycle.state.v1` is verified as `cleanup.policy=compact`, `retention.ms=-1`. The topics were recreated by `kafka-init` after the environment rebuild.
2. **DONE.** Five subjects are registered in Schema Registry: `payments.lifecycle.{state,transitions,audit,dlq,exceptions}.v1-value`. `payments.lifecycle.state.v1-value` is version 1, schema id 3. Registration was done before the STR-004 contract was defined; see Deviations.
3. **PARTIAL.** `platform/flink/jobs/j1_lifecycle_state_skeleton.py` reads the raw-route topics `icmn.pmn.pain001` (PMN) and `cpo.plm.pain002` (PLM) through the Table API `avro-confluent` format, through a read-only consumer group. It is not reading the lifecycle topics.
   - It parses `parsed_event_data` in Python, keys on `correlation_id`, and deduplicates by `event_id` within each correlation ID.
   - It emits one transition record per distinct event, including `event_type`, `previous`, and `current`.
   - It writes to stdout, not `payments.lifecycle.state.v1`.
   - Verified on job `5866dafb939974cefc34783809e56c20` against 952 raw messages from two producer runs:
     - 476 transitions: 238 PMN and 238 PLM.
     - 238 correlation IDs, each with one first-seen record and one later record.
     - 476 duplicates dropped by `flink_j1_duplicate_events_total`.
     - Stage split: PMN `payment-routing` 225, PMN `payment-validation` 13; PLM `mobile-money-credit` 225, PLM `payment-validation` 13.
     - 4 checkpoints completed, 0 failed.
   - Not verified: checkpoint recovery (a killed job restored from its last checkpoint), and any output to `payments.lifecycle.state.v1`.
4. **DONE.** No existing service affected.

**Evidence:**
- Commits: `70fd9e6` (B2 schema registration), `4c8fdc9` (TaskManager registration override and Table API skeleton, WIP), `326c382` (plan-status correction), `ff5b07e` (dedup by `event_id`), `0b4d8b3` (`event_type` in output).
- Runtime counters and the stage split above, taken from the Flink REST API, the TaskManager metrics endpoint, and the job's output.

**Deviations:**
- The state topic schema (`payments.lifecycle.state.v1-value`) was registered as part of B2, ahead of the STR-004 contract. The STR-004 contract has not been reviewed against the registered schema.
- The raw topics hold duplicate messages from two producer runs (19:15 and 19:22 on 2026-10-13). The job deduplicates them, but the topics themselves were not cleared. Clearing them is a frozen-set exception and has not been approved.
- The environment was rebuilt from scratch on 2026-10-13 (volumes, images, and build cache removed). `./data` was regenerated by the generator service and verified against pre-deletion checksums for seven source folders. The `data/pdmis_ml` folder was not regenerated, because those files come from the `pdm-ml` services, which are not part of this run.

---

## STR-003: Event-time, ordering, dedup, transition contracts

**Plan exit criteria:**
1. Contracts signed off by the streaming architect.
2. J1 and J2 pass unit tests for transition, dedup, late-event, and watermark cases.
3. J1 and J2 expose Prometheus metrics for events processed, late events, DLQ routes, and transition failures.

**Status:** **BLOCKED** on STR-002B/B2 item 3. The J1 output binding to `payments.lifecycle.state.v1` and the J1 input contract are not yet defined.

**Notes:**
- Deduplication by `event_id` is implemented in the skeleton and verified. What remains for STR-003 is the dedup TTL, the event-time contract, watermark and late-event policy, and the production output binding.
- The dedup state is currently unbounded per correlation ID. A state TTL matched to the maximum lifecycle duration is required.
- `updated_at` in the output is wall-clock time, so replays produce different values for that field. Event-time semantics belong to this ticket.
- STR-002A-DEV §5.5 and §7 already anticipate parts of this ticket (idempotency key, transition rules, terminal conditions).
- Unit tests for J1 and J2 are not yet written. The current verification is an integration run, not unit tests.

---

## STR-004: Compacted state topic and audit log contracts

**Plan exit criteria:**
1. Compacted topic declared and created by `kafka-init`.
2. Replay from input log rebuilds compacted state correctly in a test environment.
3. Tombstone policy tested.
4. Audit log captures events with sufficient fidelity for governance review.

**Status:**
1. **DONE.** `payments.lifecycle.state.v1` is declared and created with `cleanup.policy=compact`.
2. **NOT STARTED.** Replay test not performed.
3. **NOT STARTED.** Tombstone policy not defined.
4. **PARTIAL.** `payments.lifecycle.audit.v1` topic and `lifecycle_audit.avsc` schema exist. No producer or content yet.

**Notes:** Two of the plan's four criteria were pre-empted by B2's topic declarations. The remaining work (retention policy, tombstone semantics, replay test, audit producer) is STR-004 proper.

---

## STR-005: SLA/gap timers and Prometheus metric contracts

**Plan exit criteria:**
1. SLA thresholds signed off.
2. Metric catalogue published.
3. Cardinality under the agreed budget at peak load.
4. Alerts specified.

**Status:** **NOT STARTED.**

**Notes:** Flink's Prometheus reporter already exposes default Flink metrics, and the `flink_j1_*` counters are visible. The STR-005-specific metrics (per-stage counters, SLA breach counters, gap detection counters) are not yet defined.

---

## STR-006: Streaming-owned PostgreSQL projections

**Plan exit criteria:**
1. Streaming schema exists with least-privilege roles.
2. Rebuild procedure tested.
3. Grafana read identity can query the projection but cannot write it.
4. No service-owned table is written by Flink.

**Status:**
1. **DONE.** The `payments_operations` database, `payment_lifecycle` schema, and four roles (`payments_ops_migration`, `payments_ops_writer`, `payments_ops_reader`, `grafana_payments_ro`) were provisioned by `platform/postgres/migrations/001_create_payments_operations.sql`. This was re-run on 2026-10-13 after the environment rebuild. The role passwords were set by that run and are held outside this repository; no values are recorded here.
2. **NOT STARTED.** Tables not designed; no projection writer.
3. **DONE.** `grafana_payments_ro` can connect but has no write grant.
4. **DONE.** No Flink-to-Postgres path exists.

**Evidence:** `docs/streaming/STR-006-postgres-provisioning-evidence.md`. **This file predates the 2026-10-13 rebuild and must be updated with the re-provisioning date before it is cited.**

**Notes:** The consolidated directive added a projection-writer requirement not present in the original plan. STR-006's scope is now larger than the plan describes.

---

## STR-007: DLQ classification, replay isolation, audit evidence

**Plan exit criteria:**
1. Every DLQ reason code is tested.
2. Replay does not double-count.
3. Replay audit trail captures who, when, what, and outcome.
4. Operators can classify and replay without engineer involvement.

**Status:** **NOT STARTED.**

**Notes:** The DLQ topic (`payments.lifecycle.dlq.v1`) and its schema exist. The classification, replay, and audit contracts do not. Note that the skeleton's `flink_j1_events_missing_key_total` counter currently counts dropped envelopes, which is a precursor to the DLQ classification, not the DLQ itself.

---

## STR-008: Prometheus, Kafka Exporter, Grafana

**Plan exit criteria:**
1. `docker compose --profile observability up` brings up Prometheus, Kafka Exporter, Grafana.
2. Prometheus scrapes all targets.
3. Grafana loads dashboards from provisioning.
4. A live Flink job's metrics appear in a Grafana panel.
5. Kafka consumer lag appears in a Grafana panel.
6. Alert rules fire correctly in a controlled test.
7. No existing service is affected.

**Status:** **NOT STARTED.**

**Notes:** Flink already exposes metrics on `:9249`, ready to be scraped. No Prometheus, Kafka Exporter, or Grafana service exists.

---

## STR-009: End-to-end validation

**Plan exit criteria:**
1. All tests pass, or documented exceptions with owner and remediation.
2. Sign-off recorded in the ADR's approval block.
3. ADR status updated to reflect validation.

**Status:** **NOT STARTED.**

**Notes:** Recovery from a checkpoint, replay, and projection rebuild are all STR-009 tests. None has been run.

---

## Summary table

| Ticket | Plan state | Reality |
|---|---|---|
| STR-001 | Complete | Documents complete; sign-off open |
| STR-002A | Complete | DEV drafted; PROD open |
| STR-002B/B1 | Complete | Verified, evidenced; one job running |
| STR-002B/B2 | Complete | Topics and subjects done; J1 reads raw topics, dedups by `event_id`, verified 476 transitions; stdout only; not bound to the lifecycle topic |
| STR-003 | Not started | Blocked on B2 item 3 (output binding and input contract) |
| STR-004 | Not started | Topic and state schema pre-empted; replay and tombstone open |
| STR-005 | Not started | Flink metrics present; SLA and gap metrics not defined |
| STR-006 | Not started | Provisioning done ahead of ticket and re-run after rebuild; tables and writer open |
| STR-007 | Not started | Topic and schema pre-empted; classification and replay open |
| STR-008 | Not started | Flink metrics ready to scrape; no observability services |
| STR-009 | Not started | Recovery, replay, and rebuild tests not run |

---

## Current blocker

The single ticket in flight is STR-002B/B2 item 3. Two open issues:

**1. TaskManager registration (worked around, root cause open).** `platform/flink/conf/flink-conf.yaml` in the repository has `jobmanager.rpc.address: flink-jobmanager`. The `flink-conf.yaml` inside the TaskManager image still contained `flink-taskmanager` after a `--no-cache` rebuild. Registration now works through a `FLINK_PROPERTIES` override on `flink-taskmanager` in `docker-compose.yaml`, committed in `4c8fdc9`. The root cause, why the image holds a stale file, has not been identified. Candidates: `.dockerignore`, the build context, or a Flink entrypoint rewrite. The plan requires `platform/flink/conf/` to be mounted read-only, which would remove the image copy and the need for the override. The mount is currently commented out in `docker-compose.yaml`.

**2. J1 output and input binding (open).** The skeleton reads the raw-route topics and writes to stdout. It is not bound to `payments.lifecycle.state.v1`, and the input contract is not defined. Resolving this is B2 item 3 and belongs to the STR-003 J1 contract. Recovery from a checkpoint has not been tested.

---

## Plan revisions required

The consolidated directive of 2026-10-12 introduced changes the plan does not yet reflect:

1. **STR-002A split into DEV and PROD.** The plan treats STR-002A as one ticket.
2. **Checkpoint A gate.** The plan does not name it; the directive requires it before contract-dependent work.
3. **Explicit Kafka → Flink → Kafka pattern.** The plan's §5 does not name this as the required architecture.
4. **Projection-writer scope in STR-006.** The plan's §9 predates the directive's requirement for a decoupled writer.
5. **Per-ticket Lambda/Kappa responsibility mapping.** §12 of the directive requires this; the plan's tickets do not include it.
6. **Raw-route input for J1.** The plan's §5 says the skeleton consumes "the lifecycle topic." The current skeleton reads the raw-route topics read-only through a development consumer group. This is a development fixture and should be recorded as one until STR-003 settles the input.

A revision pass on `docs/streaming/phased-plan.md` should incorporate these before STR-003 begins.

---

## Open verification

Before this report is relied on, confirm the following:

- The evidence files cited above exist in `docs/streaming/`: `STR-002A-DEV-event-contract.md`, `STR-002B-smoke-test.md`, `STR-002B-preflight.md`, `STR-002B-minio-iam-discovery.md`, `STR-002B-pyflink-java17-investigation.md`, and `STR-006-postgres-provisioning-evidence.md`.
- `STR-006-postgres-provisioning-evidence.md` is updated with the 2026-10-13 re-provisioning.

---

## What this report does not cover

- The internal state of long-running tickets. Only exit criteria are tracked.
- The risk register. Risks are tracked in `docs/streaming/risk-register.md`.
- The file plan. Files are tracked in `docs/streaming/file-plan.md`.
- Governance sign-offs. Those are recorded in the relevant documents' approval blocks.