# STR-001 Phased Plan — Status Report

- Status: Living document
- Date: 2026-10-13
- Related: phased-plan.md, file-plan.md, risk-register.md, ADR-STR-001-003, STR-002A-DEV-event-contract.md
- Purpose: Track the current state of each ticket in the phased plan against the plan's own exit criteria.

## How to read this report

Each ticket section reproduces the plan's exit criteria and marks each one:

- **DONE** — met, with evidence.
- **PARTIAL** — partially met; the unmet part is named.
- **IN PROGRESS** — actively being worked; blocker named.
- **BLOCKED** — cannot proceed; dependency named.
- **NOT STARTED** — no work begun.

This report is refreshed whenever a ticket closes or the plan is revised.

---

## STR-001 — Discovery and ADR

**Plan exit criteria:**
1. ADR status moved from `Proposed` to `Accepted` by the named approvers.
2. This plan reviewed by the payment platform owner.
3. No open blocker in the risk register that would invalidate STR-002A or STR-002B.

**Status:**
1. **PARTIAL** — the ADR exists at `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md` and is committed. Status remains `Proposed`; approver sign-off not yet recorded.
2. **PARTIAL** — the plan exists at `docs/streaming/phased-plan.md` and is committed. Owner review not yet recorded.
3. **DONE** — no risk-register entry blocks STR-002A or STR-002B.

**Notes:** STR-001 is complete at the document level. The two PARTIAL items are governance actions, not technical ones.

---

## STR-002A — Real emitter and event contract

**Plan exit criteria:**
1. At least one real emitter is confirmed, OR a documented decision that no real emitter exists and the route is built against synthetic events as a development fixture only.
2. Canonical key selected and signed off by the payment platform owner.
3. Stage vocabulary signed off.

**Status:**
1. **DONE** — `docs/streaming/STR-002A-DEV-event-contract.md` documents that no real emitter is present in the repository, that the route is built against the synthetic `TechnicalPaymentEvent` generator, and that STR-002A-PROD remains open for production emitter confirmation. §8 of that document records the boundary.
2. **PARTIAL** — `correlation_id` is named as the canonical development key in STR-002A-DEV §5.1. Owner sign-off not yet recorded.
3. **DONE** — the stage vocabulary is defined in STR-002A-DEV §6: `payment-validation`, `payment-routing`, `mobile-money-credit`, `agent-cashout`. Sourced from `services/payment-xml-generator/generator_common.py`.

**Notes:** The consolidated directive of 2026-10-12 split STR-002A into DEV and PROD. The DEV half is drafted. The PROD half remains open and is tracked separately.

---

## STR-002B — Substrate build-out

The plan does not name B1 and B2 as sub-tickets; this report does, because the work has been sequenced that way in practice.

### STR-002B / B1 — Substrate infrastructure

**Plan exit criteria (from §5):**
1. `docker compose --profile streaming up` brings up Flink and the Flink UI is reachable.
2. The skeleton job submits, runs, and produces to the lifecycle topic.
3. Prometheus-format metrics are exposed by the Flink job.
4. No existing service is affected.
5. `.env` gains only the variables required for STR-002B.

**Status:**
1. **DONE** — JobManager and TaskManager both reach `healthy`; the UI is on `127.0.0.1:8082`.
2. **PARTIAL** — the skeleton job submits and runs; the "produces to the lifecycle topic" clause is deferred to STR-003 per plan §6 (J1/J2).
3. **DONE** — Flink exposes Prometheus metrics on port 9249.
4. **DONE** — verified in `docs/streaming/STR-002B-smoke-test.md`.
5. **DONE** — `.env` gained `FLINK_CHECKPOINT_S3_ACCESS_KEY_ID`, `FLINK_CHECKPOINT_S3_SECRET_ACCESS_KEY`, `FLINK_JOBMANAGER_UI_PORT`.

**Evidence:** `docs/streaming/STR-002B-smoke-test.md`, `docs/streaming/STR-002B-preflight.md`, `docs/streaming/STR-002B-minio-iam-discovery.md`, `docs/streaming/STR-002B-pyflink-java17-investigation.md`.

### STR-002B / B2 — Contract-bound topic declarations and Flink input binding

**Plan exit criteria (from §5):**
1. Derived topics declared and created.
2. Schema subjects registered.
3. Flink skeleton consumes the source topics.
4. No existing service affected.

**Status:**
1. **DONE** — five topics declared in `platform/kafka/topics.yaml`; created by `kafka-init`; `payments.lifecycle.state.v1` verified as `cleanup.policy=compact`.
2. **DONE** — five Avro schemas added under `contracts/kafka/`; five subjects registered in Schema Registry via `platform/kafka/configure-schema-registry.sh`.
3. **IN PROGRESS** — the skeleton has been rewritten to consume Avro via `confluent-kafka` and key by `correlation_id`. Blocked on a Flink cluster config issue: `flink-conf.yaml` on disk has `jobmanager.rpc.address: flink-jobmanager` but the container reads `flink-taskmanager`, preventing TaskManager registration and job scheduling.
4. **DONE** — no existing service affected.

**Evidence:** `git log` commits `467112f`, `6bdf8a9`, `70fd9e6`.

**Blocker:** TaskManager RPC resolution. Diagnostics in flight; no resolution yet.

---

## STR-003 — Event-time, ordering, dedup, transition contracts

**Plan exit criteria:**
1. Contracts signed off by the streaming architect.
2. J1 and J2 pass unit tests for transition, dedup, late-event, and watermark cases.
3. J1 and J2 expose Prometheus metrics for events processed, late events, DLQ routes, and transition failures.

**Status:** **BLOCKED** on STR-002B/B2. None of the three criteria can be evaluated until B2 closes.

**Notes:** STR-002A-DEV §5.5 and §7 already anticipate parts of this ticket (idempotency key, transition rules, terminal conditions). The remaining work is the event-time contract, watermark policy, dedup TTL, and J1/J2 implementation.

---

## STR-004 — Compacted state topic and audit log contracts

**Plan exit criteria:**
1. Compacted topic declared and created by `kafka-init`.
2. Replay from input log rebuilds compacted state correctly in a test environment.
3. Tombstone policy tested.
4. Audit log captures events with sufficient fidelity for governance review.

**Status:**
1. **DONE** — `payments.lifecycle.state.v1` declared and created with `cleanup.policy=compact`.
2. **NOT STARTED** — replay test not performed.
3. **NOT STARTED** — tombstone policy not defined.
4. **PARTIAL** — `payments.lifecycle.audit.v1` topic and `lifecycle_audit.avsc` schema exist; no producer or content yet.

**Notes:** Two of the plan's four criteria were pre-empted by B2's topic declarations. The remaining work — retention policy, tombstone semantics, replay test, audit producer — is STR-004 proper.

---

## STR-005 — SLA/gap timers and Prometheus metric contracts

**Plan exit criteria:**
1. SLA thresholds signed off.
2. Metric catalogue published.
3. Cardinality under the agreed budget at peak load.
4. Alerts specified.

**Status:** **NOT STARTED.**

**Notes:** Flink's Prometheus reporter already exposes default Flink metrics. The STR-005-specific metrics (per-stage counters, SLA breach counters, gap detection counters) are not yet defined.

---

## STR-006 — Streaming-owned PostgreSQL projections

**Plan exit criteria:**
1. Streaming schema exists with least-privilege roles.
2. Rebuild procedure tested.
3. Grafana read identity can query the projection but cannot write it.
4. No service-owned table is written by Flink.

**Status:**
1. **DONE** — `payments_operations` database, `payment_lifecycle` schema, and four roles (`payments_ops_migration`, `payments_ops_writer`, `payments_ops_reader`, `grafana_payments_ro`) provisioned ahead of STR-006 by explicit owner approval.
2. **NOT STARTED** — tables not designed; no projection writer.
3. **DONE** — `grafana_payments_ro` can connect but has no write grant.
4. **DONE** — no Flink-to-Postgres path exists.

**Evidence:** `docs/streaming/STR-006-postgres-provisioning-evidence.md`.

**Notes:** The consolidated directive added a projection-writer requirement not present in the original plan. STR-006's scope is now larger than the plan describes.

---

## STR-007 — DLQ classification, replay isolation, audit evidence

**Plan exit criteria:**
1. Every DLQ reason code is tested.
2. Replay does not double-count.
3. Replay audit trail captures who, when, what, and outcome.
4. Operators can classify and replay without engineer involvement.

**Status:** **NOT STARTED.**

**Notes:** The DLQ topic (`payments.lifecycle.dlq.v1`) and its schema (`lifecycle_dlq.avsc`) exist. The classification, replay, and audit contracts do not.

---

## STR-008 — Prometheus, Kafka Exporter, Grafana

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

## STR-009 — End-to-end validation

**Plan exit criteria:**
1. All tests pass, or documented exceptions with owner and remediation.
2. Sign-off recorded in the ADR's approval block.
3. ADR status updated to reflect validation.

**Status:** **NOT STARTED.**

---

## Summary table

| Ticket | Plan state | Reality |
|---|---|---|
| STR-001 | Complete | Documents complete; sign-off open |
| STR-002A | Complete | DEV drafted; PROD open |
| STR-002B/B1 | Complete | Verified, evidenced |
| STR-002B/B2 | Complete | 3 of 4 change sets done; Flink input binding blocked on TaskManager RPC |
| STR-003 | Not started | Blocked on B2 |
| STR-004 | Not started | Topic and schema pre-empted; rest open |
| STR-005 | Not started | — |
| STR-006 | Not started | Provisioning done ahead of ticket; tables and writer open |
| STR-007 | Not started | Topic and schema pre-empted; rest open |
| STR-008 | Not started | — |
| STR-009 | Not started | — |

---

## Current blocker

The single ticket in flight is STR-002B/B2. The blocker is a Flink cluster config issue:

- `platform/flink/conf/flink-conf.yaml` on disk has `jobmanager.rpc.address: flink-jobmanager`.
- The same file inside the running TaskManager container has `jobmanager.rpc.address: flink-taskmanager`.
- The rebuild with `--no-cache` did not update the file inside the image.
- The TaskManager cannot register with the ResourceManager because it is looking at the wrong hostname.
- The job submitted to the JobManager fails with `NoResourceAvailableException` because no TaskManager is registered.

Diagnostics in flight to determine whether `.dockerignore`, the Docker build context, or a Flink entrypoint rewrite is the cause.

---

## Plan revisions required

The consolidated directive of 2026-10-12 introduced changes the plan does not yet reflect:

1. **STR-002A split into DEV and PROD.** The plan treats STR-002A as one ticket.
2. **Checkpoint A gate.** The plan does not name it; the directive requires it before contract-dependent work.
3. **Explicit Kafka → Flink → Kafka pattern.** The plan's §5 does not name this as the required architecture.
4. **Projection-writer scope in STR-006.** The plan's §9 predates the directive's requirement for a decoupled writer.
5. **Per-ticket Lambda/Kappa responsibility mapping.** §12 of the directive requires this; the plan's tickets do not include it.

A revision pass on `docs/streaming/phased-plan.md` should incorporate these before STR-003 begins.

---

## What this report does not cover

- The internal state of long-running tickets. Only exit criteria are tracked.
- The risk register. Risks are tracked in `docs/streaming/risk-register.md`.
- The file plan. Files are tracked in `docs/streaming/file-plan.md`.
- Governance sign-offs. Those are recorded in the relevant documents' approval blocks.
