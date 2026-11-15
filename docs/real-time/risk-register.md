# STR-001 Risk Register

- Status: Draft
- Date: 2026-10-11
- Owner: Streaming architect
- Related documents: ADR-STR-001-003, STR-001-compose-recon, phased-plan, file-plan
- Related tickets: STR-002 through STR-009

## 1. Purpose and scope

This register consolidates the unresolved inputs, delivery risks, and boundary risks for the proposed streaming route across STR-002 through STR-009. It covers contract, processing, replay, serving, observability, security, operations, and sequencing concerns; it does not authorise implementation or reopen the separate lakehouse route (`docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md:10-20`, `docs/discovery/STR-001-compose-recon.md:677-686`).

## 2. How to read this register

An **Unknown** is a fact or decision that the available evidence cannot establish. A **risk** is an uncertain event or condition that could affect correctness, security, operations, governance, or schedule. Probability estimates likelihood; Impact estimates consequence. Owner role identifies accountability, not an assigned individual. Target ticket is the earliest expected resolution point. Where evidence does not support a stronger rating, Probability is Medium pending operational evidence.

## 3. Risk register

| ID | Type | Description | Probability | Impact | Owner role | Mitigation or resolving action | Target ticket | Source |
|---|---|---|---|---|---|---|---|---|
| U1 | Unknown | The authoritative STR-001 ticket repository path is not recorded, although the ADR states its supplied text was reviewed and remains authoritative. | Medium | Medium | Platform architecture owner | Record the authoritative ticket location and reconcile any future divergence through ADR governance. | STR-001 close-out | ADR §8 U1; `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md:22-24` |
| U2 | Unknown | Real lifecycle telemetry emitters and their conformance to `TechnicalPaymentEvent` are unconfirmed; repository evidence is synthetic. | High | High | Payment platform owner | Identify real producers and obtain governed, redacted event-contract evidence. | STR-002A | ADR §8 U2; discovery T14 |
| U3 | Unknown | No canonical lifecycle key is confirmed across stages, and uniqueness/immutability are not proven for production data. | High | High | Streaming architect | Select the key through source-owner review and redacted production validation. | STR-002A | ADR §8 U3; ADR §3.F |
| U4 | Unknown | Ordering, partitioning, duplicates, event-time accuracy, stage graph, and late/out-of-order policy are undefined. | High | High | Streaming architect | Define and approve the processing contract before job design. | STR-003 | ADR §8 U4; ADR §6 Negative |
| U5 | Unknown | Event-log, compacted-state, DLQ, and audit retention requirements are undecided. | High | High | Platform operations owner | Approve replay horizon and retention policy before declaring new topics. | STR-004 | ADR §8 U5; `platform/kafka/topics.yaml:179-191` |
| U6 | Unknown | The stream processor and its state, checkpoint, and recovery boundaries are not approved; no Flink component exists. | Medium | High | Streaming architect | Record the processor and recovery boundary in the target architecture. | STR-003 | ADR §8 U6; discovery T5–T6 |
| U7 | Unknown | Prometheus/Grafana ownership, deployment, availability, authentication, cardinality, and alert routing are not agreed. | High | High | Platform operations owner | Approve the observability operating model and acceptance boundary. | STR-008 | ADR §8 U7; discovery T5–T6 |
| U8 | Unknown | PostgreSQL use cases, streaming-owned schema/roles, retention, migration ownership, and rebuild contract are undecided. | Medium | High | Streaming architect | Confirm whether a relational projection is needed and define its ownership boundary. | STR-006 | ADR §8 U8; ADR §4 |
| U9 | Unknown | Production Kafka Connect/CDC activation remains separately governed and inactive. | Low | Medium | Platform data owner | Triage whether CDC is a lifecycle source; keep activation outside this route unless separately approved. | STR-002A | ADR §8 U9; discovery T14 |
| U10 | Unknown | Runtime Compose state, active CLI profiles, volume initialisation, and external logging driver were not inspected. | Medium | Medium | Platform operations owner | Perform approved read-only runtime inspection before implementation planning is finalised. | STR-002B | Discovery Known unknowns |
| U11 | Unknown | Product-default metrics endpoints and Schema Registry subject/compatibility state are not runtime-verified. | Medium | Medium | Platform operations owner | Use authenticated read-only probes and inventory existing subjects before observability/schema planning. | STR-002B | Discovery T4, T6 and Known unknowns |
| U12 | Unknown | Production sizing, retention cost, TLS/authentication, availability, and data-protection requirements for new services are absent. | High | High | Platform operations owner | Obtain approved non-functional requirements and capacity assumptions. | STR-002B | Discovery Known unknowns |
| R1 | Technical risk | Kafka auto-creation is disabled and no compacted lifecycle-state topic exists; undeclared or incorrectly configured topics will block or invalidate processing. | High | High | Streaming architect | Require governed topic declarations and validation before dependent jobs are accepted. | STR-002B / STR-004 | Discovery T3; `docker-compose.yaml:254-256`; `platform/kafka/topics.yaml:5-8` |
| R2 | Technical risk | The generic `PaymentEvent` schema does not expose lifecycle correlation fields as first-class fields, while the technical schema does; consumers could bind to incompatible envelopes. | Medium | High | Streaming architect | Fix the accepted input envelope and subject boundary before processing design. | STR-002A | ADR §3.A; `contracts/kafka/payment_event.avsc:1-22`; `contracts/kafka/technical_payment_event.avsc:1-35` |
| R3 | Technical risk | Undefined watermarks, timer recovery, deduplication, and transition semantics could generate false gaps, false SLA breaches, or divergent replay state. | High | High | Streaming architect | Make event-time and state-transition acceptance criteria prerequisites for jobs J1/J2. | STR-003 | ADR §6 Negative; ADR §8 U4 |
| R4 | Operational risk | Existing retry/DLQ topics use delete retention and are not a compacted source of lifecycle state, limiting replay and audit horizons. | High | High | Platform operations owner | Align retention and audit requirements with controlled replay contracts. | STR-004 / STR-007 | `platform/kafka/topics.yaml:179-191`; ADR §8 U5 |
| R5 | Operational risk | No repository-configured Prometheus-scrapable endpoint exists, so dashboards and alerts cannot be accepted from current configuration. | High | High | Platform operations owner | Define measurable scrape targets and evidence requirements before dashboard acceptance. | STR-005 / STR-008 | Discovery T6; ADR §3.C |
| R6 | Technical risk | PostgreSQL init scripts are tied to first volume initialisation; treating a new init script as an upgrade mechanism could leave existing environments unprovisioned. | Medium | High | Platform operations owner | Require an explicit migration/upgrade path and runtime-state check for existing volumes. | STR-006 | Discovery T7; `platform/docker/dockerfiles/Dockerfile.postgresql:6-7`; `docker-compose.yaml:43-45` |
| R7 | Governance risk | Streaming projections could accidentally write into service-owned PostgreSQL tables or make PostgreSQL the lifecycle authority. | Medium | High | Streaming architect | Enforce a dedicated streaming-owned boundary and rebuildability review. | STR-006 | ADR §4 Boundary rules; discovery Frozen set |
| R8 | Security risk | New services may bypass existing secret-source, least-privilege, non-root, read-only, or dropped-capability patterns. | Medium | High | Platform operations owner | Include security-boundary evidence in each service acceptance review; never copy secret values into configuration. | STR-002B / STR-006 / STR-008 | Discovery T8–T9; `services/shared/security/secret_provider.py:33-42` |
| R9 | Operational risk | Replay could duplicate effects or contaminate live projections if isolation, authorisation, provenance, and idempotency are not explicit. | Medium | High | Streaming architect | Define replay isolation and auditable operator controls before enabling reprocessing. | STR-007 | ADR §4 Kappa responsibilities; ADR §6 Negative |
| R10 | Governance risk | Streaming dashboards could become coupled to the lakehouse or alter Trino/Superset/Nessie boundaries contrary to the ADR. | Low | High | Platform architecture owner | Enforce the frozen set and review all data-source dependencies against ADR §5. | STR-008 | ADR §5; discovery Frozen set |
| R11 | Governance risk | The phased plan exists but is a Draft; sequencing, entry/exit criteria, and per-ticket Outputs are not yet approved and could change before STR-002 opens. | Medium | High | Platform architecture owner | Approve the phased plan before opening STR-002A or STR-002B. | STR-001 close-out | `docs/streaming/phased-plan.md` |
| R12 | Schedule risk | STR-009 acceptance criteria are not yet defined, allowing earlier tickets to complete without evidence sufficient for end-to-end recovery and failure validation. | High | High | Streaming architect | Establish traceable acceptance criteria before implementation tickets close. | STR-009 | ADR §6a deliverable 7; ADR §6 STR-009 implication |
| R13 | Security risk | A name-based static scan cannot prove that all tracked files are free of literal secrets. | Low | High | Platform operations owner | Run an approved redacted secret scanner before implementation work begins. | STR-002B | Discovery T9 and Known unknowns |

## 4. Top 5 risks by impact

1. **U2 — no confirmed real emitter:** without real input, every downstream contract remains provisional.
2. **U3 — no canonical key:** partitioning, keyed state, joins, deduplication, and replay correctness depend on it.
3. **R3 — undefined event-time correctness:** incorrect timers or watermarks would make operational alerts untrustworthy.
4. **R11 — phased plan not yet approved:** sequencing and acceptance gates exist but are not binding until sign-off.
5. **R5 — no verified metrics surface:** operational monitoring cannot be accepted without measurable scrape targets.

## 5. Risks explicitly out of scope

- Lakehouse correctness, performance, and analytical-model risks remain with the existing lakehouse programme because the lakehouse route is a separate concern (`docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md:10-20`).
- CDC activation risk is separately governed unless STR-002A confirms it as a lifecycle source (ADR §8 U9).
- Existing MDM, Superset, Trino ACL, Nessie publication, and service-owned database risks are outside this route and frozen (`docs/discovery/STR-001-compose-recon.md:677-686`).
- Historical-data regeneration, Kafka offset reset, and volume replacement are not authorised by STR-001 and are excluded from mitigation choices (discovery Frozen set).

## 6. Open questions for STR-002

- Which real systems emit each lifecycle stage, and under what source-owner contract?
- Which identifier is populated, immutable, unique at the required scope, and stable across every stage?
- What envelope and Schema Registry subjects distinguish business messages from lifecycle telemetry?
- What ordering, partitioning, duplicate, timestamp, and missing-stage guarantees do producers provide?
- What non-functional requirements constrain Flink, checkpointing, replay, Prometheus, Grafana, and Kafka Exporter?
- Is CDC relevant to lifecycle telemetry, or should it remain separately dormant?
- Which decisions belong to STR-002A versus STR-002B, and does the approved phased plan confirm that split?

## 7. Known unknowns

- The authoritative repository path for the supplied STR-001 ticket text remains unrecorded (ADR §8 U1).
- Runtime container, profile, metrics, subject, volume, and logging state was not inspected (discovery Known unknowns).
- Real producer evidence and production identifier quality were not available (ADR §8 U2–U4).
- Whether the phased plan will change before approval (see R11).