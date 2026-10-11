# STR-001 File Plan

- Status: Draft
- Date: 2026-10-11
- Owner: Streaming architect
- Related documents: ADR-STR-001-003, STR-001-compose-recon, phased-plan, risk-register
- Covers tickets: STR-002A, STR-002B, STR-003 through STR-009

## 1. Purpose and scope

This planning artifact identifies verified dependencies, frozen paths, and the smallest presently supportable set of proposed file locations for the streaming route. It is not authorisation to create, modify, migrate, deploy, or operate any file or service. Exact file scope remains provisional where ticket-level design decisions do not yet exist (ADR §6a deliverable 6; discovery Known unknowns). Each ticket's section cross-references the corresponding section of `docs/streaming/phased-plan.md`.

## 2. How to read this plan

- **New:** proposed path to be created by the named ticket; it does not exist today.
- **Modify:** verified existing path that the named ticket is expected to change after approval.
- **Modify (governed):** verified existing path whose modification requires sign-off outside the ticket (e.g. an ADR metadata change).
- **Read-only dependency:** verified existing evidence or contract that the ticket must consume without changing.
- **Frozen:** verified existing path or state that the streaming route must not change.
- **No change required:** verified existing path that the ticket considered touching but that preflight confirmed does not need modification.

Every proposed path is labelled **proposed**. Where a ticket has not fixed an exact filename, only its directory is listed here and the tentative filename is deferred to §6.

## 3. Files by ticket

### STR-002A — Event contract and correlation specification
Cross-reference: `docs/streaming/phased-plan.md` §5 (STR-002A).

| Path | Change type | Purpose | Notes |
|---|---|---|---|
| `docs/streaming/` | New (proposed) | Hold the approved event/correlation contract at a filename fixed by STR-002A. | Exact filename is uncertain; ADR §6 STR-002 and §8 U2–U4. |
| `contracts/kafka/payment_event.avsc` | Read-only dependency | Evidence for the current generic business-event envelope. | Verified; ADR §3.A; discovery T14. |
| `contracts/kafka/technical_payment_event.avsc` | Read-only dependency | Evidence for current lifecycle fields and identifiers. | Verified; ADR §3.F; discovery T14. |
| `services/payment-xml-generator/generator_common.py` | Read-only dependency | Evidence that present lifecycle telemetry and identifiers are synthetic. | Verified; ADR §3.E–F. |
| `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md` | Modify (governed) | Remove the "provisional" caveat in §4 if the event contract and canonical key are confirmed. | Requires approver sign-off; ADR metadata. |

### STR-002B — Topics, schemas, and Flink service
Cross-reference: `docs/streaming/phased-plan.md` §5 (STR-002B).

| Path | Change type | Purpose | Notes |
|---|---|---|---|
| `docker-compose.yaml` | Modify | Declare approved streaming services, profiles, dependencies, health contracts, networks, and volumes. | Verified existing file; discovery T1–T2, T10–T12. |
| `platform/kafka/topics.yaml` | Modify | Add only separately approved new streaming topics; existing topic definitions remain frozen. | Verified; auto-create is disabled (`docker-compose.yaml:254-256`); discovery T3. |
| `contracts/kafka/` | New/Modify (proposed scope) | Hold approved streaming schemas or revisions selected by STR-002A/2B. | Exact schema filenames and whether existing contracts change are uncertain; ADR §8 U2–U5. |
| `platform/flink/` | New (proposed) | Hold Flink service configuration and approved job packaging layout. | Directory proposed by discovery T15; exact files uncertain. |
| `platform/docker/dockerfiles/` | New file in existing directory (proposed) | Hold an approved Flink image definition if STR-002B chooses a custom image. | Filename is uncertain; conventions in discovery T8. |
| `programme/tooling/programme_control/collectors/compose_inventory.py` | No change required | STR-002B preflight (P3) confirmed the collector auto-discovers services from `docker-compose.yaml`; no code change is needed for new Flink services. | `docs/streaming/STR-002B-preflight.md` §P3 |

### STR-003 — Event-time contracts and J1/J2
Cross-reference: `docs/streaming/phased-plan.md` §6 (STR-003).

| Path | Change type | Purpose | Notes |
|---|---|---|---|
| `docs/streaming/` | New (proposed) | Hold ordering, watermark, lateness, deduplication, transition, J1, and J2 contracts. | Exact filename is uncertain; ADR §6 STR-003 and §8 U4/U6. |
| `platform/flink/` | New (proposed) | Hold approved J1/J2 job artifacts and job configuration. | Exact language, module, and filenames are not decided; ADR §6 STR-003 and discovery T15. |
| `contracts/kafka/technical_payment_event.avsc` | Read-only dependency | Input-field evidence until STR-002A approves the canonical contract. | Verified; ADR §3.F. |

### STR-004 — Compacted state topic and J3/J4
Cross-reference: `docs/streaming/phased-plan.md` §7 (STR-004).

| Path | Change type | Purpose | Notes |
|---|---|---|---|
| `platform/kafka/topics.yaml` | Modify | Declare approved compacted-state/audit topics without altering existing topic definitions. | Verified; no compacted topic exists (`platform/kafka/topics.yaml:5-8`); ADR §6 STR-004. |
| `docs/streaming/` | New (proposed) | Hold compacted-key/value, retention, tombstone, replay, J3, and J4 contracts. | Exact filename uncertain; ADR §8 U5. |
| `platform/flink/` | New (proposed) | Hold approved J3/J4 artifacts and configuration. | Exact files depend on STR-003/4 decisions; ADR §6 STR-004 and discovery T15. |

### STR-005 — SLA/gap and metrics contracts
Cross-reference: `docs/streaming/phased-plan.md` §8 (STR-005).

| Path | Change type | Purpose | Notes |
|---|---|---|---|
| `docs/streaming/` | New (proposed) | Hold SLA, expected-gap, counter, label, cardinality, and alert-input contracts. | Exact filename uncertain; ADR §6 STR-005; ADR §4 Lambda responsibilities. |
| `platform/flink/` | New (proposed) | Hold approved SLA/gap job configuration or artifacts. | Exact files depend on STR-003 event-time contracts; ADR §4 Lambda responsibilities and §6 STR-005. |
| `platform/prometheus/` | Read-only dependency (proposed directory) | Provide the later scrape/rule boundary; STR-005 defines contracts, not observability deployment. | Directory proposed by discovery T15; creation ownership remains STR-008. |

### STR-006 — PostgreSQL projections
Cross-reference: `docs/streaming/phased-plan.md` §9 (STR-006).

| Path | Change type | Purpose | Notes |
|---|---|---|---|
| `docs/streaming/` | New (proposed) | Hold projection use cases, ownership, retention, and rebuild contracts. | Exact filename uncertain; ADR §4 and §8 U8. |
| `platform/postgres/initdb/` | New numbered file in existing directory (proposed) | Hold an approved streaming-owned schema/role bootstrap only if STR-006 selects PostgreSQL. | Exact filename and whether init is appropriate are uncertain; discovery T7. |
| `docker-compose.yaml` | Modify | Supply approved projection connection variables and service dependency only if required. | Verified; secret values remain outside the file; discovery T9. |
| `orchestration/transform_runtime/migrations/` | Frozen | Preserve transform-service ownership; streaming projections must not use these migrations. | Verified existing directory; ADR §3.B and §4 boundary rules. |

### STR-007 — DLQ and replay
Cross-reference: `docs/streaming/phased-plan.md` §10 (STR-007).

| Path | Change type | Purpose | Notes |
|---|---|---|---|
| `docs/streaming/` | New (proposed) | Hold failure classification, replay isolation, authorisation, provenance, and audit contracts. | Exact filename uncertain; ADR §6 STR-007. |
| `services/kafka-consumer-events/replay_dlq.py` | Read-only dependency | Evidence for existing exact-record replay and duplicate handling. | Verified; ADR §3.A. Existing Raw-route behavior is not modified by this plan. |
| `platform/kafka/topics.yaml` | Modify | Add an approved streaming-route DLQ/audit topic only if STR-007 requires separation. | Existing retry/DLQ definitions remain frozen; ADR §8 U5. Exact need is uncertain. |
| `platform/flink/` | New (proposed) | Hold approved streaming failure/replay job configuration or artifacts. | Exact files depend on STR-007 decisions; ADR §4 Kappa responsibilities and §6 STR-007. |

### STR-008 — Prometheus, Kafka Exporter, and Grafana
Cross-reference: `docs/streaming/phased-plan.md` §11 (STR-008).

| Path | Change type | Purpose | Notes |
|---|---|---|---|
| `docker-compose.yaml` | Modify | Declare approved observability services, dependencies, healthchecks, network, profiles, and volumes. | Verified; no such services exist (discovery T5–T6). |
| `platform/prometheus/` | New (proposed) | Hold scrape and rule configuration. | Directory proposed by discovery T15; exact filenames uncertain. |
| `platform/grafana/` | New (proposed) | Hold provisioning and dashboard configuration. | Directory proposed by discovery T15; exact filenames uncertain. |
| `platform/docker/dockerfiles/` | New file(s) in existing directory (proposed) | Hold custom image definitions only if approved images require repository packaging. | Exact need and filenames uncertain; discovery T8. |
| `programme/tooling/programme_control/collectors/compose_inventory.py` | No change required | STR-002B preflight (P3) confirmed the collector auto-discovers services; the same applies to observability services added in STR-008. | `docs/streaming/STR-002B-preflight.md` §P3 |
| `docs/streaming/` | New (proposed) | Hold datasource boundaries, dashboard ownership, alert routing, and operational acceptance contracts. | Exact filename uncertain; ADR §6 STR-008 and §8 U7. |

### STR-009 — Test evidence
Cross-reference: `docs/streaming/phased-plan.md` §12 (STR-009).

| Path | Change type | Purpose | Notes |
|---|---|---|---|
| `docs/streaming/` | New (proposed) | Hold traceable recovery, replay, ordering, timer, projection rebuild, and failure-mode acceptance evidence. | Exact evidence filenames uncertain; ADR §6 STR-009 and §6a deliverable 7. |
| `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md` | Modify (governed) | Update ADR metadata on validation completion (e.g. status transition to validated). | Requires approver sign-off; ADR metadata and §6a deliverable 7. |
| `docs/discovery/STR-001-compose-recon.md` | Read-only dependency | Verify acceptance evidence against the discovered baseline. | Existing discovery report remains unchanged; discovery T1–T15. |

## 4. Files explicitly frozen

| Path or state | Change type | Reason | Source |
|---|---|---|---|
| `platform/trino/etc/rules.json` | Frozen | Trino file-based access control, not streaming business rules. | ADR §3.D; discovery T15 |
| `platform/trino/etc/access-control.properties` and other `platform/trino/etc/` authentication files | Frozen | Existing analytical access/authentication boundary. | Discovery Frozen set; `docker-compose.yaml:625-637` |
| `platform/nessie/` | Frozen | Lakehouse catalog/publication is a separate concern. | ADR §1 and §5; discovery Frozen set |
| `platform/superset/` | Frozen | Existing analytical BI configuration and dashboards are outside the streaming Grafana route. | ADR §1 and §5; discovery Frozen set |
| `orchestration/transform_runtime/migrations/` | Frozen | Existing transform-ledger/service-owned migrations are not a streaming projection boundary. | ADR §3.B and §4 |
| `platform/pdm-source-postgres/initdb/` | Frozen | Existing MDM source-owned schemas/tables must not receive Flink projections. | ADR §3.B; discovery Frozen set |
| Existing entries in `platform/kafka/topics.yaml` | Frozen | Existing topic definitions may not be altered; future tickets may add separately approved topics. | Discovery Frozen set; `docker-compose.yaml:254-256` |
| `.env`, `.env.*` | Modify (governed) | Variable names may be added or renamed by ticket. Existing secret values must not be altered except by the sanctioned rotation pass. New values must not be committed to tracked files; they are supplied locally or via `.secrets/`. | Discovery T9; ADR §6a deliverable 8 |
| `.secrets/`, mounted secret values | Frozen | Secret material must never enter tracked planning or configuration. | Discovery T9 and Frozen set |
| Existing named volumes and their data | Frozen | Offset, state, lakehouse, database, and audit data are not authorised for mutation. | Discovery T10 and Frozen set |
| Existing Kafka offsets, retry/DLQ records, and historical data | Frozen | STR-001 authorises no reset, deletion, or regeneration. | Discovery Frozen set |
| `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md` | Modify (governed) only | Accepted decision record changes require separate governance; see STR-002A and STR-009. | This file-plan scope; ADR metadata |
| `docs/discovery/STR-001-compose-recon.md` | Frozen | Baseline evidence must remain unchanged. | This file-plan scope |

## 5. Directory structure proposed

```text
platform/
├── flink/                 # Proposed Flink configuration and job packaging boundary.
├── prometheus/            # Proposed scrape and rule configuration boundary.
├── grafana/               # Proposed provisioning and dashboard boundary.
└── postgres/
    └── initdb/            # Existing directory; proposed new numbered streaming bootstrap only if STR-006 approves it.
docs/
└── streaming/             # Planning, contracts, and acceptance evidence for the streaming route.
```

No directory is created by this document other than the already-authorised `docs/streaming/` that contains the STR-001 outputs. The other paths are proposals only (discovery T15).

## 6. Files with uncertain paths

| Tentative path | Why uncertain | Ticket that will fix it | Plan cross-reference |
|---|---|---|---|
| `docs/streaming/<event-contract>.md` | Contract name and split depend on real-emitter and canonical-key decisions. | STR-002A | phased-plan §5 (STR-002A) |
| `contracts/kafka/<streaming-event>.avsc` | Subject, envelope, format, and whether an existing schema can evolve are undecided. | STR-002A / STR-002B | phased-plan §5 |
| `platform/docker/dockerfiles/Dockerfile.<flink>` | Custom image need, base image, packaging, and exact filename are undecided. | STR-002B | phased-plan §5 (STR-002B) |
| `platform/flink/<job-or-config>` | Language, build system, job split, state backend, and deployment layout are undecided. | STR-002B / STR-003 | phased-plan §5–§6 |
| `docs/streaming/<event-time-contract>.md` | J1/J2 boundaries and contract names await STR-003. | STR-003 | phased-plan §6 |
| `docs/streaming/<state-contract>.md` | J3/J4, topic, key/value, retention, and tombstone decisions await STR-004. | STR-004 | phased-plan §7 |
| `docs/streaming/<metrics-contract>.md` | SLA/gap vocabulary and metric names/cardinality are undecided. | STR-005 | phased-plan §8 |
| `platform/postgres/initdb/<number>_<streaming-schema>.sh` | Schema/database choice, roles, numbering, migration method, and upgrade behavior are undecided. | STR-006 | phased-plan §9 |
| `docs/streaming/<replay-contract>.md` | DLQ separation, authorisation, and audit evidence are undecided. | STR-007 | phased-plan §10 |
| `platform/prometheus/<config>` | Scrape targets, rule structure, authentication, and retention are undecided. | STR-008 | phased-plan §11 |
| `platform/grafana/<provisioning-or-dashboard>` | Datasources, dashboard inventory, identifiers, and provisioning format are undecided. | STR-008 | phased-plan §11 |
| `platform/docker/dockerfiles/Dockerfile.<observability-service>` | Custom-image need is not established. | STR-008 | phased-plan §11 |
| `docs/streaming/<acceptance-evidence>.md` | Evidence split and naming depend on acceptance criteria. | STR-009 | phased-plan §12 |

## 7. Known unknowns

- Exact event schema subjects, filenames, and compatibility approach are unresolved (ADR §8 U2–U4; discovery T4).
- Exact topic names, retention, partitioning, compaction, and audit topology are unresolved (ADR §8 U5).
- Flink language/build layout, job boundaries, state backend, checkpoint location, and custom-image need are unresolved (ADR §8 U6).
- PostgreSQL may not be required; if it is, schema, role, migration/upgrade method, and exact init filename remain unresolved (ADR §4 and §8 U8; discovery T7).
- Prometheus/Grafana/Kafka Exporter image strategy, config filenames, authentication, scrape targets, dashboard identifiers, and retention are unresolved (ADR §8 U7; discovery T5–T6).
- STR-009 test/evidence filenames cannot be fixed until acceptance criteria exist (ADR §6a deliverable 7).
