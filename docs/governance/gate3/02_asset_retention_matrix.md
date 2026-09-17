# Gate 3.1 asset retention matrix

Status: DEFINED proposals; owner/legal approvals BLOCKED. No retention setting is changed and no regulatory period is invented. Proposed owners below are roles requiring named assignment. Source/derivative labels describe platform authority, not legal authority.

## Policy declarations and precedence

`platform/config/governance/governance_policies.yaml` currently declares PUBLIC 365, INTERNAL 1095, CONFIDENTIAL 2555, RESTRICTED 3650 and HIGHLY_RESTRICTED 7300 days; audit logs 2555 days. It declares enforcement/auto-archival enabled, archive path `s3a://archived-data`, and deletion disabled. These are CURRENT DECLARATIONS requiring approval, not demonstrated enforcement or regulatory requirements.

`platform/config/minio/minio-security-config.yaml` currently declares `max_days: 365` and `enforce_retention: true`. This is a CURRENT DECLARATION requiring approval; no bucket lifecycle executor was found during discovery. It does not explain whether 365 days applies globally, to selected datasets or only as a policy aspiration.

The declarations conflict if applied to the same assets: MinIO maximum 365 days versus sensitivity periods up to 7300 and audit 2555 days. No precedence or enforcement wiring was demonstrated. Neither is silently selected. Both remain unchanged; expiry is BLOCKED pending approved asset-specific scope, minimum/maximum obligations, clocks and precedence. Sensitivity alone cannot determine record retention. Automatic archival claims are also unverified.

Kafka manifest declares 1 day for `wendi.camt052`, 30 days for `wendi.camt053`, `agent.transactions` and DLQ, and 7 days for other declared domain/retry topics. Legacy landing log cleanup declares 30 days. These are configuration/code declarations requiring policy approval and effective-runtime evidence, not proposed legal periods. Existing broker expiry may continue under its current settings; this document neither pauses it nor verifies it.

Proposed decision precedence for approval: immutable Gate 2 exclusions and HOLD prohibit destructive action; applicable owner/legal-approved record obligations and asset schedule determine eligibility; implementation settings implement that schedule only after approval. Conflicts or unknown scope fail closed in planning. This is proposed governance precedence, not runtime enforcement.

## Common rules applied to every asset

Legal-hold applicability: YES for every asset below, including operational/internal state where relevant to preservation or recovery. Unknown applicability/status means BLOCKED. Protected Gate 2 exclusions apply regardless of hold status.

Deletion authority for every asset: accountable named owner plus privacy/legal hold clearance and recovery-custodian approval; security approval for restricted/sensitive audit assets; a separately authorised operator executes an exact approved manifest. No role below currently grants execution permission. See [hold/disposal policy](03_legal_hold_and_disposal_policy.md).

Production periods marked TBD must be approved for purpose/record/jurisdiction by owner and legal/privacy liaison. POC preservation proposals are temporary governance proposals, not permission for indefinite production retention. Approval must set review/end conditions for non-protected assets. [Protected baseline](01_protected_baseline.md) is never an ordinary expiry candidate.

## Per-asset records

### A01: Source files under `data/`

| Field | Asset record |
|---|---|
| Asset/layer | Source files under `data/` |
| Data owner (proposed; named assignment pending) | Source/domain owner |
| Purpose | Ingestion and reproducible source history |
| Sensitivity (provisional; approve per dataset) | Restricted; direct identifiers possible |
| Authoritative source or derivative | Authoritative POC source inputs; synthetic history where documented |
| Retention clock/start event | Source capture/observation time; generated-file mtime is not authority |
| Proposed POC retention | Retain approved demo/recovery inputs for POC; no automatic expiry |
| Proposed production retention | Owner/legal-approved record-specific schedule; TBD |
| Archive requirement | Only approved reproducibility/recovery sets |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Producer reruns, ML training/features, Raw provenance |
| Disposal method (proposed; not authorised) | Scoped source-copy disposal after derivative/suppression checks |
| Verification/evidence required | File inventory/checksums, sensitivity, copy lineage and replay exclusions |
| Current implementation status | Retention NOT IMPLEMENTED |

### A02: Kafka domain topics

| Field | Asset record |
|---|---|
| Asset/layer | Kafka domain topics |
| Data owner (proposed; named assignment pending) | Domain owner with Kafka custodian |
| Purpose | Event transport and bounded recovery window |
| Sensitivity (provisional; approve per dataset) | Restricted where payloads contain identifiers |
| Authoritative source or derivative | Transport copy; Raw is durable archive after successful ingestion |
| Retention clock/start event | Broker retention timestamp semantics and segment eligibility; confirm effective timestamp configuration |
| Proposed POC retention | Current 1/7/30-day declarations require approval; no change proposed now |
| Proposed production retention | Approved outage/replay window and record obligations; TBD |
| Archive requirement | Raw coverage required before window loss; archive only if approved |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Consumer lag/offsets, Raw completeness and event replay |
| Disposal method (proposed; not authorised) | Broker expiry only under approved policy; scoped erasure limits documented |
| Verification/evidence required | Effective topic settings, timestamp semantics, lag and Raw reconciliation |
| Current implementation status | Configuration present; NOT VERIFIED |

### A03: Retry topic `payment-events.retry`

| Field | Asset record |
|---|---|
| Asset/layer | Retry topic `payment-events.retry` |
| Data owner (proposed; named assignment pending) | Operations owner with domain owner |
| Purpose | Bounded inline retry audit |
| Sensitivity (provisional; approve per dataset) | Restricted; failure envelope may carry recoverable payload |
| Authoritative source or derivative | Derivative failure record |
| Retention clock/start event | Broker timestamp/segment eligibility; verify |
| Proposed POC retention | Current 7-day declaration requires approval |
| Proposed production retention | Approved retry/audit need, minimized payload; TBD |
| Archive requirement | Only approved incident evidence |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Failure investigations, original source identity |
| Disposal method (proposed; not authorised) | Approved broker expiry plus log-copy disposal |
| Verification/evidence required | Effective retention, envelope fields, retry behaviour and hold approach |
| Current implementation status | Configuration present; NOT VERIFIED |

### A04: DLQ topic `payment-events.dlq`

| Field | Asset record |
|---|---|
| Asset/layer | DLQ topic `payment-events.dlq` |
| Data owner (proposed; named assignment pending) | Domain owner with operations custodian |
| Purpose | Terminal failure investigation and deliberate replay |
| Sensitivity (provisional; approve per dataset) | Restricted; original payload/key encoded, not anonymised |
| Authoritative source or derivative | Derivative recoverable source copy |
| Retention clock/start event | Broker timestamp; case resolution does not silently reset clock |
| Proposed POC retention | Current 30-day declaration requires approval |
| Proposed production retention | Approved investigation/replay window; TBD |
| Archive requirement | Unresolved cases need approved hold/preservation workflow |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Replay ledger, source identity and retained schema |
| Disposal method (proposed; not authorised) | Approved broker expiry after case/hold/replay closure; scoped limits recorded |
| Verification/evidence required | Effective policy, unresolved case inventory, complete scan and execution ledger |
| Current implementation status | Partial safeguards; NOT VERIFIED |

### A05: Consumer offsets

| Field | Asset record |
|---|---|
| Asset/layer | Consumer offsets |
| Data owner (proposed; named assignment pending) | Kafka/platform owner |
| Purpose | Resume consumption and measure processing |
| Sensitivity (provisional; approve per dataset) | Internal operational metadata; topic/group identity |
| Authoritative source or derivative | Authoritative processing progress, not source data |
| Retention clock/start event | Group inactivity/offset retention semantics; confirm broker settings |
| Proposed POC retention | Retain recovery progress through approved POC; effective period unknown |
| Proposed production retention | Approved inactivity/outage window; TBD |
| Archive requirement | Include compatible offsets in recovery coverage where required |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Topic retained ranges, Raw sink and consumer group identity |
| Disposal method (proposed; not authorised) | Approved internal expiry; reset is a separate controlled action |
| Verification/evidence required | Effective offset policy, group positions, lag and recovery/reset approval |
| Current implementation status | Persistence configured; NOT VERIFIED |

### A06: Schema registry and internal Kafka state

| Field | Asset record |
|---|---|
| Asset/layer | Schema registry and internal Kafka state |
| Data owner (proposed; named assignment pending) | Kafka/schema platform owner |
| Purpose | Schema decoding, KRaft/broker integrity and recovery |
| Sensitivity (provisional; approve per dataset) | Internal/restricted configuration; schema metadata may be sensitive |
| Authoritative source or derivative | Authoritative schema/catalog/cluster state |
| Retention clock/start event | Schema version creation or state checkpoint; topic-specific semantics |
| Proposed POC retention | Preserve required schema/internal state; effective policies unknown |
| Proposed production retention | Compatibility/recovery-based approved schedule; TBD |
| Archive requirement | Version-compatible recovery set required |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Raw Avro, replay decoding, offsets, KRaft and `_schemas` |
| Disposal method (proposed; not authorised) | Approved reference-aware schema/state retirement; no blanket age deletion |
| Verification/evidence required | Internal topic cleanup/retention, schema versions and recovery coverage |
| Current implementation status | Configured services; NOT VERIFIED |

### A07: Raw `raw/v2`

| Field | Asset record |
|---|---|
| Asset/layer | Raw `raw/v2` |
| Data owner (proposed; named assignment pending) | Source/domain owner with storage custodian |
| Purpose | Source-fidelity Avro archive and reprocessing |
| Sensitivity (provisional; approve per dataset) | Restricted; direct identifiers possible |
| Authoritative source or derivative | Durable captured source record |
| Retention clock/start event | Event/source capture time; confirm authoritative field/timezone |
| Proposed POC retention | Retain approved POC/recovery corpus; no automatic expiry |
| Proposed production retention | Owner/legal-approved source record schedule; TBD |
| Archive requirement | Required while approved replay/recovery depends on it |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Staging/Bronze rebuild, lineage and disposal suppression |
| Disposal method (proposed; not authorised) | Scoped object/version disposal after complete dependency review |
| Verification/evidence required | Object inventory/checksums, clock, policies, holds and reference dependencies |
| Current implementation status | Idempotent sink present; retention NOT IMPLEMENTED |

### A08: Staging temporary state

| Field | Asset record |
|---|---|
| Asset/layer | Staging temporary state |
| Data owner (proposed; named assignment pending) | Transformation owner |
| Purpose | Temporary parsing/transformation |
| Sensitivity (provisional; approve per dataset) | Restricted source-shaped temporary data |
| Authoritative source or derivative | Derivative ephemeral views/local state |
| Retention clock/start event | End of approved dbt invocation/container disposal |
| Proposed POC retention | Dispose temporary state after successful invocation and required sanitized diagnostics; proposed |
| Proposed production retention | Minimum necessary processing lifetime; approved diagnostics schedule TBD |
| Archive requirement | No routine archival; explicit investigation hold only |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Same-invocation Raw-to-Bronze execution and temporary diagnostics |
| Disposal method (proposed; not authorised) | Remove local temporary state only after dependency/hold checks |
| Verification/evidence required | Temporary database/file inventory and disposable-run cleanup evidence |
| Current implementation status | Ephemeral view design; residual disposal NOT VERIFIED |

### A09: Bronze

| Field | Asset record |
|---|---|
| Asset/layer | Bronze |
| Data owner (proposed; named assignment pending) | Domain owner with transformation custodian |
| Purpose | Source-aligned operational provenance |
| Sensitivity (provisional; approve per dataset) | Restricted; permitted source identifiers |
| Authoritative source or derivative | Derivative of Raw/source |
| Retention clock/start event | Source observation/event time; lineage must preserve clock |
| Proposed POC retention | Preserve frozen baseline; other data needs approved schedule |
| Proposed production retention | Record-specific approved operational/provenance schedule; TBD |
| Archive requirement | Approved recovery dependencies only |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Raw, Silver, lineage and protected historical references |
| Disposal method (proposed; not authorised) | Coordinated table/history/file disposal; never drop-and-recreate as erasure proof |
| Verification/evidence required | Lineage, current/history inventory and all retained references |
| Current implementation status | Persistent tables; retention NOT IMPLEMENTED |

### A10: Silver

| Field | Asset record |
|---|---|
| Asset/layer | Silver |
| Data owner (proposed; named assignment pending) | Analytics data owner |
| Purpose | Privacy-boundary analytical use |
| Sensitivity (provisional; approve per dataset) | Pseudonymous confidential; restricted identity exceptions |
| Authoritative source or derivative | Derivative analytical data |
| Retention clock/start event | Underlying observation/event time; refresh is not clock reset |
| Proposed POC retention | Preserve frozen baseline; other data needs approved schedule |
| Proposed production retention | Approved purpose/record schedule; TBD |
| Archive requirement | Approved reproducibility/history need only |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Bronze, token versions, Gold and ML |
| Disposal method (proposed; not authorised) | Dependency-aware current/history disposal with derivative reconciliation |
| Verification/evidence required | Field classification, lineage, holds, snapshots and downstream inventory |
| Current implementation status | Privacy boundary present; retention NOT IMPLEMENTED |

### A11: Silver Vault

| Field | Asset record |
|---|---|
| Asset/layer | Silver Vault |
| Data owner (proposed; named assignment pending) | Identity/privacy owner |
| Purpose | Restricted identity/token linkage |
| Sensitivity (provisional; approve per dataset) | Highly restricted linkage and direct/internal identity |
| Authoritative source or derivative | Authoritative linkage generated from controlled source/key |
| Retention clock/start event | Mapping generation/version activation; source retention and key retirement also apply |
| Proposed POC retention | Preserve frozen mappings; no rematerialisation or expiry |
| Proposed production retention | Approved mapping/version/key lifecycle; TBD |
| Archive requirement | Only restricted approved recovery/history needs |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Canonical token joins, key recovery, old versions and historical outputs |
| Disposal method (proposed; not authorised) | Authorised version-aware mapping/history disposal; mapping deletion alone insufficient |
| Verification/evidence required | 250-row historical evidence, active/version inventory, holds and key dependencies |
| Current implementation status | Version fields present; lifecycle PARTIAL |

### A12: Gold

| Field | Asset record |
|---|---|
| Asset/layer | Gold |
| Data owner (proposed; named assignment pending) | Business analytics owner |
| Purpose | Dimensional facts and business-ready analytics |
| Sensitivity (provisional; approve per dataset) | Confidential/pseudonymous; assess each model |
| Authoritative source or derivative | Derivative of Silver |
| Retention clock/start event | Underlying record period/event time; rebuild does not restart clock |
| Proposed POC retention | Preserve frozen baseline; other data needs approved schedule |
| Proposed production retention | Approved analytical/history schedule; TBD |
| Archive requirement | Approved historical business/recovery requirements |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Silver, surrogate keys and Consumption |
| Disposal method (proposed; not authorised) | Coordinated current/history disposal plus serving reconciliation |
| Verification/evidence required | Grain/lineage, snapshot/reference and downstream evidence |
| Current implementation status | Persistent models; retention NOT IMPLEMENTED |

### A13: Consumption

| Field | Asset record |
|---|---|
| Asset/layer | Consumption |
| Data owner (proposed; named assignment pending) | Serving/business data owner |
| Purpose | BI/agent/ML serving |
| Sensitivity (provisional; approve per dataset) | Pseudonymous/non-identifying by default; assess derivatives |
| Authoritative source or derivative | Derivative serving surface |
| Retention clock/start event | Underlying reporting/event period; refresh is not reset |
| Proposed POC retention | Preserve frozen baseline and approved demo outputs |
| Proposed production retention | Approved serving/export/cache schedule; TBD |
| Archive requirement | Only approved reporting evidence |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Gold/Silver, BI exports/caches, agents and access policy |
| Disposal method (proposed; not authorised) | Serving/history and export/cache disposal coordinated with upstream |
| Verification/evidence required | Serving inventory, RBAC, export/cache lineage and disposal evidence |
| Current implementation status | Privacy/RBAC present; retention NOT IMPLEMENTED |

### A14: ML training inputs

| Field | Asset record |
|---|---|
| Asset/layer | ML training inputs |
| Data owner (proposed; named assignment pending) | ML owner with source owner |
| Purpose | Reproducible synthetic POC training |
| Sensitivity (provisional; approve per dataset) | Restricted unless verified non-identifying synthetic data |
| Authoritative source or derivative | Source input to model; derived/generated history |
| Retention clock/start event | Dataset release/observation time; associated model release links |
| Proposed POC retention | Preserve inputs needed by accepted release; no retraining |
| Proposed production retention | Approved reproducibility/purpose schedule; TBD |
| Archive requirement | Immutable release-linked dataset manifest |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Model reproducibility, source generator/seed and environment |
| Disposal method (proposed; not authorised) | Release/dependency-aware dataset disposal and residual copy review |
| Verification/evidence required | Dataset hash/provenance, generator/seed, features and model linkage |
| Current implementation status | Files present; retention NOT IMPLEMENTED |

### A15: ML features

| Field | Asset record |
|---|---|
| Asset/layer | ML features |
| Data owner (proposed; named assignment pending) | ML owner with analytics owner |
| Purpose | Training/scoring inputs |
| Sensitivity (provisional; approve per dataset) | Restricted/pseudonymous; source IDs possible |
| Authoritative source or derivative | Derivative of operational/history inputs |
| Retention clock/start event | Feature as-of time and release/run ID |
| Proposed POC retention | Retain accepted provenance; future transient features expire after approved validation/recovery need |
| Proposed production retention | Approved feature/reproduction window; TBD |
| Archive requirement | Only release-linked reproducibility/hold need |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Source state, feature contract, token versions and predictions |
| Disposal method (proposed; not authorised) | Scoped feature-file/table/history disposal |
| Verification/evidence required | Feature manifest/hash, as-of/source provenance, contract and dependencies |
| Current implementation status | Feature builder present; retention NOT IMPLEMENTED |

### A16: ML model artefacts

| Field | Asset record |
|---|---|
| Asset/layer | ML model artefacts |
| Data owner (proposed; named assignment pending) | ML release owner |
| Purpose | Versioned predictive default-risk model |
| Sensitivity (provisional; approve per dataset) | Confidential; evaluate learned-data sensitivity |
| Authoritative source or derivative | Derivative fitted model; authoritative released binary |
| Retention clock/start event | Immutable release creation/promotion time |
| Proposed POC retention | Preserve accepted v1 artefacts unchanged; no overwrite/retraining |
| Proposed production retention | Approved release/rollback/reproducibility schedule; TBD |
| Archive requirement | Immutable accepted and required rollback releases |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Training inputs, pipeline metadata, dependency versions and scoring |
| Disposal method (proposed; not authorised) | Retire release only after rollback/reproduction dependency approval |
| Verification/evidence required | Artefact hashes, environment, Git/Nessie provenance and promotion/rollback record |
| Current implementation status | Fixed v1 files/metadata; version lifecycle PARTIAL |

### A17: ML predictions

| Field | Asset record |
|---|---|
| Asset/layer | ML predictions |
| Data owner (proposed; named assignment pending) | ML owner with serving owner |
| Purpose | Authorised predictive early warning, not fraud determination |
| Sensitivity (provisional; approve per dataset) | Restricted/pseudonymous; case fields may remain |
| Authoritative source or derivative | Derivative scored outputs |
| Retention clock/start event | Scoring as-of/run time tied to release/source |
| Proposed POC retention | Preserve accepted outputs; other runs require approved schedule |
| Proposed production retention | Approved decision/purpose schedule; TBD |
| Archive requirement | Approved evidence/reproduction need only |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Release/features, Consumption and authorized cases |
| Disposal method (proposed; not authorised) | Copy-aware file/table/history disposal with serving reconciliation |
| Verification/evidence required | Run/release/input hashes, semantic label, token versions and copy inventory |
| Current implementation status | Scorer/aggregate logging present; retention NOT IMPLEMENTED |

### A18: Operational logs

| Field | Asset record |
|---|---|
| Asset/layer | Operational logs |
| Data owner (proposed; named assignment pending) | Service/operations owner |
| Purpose | Diagnostics and incident response |
| Sensitivity (provisional; approve per dataset) | Restricted until payload/identifier exposure assessed |
| Authoritative source or derivative | Derivative diagnostics |
| Retention clock/start event | Log event timestamp |
| Proposed POC retention | Current legacy landing cleanup declares 30 days; active coverage unverified; period needs approval |
| Proposed production retention | Approved diagnostic/incident schedule with minimisation; TBD |
| Archive requirement | Hold/incident evidence only |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Failure investigation, DLQ and service recovery |
| Disposal method (proposed; not authorised) | Approved rotation/expiry of all destinations after hold checks |
| Verification/evidence required | Destination inventory, redaction tests, effective rotation and incident exclusions |
| Current implementation status | Partial legacy cleanup; global retention NOT IMPLEMENTED |

### A19: Audit logs

| Field | Asset record |
|---|---|
| Asset/layer | Audit logs |
| Data owner (proposed; named assignment pending) | Governance/security owner |
| Purpose | Accountability, decisions and approvals |
| Sensitivity (provisional; approve per dataset) | Restricted actor/resource/purpose metadata |
| Authoritative source or derivative | Authoritative decision evidence; may contain derivative context |
| Retention clock/start event | Audit event timestamp; case closure rules require explicit approval |
| Proposed POC retention | Durable retention design needed; current seven-year declaration not approved here |
| Proposed production retention | Owner/legal-approved audit schedule; TBD |
| Archive requirement | Durable restricted evidence archive as approved |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Governance investigations, disposal/replay approvals and recovery |
| Disposal method (proposed; not authorised) | Controlled evidence disposal with integrity/hold checks |
| Verification/evidence required | Persistent destination, access/integrity/rotation design and retrieval evidence |
| Current implementation status | JSONL fsync present; default container `/tmp` durability NOT VERIFIED |

### A20: Nessie metadata

| Field | Asset record |
|---|---|
| Asset/layer | Nessie metadata |
| Data owner (proposed; named assignment pending) | Catalog owner with recovery custodian |
| Purpose | Versioned catalog and recovery references |
| Sensitivity (provisional; approve per dataset) | Restricted operational metadata; paths/schema may reveal information |
| Authoritative source or derivative | Authoritative catalog state |
| Retention clock/start event | Commit/reference creation; reachability governs eligibility |
| Proposed POC retention | Preserve protected hashes/tag and required metadata; no GC |
| Proposed production retention | Approved reference/history/recovery policy; TBD |
| Archive requirement | Compatible PostgreSQL/catalog backup required |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | All retained references, Iceberg files and recovery manifests |
| Disposal method (proposed; not authorised) | Reference-aware approved metadata GC; protected refs never ordinary candidates |
| Verification/evidence required | Complete branch/tag/hash inventory, JDBC recovery and reachability proof |
| Current implementation status | JDBC2 store configured; retention/protection NOT VERIFIED |

### A21: Iceberg snapshots/data files

| Field | Asset record |
|---|---|
| Asset/layer | Iceberg snapshots/data files |
| Data owner (proposed; named assignment pending) | Table owner with catalog/storage custodians |
| Purpose | Current/history table contents and recovery |
| Sensitivity (provisional; approve per dataset) | Inherits highest relevant table sensitivity |
| Authoritative source or derivative | Authoritative physical table state; derived from upstream data |
| Retention clock/start event | Snapshot commit/file creation plus source retention; age alone insufficient |
| Proposed POC retention | Preserve all frozen/retained dependencies; no expiry/orphan removal |
| Proposed production retention | Approved snapshot/history policy and grace windows; TBD |
| Archive requirement | Retained reference/recovery files required |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Nessie branches/tags, table metadata, holds and backup manifests |
| Disposal method (proposed; not authorised) | Reference-aware expiry/orphan/GC only after reachability proof |
| Verification/evidence required | Snapshot/file manifest, complete cross-reference reachability and disposable-fixture tests |
| Current implementation status | Storage configured; maintenance NOT IMPLEMENTED |

### A22: Backups

| Field | Asset record |
|---|---|
| Asset/layer | Backups |
| Data owner (proposed; named assignment pending) | Recovery custodian with data owner |
| Purpose | Restore and protected acceptance recovery |
| Sensitivity (provisional; approve per dataset) | Restricted; inherits highest copied sensitivity |
| Authoritative source or derivative | Recovery copy; not independent disposal authority |
| Retention clock/start event | Backup completion/time manifest; source obligations and holds also apply |
| Proposed POC retention | Both Gate 2 prefixes permanently excluded from ordinary retention; other sets require approval |
| Proposed production retention | Frozen exclusions persist; future-set RPO/RTO/retention approved by owner/legal; TBD |
| Archive requirement | Immutable manifest and protected source copies |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Catalog/files/keys/offsets, compatible restore and suppression |
| Disposal method (proposed; not authorised) | Only non-protected sets eligible after dependency/hold clearance |
| Verification/evidence required | Gate 2 historical integrity records, complete manifests and isolated restore |
| Current implementation status | Gate 2 integrity recorded; general retention/restore NOT VERIFIED |

### A23: Gate evidence

| Field | Asset record |
|---|---|
| Asset/layer | Gate evidence |
| Data owner (proposed; named assignment pending) | Gate/governance owner |
| Purpose | Acceptance, incidents and control accountability |
| Sensitivity (provisional; approve per dataset) | Internal/restricted sanitized evidence; no secrets/raw identifiers |
| Authoritative source or derivative | Authoritative acceptance record |
| Retention clock/start event | Acceptance/freeze time; never silently rewrite history |
| Proposed POC retention | Preserve Gate 1/2 and subsequent Gate records; no routine disposal |
| Proposed production retention | Owner/legal-approved evidence schedule; frozen record protection persists |
| Archive requirement | Version-controlled immutable historical record and approved recovery copy |
| Legal-hold applicability | YES; common hold rule applies; unknown status BLOCKED |
| Deletion authority | Common approval rule applies; named asset owner, privacy/legal, recovery and security where restricted; separate operator |
| Replay/recovery dependency | Freeze authority, baseline decisions and audit traceability |
| Disposal method (proposed; not authorised) | Exceptional authorised evidence retirement only; never ordinary cleanup |
| Verification/evidence required | Git commit hashes, sanitized evidence, approval history and integrity |
| Current implementation status | Gate 2 evidence frozen; general evidence retention DEFINED here |

## Required approval decisions

Assign named owners and reviewers; approve asset classification, authoritative timestamps/timezones, retention purpose/period/review conditions and archive criteria; determine legal/regulatory hold authority and obligations; reconcile policy declarations without choosing silently; approve recovery windows, backup coverage, audit/evidence retention, ML release retention and residual-copy exceptions. Record decision/version/approver/date before implementing settings. Unresolved decisions remain BLOCKED.

## Gate 3.2 factual verification update

VERIFIED: all 26 managed Kafka topics match declared partitions/RF/cleanup/retention; effective retention.bytes=-1, segment.ms=604800000 and min.compaction.lag.ms=0. Broker offsets.retention.minutes=10080. Active `dp-ai-payment` has no anonymous policy, lifecycle, enabled versioning or object lock. Expected Gate 2 backup bucket is absent (DRIFT / DEFECT). A08 Staging scope must distinguish ephemeral parsing views from two VERIFIED persistent Iceberg seed tables: `staging.uganda_district_geojson` and `staging.uganda_superset_district_iso`. These derivative geography reference assets share the transformation owner proposal, use source/seed release time as their proposed clock, preserve frozen reference dependencies for POC, require owner-approved production retention/archive decisions, hold/deletion approvals and reference-aware table/history disposal with checksum/lineage evidence. They are not disposed when a dbt container ends. No retention proposal is approved by these observations.

See [runtime verification](06_runtime_verification.md) for scope, methods and limitations. These observations authorise no remediation or lifecycle mutation.
