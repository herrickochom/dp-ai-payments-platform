# STR-002A Checkpoint A Technical Design Report

- Status: Draft for approval
- Date: 2026-10-12
- Ticket: STR-002A-DEV
- Related: ADR-STR-001-003, discovery report, phased plan, Kafka -> Flink -> Kafka directive
- Approver(s): platform architecture owner; payment platform owner

## 1. Executive summary

The repository now has a verified Flink development substrate and a provisioned, empty streaming-owned PostgreSQL boundary, but its lifecycle contract remains based on synthetic PMN/PLM evidence and its canonical production correlation key is not approved. The minimal design is therefore to consume the existing technical-source topics with an isolated Flink consumer group, preserve Avro and Schema Registry as the Kafka contract, key state only when a validated correlation identifier exists, publish governed derived outputs, and treat PostgreSQL as a rebuildable serving projection rather than lifecycle authority.

1. Approve direct consumption of 'icmn.pmn.pain001' and 'cpo.plm.pain002', with no new normalised input topic unless a concrete format or governance incompatibility is demonstrated.
2. Approve 'correlation_id' as the development canonical journey key, conditional on real-emitter evidence proving population, immutability, and cross-stage uniqueness.
3. Approve the Kafka-derived-log and compacted-state boundary as authoritative, with PostgreSQL used only for rebuildable operational projections.

## 2. Current state

| Component | Path | Status | Verified |
|---|---|---|---|
| Flink session cluster | 'docker-compose.yaml:1616-1687' | Exists: JobManager and TaskManager | Compose definitions and health/dependency contracts |
| Streaming profile | 'docker-compose.yaml:1622-1623', 'docker-compose.yaml:1661-1662' | Exists | Both Flink services use profile 'streaming' |
| Flink runtime configuration | 'platform/flink/conf/flink-conf.yaml:24-128' | Exists | RocksDB, exactly-once checkpoints, restart policy, Prometheus reporter |
| Checkpoint/savepoint storage | 'docker-compose.yaml:1627-1632', 'docker-compose.yaml:1666-1671' | Exists | Dedicated MinIO prefixes and runtime credential identity |
| J1 smoke-test skeleton | 'platform/flink/jobs/j1_lifecycle_state_skeleton.py:1-120' | Exists, explicitly non-canonical | JSON-only development skeleton |
| J1 real lifecycle job | 'platform/flink/jobs/j1_lifecycle_state_skeleton.py:1-5' | Does not exist | Existing file disclaims canonical state-machine status |
| PostgreSQL database and schema | 'platform/postgres/migrations/001_create_payments_operations.sql:21-31', 'platform/postgres/migrations/001_create_payments_operations.sql:104-115' | 'payments_operations' and 'payment_lifecycle' exist | Provisioning evidence confirms application |
| PostgreSQL roles | 'platform/postgres/migrations/001_create_payments_operations.sql:33-64' | Four roles exist | Migration, writer, reader, and Grafana read-only roles |
| PostgreSQL projection tables | 'platform/postgres/migrations/001_create_payments_operations.sql:18' | Do not exist | Migration explicitly creates no tables |
| Technical source topics | 'platform/kafka/topics.yaml:18-37' | Exist | PMN and PLM declarations |
| Derived lifecycle topics | 'platform/kafka/topics.yaml:5-191' | Do not exist | No compacted state or streaming audit output declaration |
| Projection writer | 'docs/streaming/phased-plan.md:221-246' | Does not exist | Remains STR-006 design/work |
| Prometheus, Grafana, Kafka Exporter services | 'docker-compose.yaml:1-1709' | Do not exist | No service definitions; only Flink's reporter is configured |

The older discovery report states that Flink, Prometheus, and Grafana were absent ('docs/discovery/STR-001-compose-recon.md:56', 'docs/discovery/STR-001-compose-recon.md:78'). That statement is superseded for Flink by the current Compose and configuration evidence above. It remains accurate for Prometheus and Grafana services, and Kafka Exporter is also absent.

## 3. Current TechnicalPaymentEvent schema and producer behaviour

'TechnicalPaymentEvent' is an Avro record ('contracts/kafka/technical_payment_event.avsc:1-35'). Mandatory fields without a nullable union or default include 'event_id', 'event_type', 'correlation_id', 'end_to_end_id', 'business_reference', 'stage', 'status', 'event_timestamp', 'processing_timestamp', 'source_system', 'target_system', 'channel', 'retry_count', and 'is_timeout'. 'event_family' and 'currency' are mandatory values with defaults. Optional-with-default fields use a 'null' union and default 'null': 'instruction_id', 'transaction_id', 'uetr', 'x_trace', 'instructed_amount', 'latency_ms', error fields, and provider/channel extension fields ('contracts/kafka/technical_payment_event.avsc:5-35'). There are no optional fields lacking defaults.

The producer maps 'icmn.pmn.pain001' and 'cpo.plm.pain002' to 'TECHNICAL_PAYMENT_EVENT' ('services/payment-producer/kafka_producer.py:80-87'), parses the technical XML fields into the snake-case Avro record ('services/payment-producer/kafka_producer.py:398-463'), selects the first populated stable key from 'end_to_end_id', 'instruction_id', 'correlation_id', and 'transaction_id', and rejects a record with no stable key ('services/payment-producer/kafka_producer.py:119-154'). It serialises values with Schema Registry's 'AvroSerializer' and string keys ('services/payment-producer/kafka_producer.py:700-716').

The generators create deterministic synthetic technical events, including stable identifiers and telemetry, rather than evidence of live emitters ('services/payment-xml-generator/generator_common.py:364-430', 'services/payment-xml-generator/icmn_generator.py:111-122', 'services/payment-xml-generator/cpo_generator.py:62-73'). The current Flink skeleton uses 'SimpleStringSchema' and 'json.loads' ('platform/flink/jobs/j1_lifecycle_state_skeleton.py:44-49', 'platform/flink/jobs/j1_lifecycle_state_skeleton.py:78-86'); it cannot directly decode the producer's Schema Registry Avro wire payload. B2 therefore requires an Avro-aware Flink source configured for the registered subject, or an explicitly approved format change. Treating Avro bytes as JSON is not compatible.

## 4. PMN/PLM source and correlation evidence

The verified technical sources are 'icmn.pmn.pain001' and 'cpo.plm.pain002' ('platform/kafka/topics.yaml:18-21', 'platform/kafka/topics.yaml:33-36'). Synthetic generation populates 'CorrelationId', 'PaymentInstructionId' (the schema's 'instruction_id', corresponding to 'InstrId' in business XML), 'EndToEndId', 'TransactionId', 'UETR', 'BusinessReference', and 'XTrace' ('services/payment-xml-generator/generator_common.py:418-430'; 'services/payment-producer/kafka_producer.py:398-430').

| Correlation level | Candidate identifiers | Evidence and boundary |
|---|---|---|
| Message-level | 'event_id', source message identifiers | Identifies one event/message; deliberately unsuitable as a journey partition key |
| Instruction-level | 'InstrId' / 'PaymentInstructionId', 'EndToEndId' | Business generators populate these for payment instructions ('services/payment-xml-generator/icmn_generator.py:78-80') |
| Transaction-level | 'TransactionId', 'UETR' | Transaction and cross-network identifiers; nullable in the technical Avro contract |
| Journey-level | 'CorrelationId', 'BusinessReference', 'XTrace' | Deterministically derived from the synthetic loan journey, but production uniqueness is not proven |

'correlation_id' is the best development canonical key because it is required by the Avro schema and is generated consistently at journey level ('contracts/kafka/technical_payment_event.avsc:6', 'services/payment-xml-generator/generator_common.py:424-430'). This recommendation is provisional: repository evidence does not prove that real emitters exist, that the value remains immutable across PMN and PLM, or that it is globally unique. 'EndToEndId' is the required fallback candidate for investigation, not an automatic substitute, because its business scope and reuse rules require owner confirmation.

## 5. Existing topic reuse vs. new canonical topic

The Kafka -> Flink -> Kafka directive is satisfied by consuming both existing PMN/PLM technical topics through a new, isolated Flink consumer group. This avoids changing the producer, existing consumer offsets, topic definitions, retention, or the lakehouse route. Direct consumption is preferred because both topics already map to the same Avro record and producer path ('services/payment-producer/kafka_producer.py:80-87', 'services/payment-producer/kafka_producer.py:700-716').

A new normalised input topic is not justified merely to give Flink one subscription or one name: Flink can subscribe to both sources and normalise after deserialisation. It becomes justified only if approved evidence shows a technical incompatibility that cannot safely be resolved at the source boundary, such as different wire formats or schema subjects, irreconcilable field semantics, incompatible access/retention controls, or a required replay log whose retention and governance cannot be applied to the source topics. Any such topic must preserve source topic, partition, offset, schema identity, event identity, and replay provenance. Derived state, audit, exception, and enriched-output topics are separate output contracts, not a replacement input.

## 6. Proposed minimal schema changes and compatibility

The existing 'TechnicalPaymentEvent' schema is sufficient for direct semantic consumption: it carries event identity, candidate correlation identifiers, stage/status, event and processing times, telemetry, retry/timeout, and error data ('contracts/kafka/technical_payment_event.avsc:4-35'). The required Flink change is an Avro/Schema Registry deserialiser, not a JSON parser. No input-schema change is required for Checkpoint A.

If real-emitter validation reveals missing provenance, only backward-compatible fields with nullable unions and default 'null' should be added, for example a governed source-message identifier or contract version. Existing field names, types, meanings, and defaults must not change. Derived output schemas should be new records/subjects rather than additions that overload the source event.

Schema Registry currently applies global 'BACKWARD_TRANSITIVE' compatibility by default ('platform/kafka/configure-schema-registry.sh:4-15'). New streaming subjects should use 'BACKWARD_TRANSITIVE' unless a documented consumer/replay requirement demands a stricter mode. The script needs extension only if subject-specific compatibility must differ from the global setting; otherwise it already establishes the intended mode. B2 must register and compatibility-test every approved subject before enabling a producer.

## 7. Kafka keying and Flink correlation design

Use validated 'correlation_id' as the Flink keyed-state key for development. Before production, the payment platform owner must demonstrate non-empty population, immutability across PMN/PLM stages, collision bounds, and the relationship to 'EndToEndId', 'TransactionId', and 'UETR'. State, timers, deduplication, transitions, and joins must use the same approved correlation contract.

An event without a reliable canonical key must not enter lifecycle keyed state. Route it to the governed failure output with source topic/partition/offset, event identity, schema identity, and a reason code; increment a bounded missing-key counter. The literal 'unknown' must never be a shared lifecycle key because it merges unrelated payments. The current skeleton's fallback to 'unknown' ('platform/flink/jobs/j1_lifecycle_state_skeleton.py:101-114') is acceptable only as evidence of a smoke-test limitation and must not survive into J1.

Kafka partitioning key and lifecycle correlation key are related but distinct contracts. The producer currently prefers 'end_to_end_id' before 'correlation_id' ('services/payment-producer/kafka_producer.py:119-154'), so source partition ordering cannot be assumed to equal canonical-key ordering. Flink must re-key by the canonical key after deserialisation. Derived topics should use that canonical key as the Kafka record key where per-journey ordering and compaction are required; audit or failure outputs may retain an event/provenance key when their contract requires per-record uniqueness.

## 8. Lambda/Kappa responsibility mapping

Topic names remain approval decisions; the descriptions below are contracts, not declarations.

| Component | Lambda responsibility | Kappa responsibility |
|---|---|---|
| J1 lifecycle state machine | Evaluate transitions and current state with low latency | Deterministically rebuild keyed state from retained events |
| J2 gap detector | Detect missing/out-of-order expected stages and emit bounded metrics | Re-evaluate gaps under controlled replay without duplicate cases |
| J3 SLA evaluator | Run event-time timers and emit SLA breaches | Restore timers from checkpoints/replay and reproduce auditable outcomes |
| J4 business-telemetry joiner | Enrich operational context for live decisions | Reproduce joins from versioned, retained inputs with defined lateness |
| J5 projection writer | Upsert current views and append operator-visible exceptions | Rebuild disposable PostgreSQL projections and persist replay checkpoints |
| Compacted current-state topic | Serve latest per-journey state to downstream consumers | Recoverable current-state projection with tombstone/rebuild rules |
| Lifecycle transition/audit topic | Supply recent transition activity | Retained ordered audit and replay provenance |
| Operational exception output topic | Feed low-latency exception handling | Preserve classification and reprocessing history |
| Enriched lifecycle output topic | Supply joined operational records | Retain the versioned join result where its contract requires replay |
| Streaming failure/DLQ topic | Isolate invalid records without blocking live flow | Support authorised, traceable replay without deleting failure evidence |

J1-J4 align with the phased plan's state, gap, SLA, and join responsibilities ('docs/streaming/phased-plan.md:140-219'). J5 is the projection-writer boundary required by STR-006; its exact runtime form is unresolved and it is not a Flink state backend.

## 9. PostgreSQL operational database design

Verified provisioned state is the 'payments_operations' database, 'payment_lifecycle' schema, and four roles: 'payments_ops_migration', 'payments_ops_writer', 'payments_ops_reader', and 'grafana_payments_ro' ('platform/postgres/migrations/001_create_payments_operations.sql:7-18'; 'docs/streaming/STR-006-postgres-provisioning-evidence.md:12-19'). There are zero streaming tables.

| Candidate table | Purpose | Write/replay rule |
|---|---|---|
| 'current_state' | One current row per canonical journey key for drill-down | Deterministic upsert guarded by event/version ordering |
| 'lifecycle_transitions' | Append-only bounded operational transition history | Unique event/provenance constraint prevents replay duplicates |
| 'operational_exceptions' | Queryable exception/case projection | Separate derived lifecycle facts from operator workflow fields |
| 'projection_checkpoints' | Writer progress and rebuild generation metadata | Transactionally advance only with committed projection writes |

J5 should consume approved derived Kafka outputs, write with 'payments_ops_writer', expose no DDL privilege, and make every operation idempotent. Current-state upserts require a deterministic version comparator; append tables require a stable unique event/provenance key. Replays use a distinct rebuild generation or isolated consumer group, suppress duplicate side effects, and reconcile before cutover. Kafka retained history and the compacted state topic remain authoritative.

The existing SQL was applied manually and documents that no tables are created ('platform/postgres/migrations/001_create_payments_operations.sql:18'). PostgreSQL init scripts do not re-run against an existing data volume, so STR-006 must adopt a versioned, independently runnable migration mechanism under the existing migration boundary, record applied versions/checksums, and test forward migration plus clean rebuild. A separate production PostgreSQL instance is warranted only after capacity, availability, backup/restore, data classification, network isolation, ownership, and failure-domain requirements show the shared platform server cannot meet the approved service objectives.

## 10. Prometheus/Grafana integration boundaries

Metric contracts begin with counters for processed, invalid, late, deduplicated, transition, exception, DLQ, and replay events; gauges for job health, consumer lag, active keyed state, pending timers, and projection freshness; and histograms for event latency, processing latency, checkpoint duration, and projection-write latency. Labels are restricted to bounded vocabularies such as job, source topic, stage, status, reason class, and replay mode. STR-005 must set a numeric series/cardinality budget and test it before STR-008 deployment.

Payment identifiers, 'correlation_id', 'event_id', transaction/instruction IDs, UETR, raw error text, source offsets, and business references must never be Prometheus labels. They belong in controlled logs or PostgreSQL drill-down records subject to classification and retention policy.

Grafana uses Prometheus for aggregate metrics and alerts. It uses PostgreSQL only for justified per-payment drill-down through the existing 'grafana_payments_ro' read-only role ('platform/postgres/migrations/001_create_payments_operations.sql:16-17', 'platform/postgres/migrations/001_create_payments_operations.sql:135-138'). Dashboard naming, folder ownership, datasource secret delivery, alert routing, on-call ownership, and runbook links are STR-008 approval concerns. Flink already exposes a Prometheus reporter endpoint, but no Prometheus, Grafana, or Kafka Exporter service exists ('platform/flink/conf/flink-conf.yaml:90-98').

## 11. Exact files proposed for creation or modification

Where the repository has not approved an exact filename, this table names the existing directory boundary and explicitly leaves the filename undecided rather than inventing one.

| Path | Change type | Ticket | Purpose |
|---|---|---|---|
| 'docs/streaming/STR-002A-checkpoint-A.md' | Create | STR-002A-DEV | This approval report and correlation design |
| 'docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md' | Modify, governed and conditional | STR-002A / STR-009 | Remove provisional caveat after contract approval; later record validation status |
| 'contracts/kafka/technical_payment_event.avsc' | Modify only if evidence requires additive fields | STR-002A / STR-002B B2 | Backward-compatible source-contract extension |
| 'platform/kafka/configure-schema-registry.sh' | Modify only if subject-specific mode is approved | STR-002B B2 | Configure compatibility where global mode is insufficient |
| 'platform/kafka/topics.yaml' | Modify by additions only | STR-002B B2 / STR-004 / STR-007 | Declare separately approved derived, compacted, audit, or failure topics |
| 'platform/flink/jobs/j1_lifecycle_state_skeleton.py' | Modify or replace after contract approval | STR-003 | Remove JSON-only smoke-test behaviour and implement canonical J1 contract |
| 'platform/flink/' | Create new files; exact names pending ticket design | STR-003 through STR-007 | J2-J5 jobs, tests, and job configuration |
| 'platform/postgres/migrations/' | Create new versioned migration; exact name pending STR-006 | STR-006 | Create projection tables, constraints, grants, and migration ledger |
| 'docker-compose.yaml' | Modify | STR-006 / STR-008 | Projection connection boundary and observability services |
| 'platform/prometheus/' | Create new files; exact names pending STR-008 | STR-008 | Scrape and alert-rule configuration |
| 'platform/grafana/' | Create new files; exact names pending STR-008 | STR-008 | Datasource and dashboard provisioning |
| 'platform/docker/dockerfiles/' | Create only if custom images are approved | STR-008 | Package observability components when upstream images are insufficient |
| 'docs/streaming/' | Create contract/evidence files; exact names pending each ticket | STR-003 through STR-009 | Event-time, state, metrics, projection, replay, operations, and acceptance evidence |

No change is proposed to 'payment_event.avsc', the producer, generators, existing topic entries, or '001_create_payments_operations.sql' at this checkpoint.

## 12. Security, governance, and regression risks

No frozen-set file or state is proposed for modification. Additions to 'platform/kafka/topics.yaml' must not alter its existing entries; the ADR changes are governed exceptions requiring approval. Trino, Nessie, Superset, transform-runtime migrations, PDM source Postgres, existing volumes, offsets, retry/DLQ records, and historical data remain untouched ('docs/streaming/file-plan.md:123-139').

Credentials remain runtime-only. Flink uses the dedicated checkpoint identity; J5 uses 'payments_ops_writer'; migrations use 'payments_ops_migration'; application readers inherit 'payments_ops_reader'; Grafana uses only 'grafana_payments_ro'. No role should receive superuser, role-creation, database-creation, or schema-creation rights beyond the migration owner boundary ('platform/postgres/migrations/001_create_payments_operations.sql:9-18', 'platform/postgres/migrations/001_create_payments_operations.sql:108-138').

Technical events may contain identifiers and error descriptions that could become personal or linkable data when joined with business records. Before persistence, the security owner must classify each field, approve minimisation, masking, retention, deletion, encryption, audit, and access requirements, and decide whether free-text errors are allowed. Prometheus receives no per-payment or free-text values. PostgreSQL stores only approved operational fields.

The lakehouse route's regression risk is bounded by isolated Flink consumer groups, no offset resets, no producer changes, no mutation of source topics, and no changes to Raw/Iceberg/Nessie/Trino/Superset paths. Security-owner input remains required for data classification, cross-topic joins, DLQ payload handling, Grafana drill-down authorisation, transport/authentication controls, retention, and production network boundaries.

## 13. Updated dependencies and acceptance criteria

| Ticket | Blocked by | Unblocks when complete |
|---|---|---|
| STR-002A-DEV | Approval of this checkpoint and access to real-emitter evidence | STR-002B B2 contract work and STR-003 |
| STR-002B B1 | None remaining | Complete; smoke evidence exists ('docs/streaming/STR-002B-smoke-test.md:77-109') |
| STR-002B B2 | STR-002A approved source, schema, subjects, compatibility, key, and topic decision | STR-003 |
| STR-003 | STR-002A and STR-002B B2 | STR-004 and STR-005 |
| STR-004 | STR-003 | STR-005, STR-006, and replay-state contracts |
| STR-005 | STR-003 and STR-004 | STR-006 and STR-008 |
| STR-006 | STR-004 and STR-005 | STR-008 and STR-009 |
| STR-007 | STR-003 and STR-004 | STR-009 |
| STR-008 | STR-005 and STR-006 | STR-009 |
| STR-009 | STR-006, STR-007, and STR-008 | Final validation/sign-off |

STR-002A-DEV must produce approved evidence for real emitters or an explicit development-only limitation; the field/stage/status/event-time contract; canonical-key rules and missing-key behaviour; source and derived topic topology; Avro subject and compatibility decisions; and named owners. Until then, B2 must not declare contract-bound topics or schemas.

STR-009 acceptance must cover: representative end-to-end Kafka -> Flink -> Kafka -> PostgreSQL/Prometheus/Grafana flow; schema compatibility; partition/re-key ordering; duplicates; late and out-of-order events; missing keys; checkpoint restore; retained-log replay; compacted-state and PostgreSQL rebuild; DLQ isolation and authorised reprocessing; projection idempotency; metric-cardinality limits; datasource least privilege; component failure/recovery; lakehouse non-regression; evidence capture; and owner sign-off. Destructive recovery tests run only in an isolated test environment.

## 14. Decisions requiring approval

1. Question: Should Flink consume the two existing PMN/PLM topics directly? Recommended answer: Yes, through an isolated consumer group. Consequence of not deciding: B2 cannot fix its input topology or consumer contract.
2. Question: Is a new canonical normalised input topic required? Recommended answer: No, unless an evidenced wire-format, semantic, governance, or retention incompatibility meets Section 5's threshold. Consequence of not deciding: topic and schema scope may expand without a technical need.
3. Question: What is the canonical lifecycle key? Recommended answer: Use 'correlation_id' for development, conditional on production evidence; never use 'unknown'. Consequence of not deciding: J1-J4 state, timers, joins, and deduplication cannot be correct.
4. Question: Is Avro the Flink input format? Recommended answer: Yes; replace the skeleton's JSON deserialisation with Schema Registry-aware Avro consumption. Consequence of not deciding: the current producer and Flink skeleton remain wire-incompatible.
5. Question: What Schema Registry compatibility applies? Recommended answer: 'BACKWARD_TRANSITIVE' for source and new streaming subjects unless a stricter subject-specific requirement is approved. Consequence of not deciding: B2 cannot register or gate schema evolution safely.
6. Question: Which store is authoritative? Recommended answer: retained Kafka history plus governed derived Kafka state; PostgreSQL is rebuildable serving data only. Consequence of not deciding: replay and recovery may create competing lifecycle authorities.
7. Question: Is the four-table PostgreSQL projection scope accepted? Recommended answer: approve it as the STR-006 design baseline, subject to field classification and retention review. Consequence of not deciding: J5, Grafana drill-down, migration, and rebuild contracts remain blocked.
8. Question: Who owns metrics, dashboards, alerts, and incident routing? Recommended answer: assign names and service objectives in STR-008, with metric names/cardinality fixed in STR-005. Consequence of not deciding: monitoring can deploy without actionable alerts or accountable responders.

## 15. Known unknowns

- Real lifecycle telemetry emitters, owners, and conformance evidence were not found.
- Production population, uniqueness, immutability, and scope of each candidate correlation identifier are unproven.
- Actual Schema Registry subject names and whether PMN/PLM share a compatible subject were not determined from the required inputs.
- Stage/status vocabulary, legal transitions, terminal states, event-time authority, lateness, watermark, deduplication TTL, and timer thresholds are undecided.
- Derived topic names, partitions, retention, compaction, tombstones, schemas, ACLs, and replay horizons are undecided.
- The required audit/enriched/failure topic split and whether existing 'payment-events.dlq' can satisfy streaming-route governance are undecided.
- J4 business inputs and temporal join/versioning semantics are not approved.
- J5 runtime technology, delivery semantics, batching, retry policy, and rebuild cutover procedure are not approved.
- PostgreSQL operational retention, capacity, recovery objectives, backup ownership, and production-instance topology are unknown.
- Data classification and PII status of identifiers, joined attributes, errors, DLQ payloads, and Grafana drill-down fields are not approved.
- Prometheus cardinality budget, retention, authentication, scrape topology, dashboard inventory, alert thresholds, on-call owner, and routing are unknown.
- Production Flink sizing, parallelism, high availability, checkpoint intervals, and recovery objectives remain unresolved.
















