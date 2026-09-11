# dp-ai-payments-platform

Governed ISO 20022 payments lakehouse with a Parish Development Model (PDM)
oversight domain. Kafka archives every event to an immutable Avro Raw layer;
dbt replays Raw into Iceberg through Nessie and owns all Bronze-to-consumption
modelling. See [docs/architecture/unified-payments-pdm-platform.md](docs/architecture/unified-payments-pdm-platform.md)
for domain boundaries and status semantics.

## Repository layout

| Path | Owns |
|---|---|
| `services/` | Runtime code, one directory per deployable |
| `transform/dbt/` | The medallion model: `br_` → `stg_` → `slv_` → `gld_` → `cns_` |
| `platform/` | Infrastructure only: container definitions and per-component config |
| `contracts/` | Schemas and registries that cross service boundaries |
| `docs/` | Architecture and domain documentation |
| `ops/` | Operator scripts |
| `data/` | Synthetic fixtures — no real NINs, names, phones, or accounts |
| `tests/` | Cross-service tests |

```text
services/
  payments-api/            payments-ingestor/
  kafka-consumer-events/   payment-producer/
  payment-xml-generator/
  agents/{dq,modeling}/    shared/            # requirements shared by services
transform/dbt/             # models, macros, profiles.yml, dbt_project.yml
platform/
  docker/dockerfiles/      # every image definition
  entrypoints/  config/
  kafka/  minio/  nessie/  postgres/  trino/  superset/  duckdb/
contracts/
  iso20022/xsd/            # ISO 20022 message schemas
  kafka/                   # Avro value schemas (*.avsc)
  metadata/                # table and source registries
docs/
  architecture/  domains/  pdm/
ops/
docker-compose.yaml        # the single authoritative compose file
```

`docker-compose.yaml` at the root is authoritative: 29 services across the
`analytics`, `dev`, `dbt`, `duckdb`, `pdm`, `trino`, and `metabase` profiles.
Build contexts are always the repository root, so every `COPY` in
`platform/docker/dockerfiles/` is written as a repo-relative path.

## Data flow

```text
Source JSON/XML
  -> Kafka + Schema Registry
  -> services/kafka-consumer-events        (the only Kafka consumer)
  -> s3://dp-ai-payment/raw/v2/**.avro     (immutable Avro archive)
  -> transform/dbt br_payment_events       (Raw replay -> Bronze Iceberg)
  -> br_* / stg_* / slv_* / gld_* / cns_*  (Iceberg via Nessie)
```

Bronze onward is Iceberg in the Nessie catalog, so tables are addressed as
`lakehouse.<layer>.<model>` rather than by S3 path. There is no Spark, Hudi, or
Hive Metastore in this platform.

## Kafka architecture and guarantees

Local development deliberately runs one KRaft broker with RF=1. Containers use
`kafka:9092`; host tools use `localhost:9094`. `platform/kafka/topics.yaml` is
the sole topic manifest. Its reconciler creates topics, increases partition
counts, and updates retention/cleanup; it fails on unsafe partition decreases or
replication drift.

The producer uses stable business keys: loan ID; loan/payment ID for repayments;
beneficiary, agent, SACCO, household, business-plan, or special-group ID; payment
or transaction ID for wallet events; and ISO message ID for XML payments. A stable
source filename is the explicit final fallback. `event_id` remains unique event
identity and is never a fallback partition key. Related entities consequently map
to the same partition. Kafka guarantees order inside a partition, not across a
topic.

The `payment-events-consumer` group archives 24 domain topics with
**at-least-once Kafka consumption plus a deterministic/idempotent MinIO sink**
(not physical exactly-once; auto commit is disabled and durable storage precedes
a synchronous commit). Raw identity is `(topic, partition, offset)` and ends in
`.../offset=O/record.avro`. A crash before storage replays; a crash after storage
but before commit replays as a harmless no-op; a crash after commit continues at
the subsequent offset.

Transient failures retry inline with configurable bounded exponential backoff and
are audited in shared `payment-events.retry`. Permanent or exhausted failures go
to `payment-events.dlq` with **at-least-once publication plus a deterministic
source-coordinate failure identity and duplicate-safe replay** (again, not
physical exactly-once):

* Every DLQ envelope carries `failure_id = <original_topic>:<original_partition>:<original_offset>`
  and the DLQ record is keyed by it; `business_key` is preserved separately as
  business metadata. The same source record reproduced on any replay therefore
  always maps to the same deterministic identity.
* **Offset invariant:** a source offset advances only after Raw storage succeeds
  durably OR DLQ publication succeeds durably. If a poison record's DLQ
  publication fails, the consumer does NOT commit it, does NOT process later
  offsets, logs a `fail_stop_dlq_publish_failed` CRITICAL, and exits non-zero.
  Docker's restart policy restarts it and Kafka replays the uncommitted source
  record. A later offset can never be committed past the failed record.
* The DLQ topic (cleanup.policy=delete) can still physically hold two records with
  the same deterministic key (e.g. the consumer crashed between DLQ ack and source
  commit, so Kafka replayed the record and the DLQ was published again). Those
  duplicates are deterministically identifiable by `failure_id`, and replay
  tooling is duplicate-safe: it scans the DLQ topic, groups envelopes by
  `failure_id`, and refuses to re-execute a replay for an identity that has
  duplicate envelopes unless `--allow-duplicate-replay` is passed explicitly.

Replay is deliberate:

```bash
docker compose exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group payment-events-consumer --describe
docker compose run --rm --entrypoint python3 payment-consumer-events \
  /app/replay_dlq.py --partition 0 --offset 12 --reason "schema repaired"
# Inspect first; repeat with --execute to replay that one record. If the output
# warns about duplicate DLQ envelopes for the same failure_id, use
# --allow-duplicate-replay only when a duplicate source replay is explicitly
# accepted.
```

One consumer is the local default. `docker compose up --scale
payment-consumer-events=N` distributes partitions safely in the same group. Per-
topic maximum parallelism is its partition count (4, 6, or 8).

Schema Registry enforces `BACKWARD_TRANSITIVE`. Add fields with defaults (usually
nullable); use Avro aliases for renames; only compatible type promotions are safe.
Removing required fields, renaming without aliases, or changing incompatible types
requires an explicitly versioned contract/topic. Kafka security protocol, SASL,
TLS CA, and Schema Registry credentials are environment-configurable; local uses
PLAINTEXT only for convenience.

`services/kafka-consumer-bronze` is intentionally not deployed because dbt owns
Bronze through Consumption. Unused reconciliation topics were replaced by retry/
DLQ topics. The incomplete landing-ingestion and duplicate dev producer profiles
are retained under `future-disabled`, preventing accidental startup.

### Deployment shapes

```text
LOCAL DEVELOPMENT
Sources -> Producer -> single-node Kafka + Schema Registry -> domain partitions
        -> payment-events-consumer -> retry/DLQ -> idempotent MinIO Raw
        -> dbt/DuckDB -> Iceberg/Nessie -> Trino -> Superset/Metabase/ML

FUTURE CLOUD
Sources -> Producers -> HA managed/distributed Kafka (3+ brokers, RF=3, min ISR=2)
        -> replicated partitions -> horizontally scaled consumer group
        -> bounded retry/DLQ -> cloud object-storage Raw -> Iceberg lakehouse
        -> query/analytics layer -> BI/ML
```

Production must replace the local single broker, RF=1, and PLAINTEXT with an HA
cluster or managed equivalent, RF=3, min ISR=2, replicated internal topics,
TLS/SASL and ACLs, managed secrets, metrics/lag alerts, and durable object-store
controls. `platform/kafka/production.env.example` records the portable client
boundary without introducing any cloud-vendor SDK or endpoint.

## Run

```bash
docker compose up -d                                    # core services
docker compose --profile dbt run --rm duckdb build      # build the lakehouse
docker compose --profile analytics up -d trino superset  # query and dashboards
```

## Agent Phase 1

Phase 1 adds one bounded, synchronous `agent-api` service above the lakehouse:

```text
User -> Agent API -> Orchestrator
                       |-- Data Discovery Agent -> controlled metadata tools
                       `-- Analytics Agent -> semantic request -> controlled query tools
                                                        |
                                                     Trino
                                                        |
                                                Iceberg / Nessie
```

Agents choose objectives and tools; tools validate permissions, SQL shape, row
limits and timeouts before Trino executes anything. The service never consumes
Kafka and never exposes unrestricted SQL. Data Discovery searches live Trino
`information_schema` metadata and labels candidate joins as inferred. Analytics
first discovers columns, emits a reusable semantic analytical request
(datasets, dimensions, measures, filters, grouping, ordering and limit), then
executes only a parsed, single-statement, read-only query. Evidence identifies
the metadata objects and Trino query used; query errors are returned as
structured failures instead of invented answers.

The request permissions boundary currently supports metadata discovery,
read-query execution and per-request row limits. It is intentionally lightweight
so later governance can strengthen it without changing agent contracts. Model
provider selection uses `AGENT_MODEL_PROVIDER`/`AGENT_MODEL`; Phase 1 defaults to
`disabled`, so deterministic tools require no API key. `rag.search` is registered
but reports `not_configured`; no vector database is introduced.

```bash
docker compose --profile agents up -d agent-api
curl http://localhost:7010/agents
curl http://localhost:7010/agents/health
curl -X POST http://localhost:7010/agents/query \
  -H 'content-type: application/json' \
  -d '{"objective":"What tables contain PDM repayment information?"}'
curl -X POST http://localhost:7010/agents/query \
  -H 'content-type: application/json' \
  -d '{"objective":"What is repayment performance by district?"}'
```

Configuration is environment-driven: `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`,
`TRINO_CATALOG`, `TRINO_SCHEMA`, `TRINO_HTTP_SCHEME`, optional
`TRINO_PASSWORD`, `AGENT_MAX_ROWS`, `AGENT_QUERY_TIMEOUT_SECONDS`,
`AGENT_MAX_QUERY_LENGTH`, and `AGENT_MAX_TOOL_CALLS`. Run focused tests with
`python3 tests/test_agent_phase1.py` or inside the built agent image.

Known Phase 1 limits: routing and semantic selection are deliberately bounded;
runtime lineage and RAG are unavailable; inferred joins are not database-backed
foreign keys; and only discovery plus geography/repayment analytics are covered.
Phase 1 itself stops at the semantic analytical request and controlled query;
the separately bounded Phase 2 builder layer below consumes that stable contract.

## Agent Phase 2 builder integration

Phase 1 remains frozen. Phase 2 adds deterministic, vendor-neutral builder
contracts after the Analytics Agent's semantic request:

```text
Analytics Agent -> SemanticAnalyticalRequest -> controlled builder tools
  -> DataSourceSpec -> VisualizationSpec(s) -> DashboardSpec
```

The Data-Source Builder resolves the requested `catalog.schema.table` and every
field through the existing Trino metadata tool, accepts only supported numeric
aggregations, preserves query/semantic provenance, and labels proposed joins as
confirmed or inferred. The Visualization Builder supports `kpi`, `table`, `bar`,
`line`, and `scatter`; it validates field existence, roles, chart shape, sorting,
and limits. The Dashboard Builder validates unique visualization IDs, filter
fields, data-source dependencies, and a bounded 12-column layout.

Builders are deterministic and require no LLM. They return validated definitions
only (`persistence=returned_only`); the existing imperative Superset dashboard
scripts remain unchanged and are a future candidate for a BI adapter behind
these contracts. Builders cannot bypass Phase 1 metadata/read permissions, do
not emit UI code, and do not publish dashboards automatically.

Endpoints:

```text
POST /builders/data-source
POST /builders/visualization
POST /builders/dashboard
POST /agents/build
```

`/agents/build` is a bounded composition workflow, not a Dashboard Agent. It
reuses Analytics output, builds a data source, deterministically recommends a KPI,
category/time chart, and underlying table when supported, then validates and
returns a dashboard definition. Run Phase 2 tests with
`python3 tests/test_agent_phase2.py`.

Phase 2 itself has no artifact persistence, overlap detection, renderer, or BI
coupling; joins are validated but single-primary-dataset specifications are its
execution boundary. The separate Phase 3 layer below consumes these contracts.

## Agent Phase 3 visualization, dashboards, and Superset

Phase 3 extends the frozen contracts without putting BI-specific fields in them:

```text
User -> Orchestrator -> Discovery / Analytics / Visualization / Dashboard Agents
  -> typed Phase 2 specifications -> deterministic builders -> BIAdapter
  -> Superset
```

The bounded Visualization Agent uses a deterministic fallback when no model is
configured: one measure becomes a KPI; category plus measure becomes a bar;
time plus measure becomes a line; two measures can become a scatter; and detail
fields become a table. Every proposal passes the existing Visualization Builder.
The Dashboard Agent orders validated visualizations, selects a primary view, and
proposes a 12-column layout; the Dashboard Builder remains the authority.

`SupersetAdapter` is vendor-specific and replaceable. It maps validated physical
datasets, `kpi`/`table`/`bar`/`line`/`scatter` types, layouts, and filters to
Superset REST resources. Stable spec IDs are embedded in asset names/slugs, so a
repeat publication performs lookup-and-update/reuse rather than deliberately
creating new assets. This is idempotent intent, not transactional exactly-once:
a remote failure may leave already-created assets, and the response reports the
partial completion accurately.

Publishing is opt-in and separate from read/build permissions. It requires
`can_publish_bi_assets=true` plus `SUPERSET_PASSWORD`. Dry-run is the default and
returns intended actions without changing Superset.

```text
POST /agents/visualize
POST /agents/dashboard       # publish=false by default
POST /publish/superset       # accepts a validated DashboardSpec
```

Environment: `SUPERSET_URL`, `SUPERSET_USERNAME`, `SUPERSET_PASSWORD`, and
`SUPERSET_DATABASE_NAME`. Current limits include no cross-dataset native-filter
scope translation, no application-side spec database, no transactional rollback
of partial Superset publication, and deterministic (not model-driven) agent
fallbacks. Phase 4 may add Data Quality and Insight Agents; it must continue to
use these controlled contracts.

## Agent Phase 4 data quality and insight intelligence

Phase 4 extends the same Agent API and orchestrator; it does not introduce a
second query layer or autonomous remediation path:

```text
User / future scheduler
  -> Agent API -> Orchestrator
       -> Data Discovery Agent
       -> Analytics Agent
       -> Visualization Agent
       -> Dashboard Agent
       -> Data Quality Agent
       -> Insight Agent
  -> controlled metadata / DQ / read-query tools
  -> Trino, dbt rule metadata, and platform health
  -> evidence-backed findings, insights, and advisory recommendations
```

The Data Quality Agent answers bounded trust questions. At image build time the
existing dbt model YAML is copied read-only into the Agent API image. The rule
catalogue projects dbt `not_null`, `unique`, `accepted_values`, and
`relationships` tests into typed vendor-neutral rules while retaining the YAML
path as provenance. Callers may also submit typed `range`, `freshness`, and
`volume` rules through `POST /dq/check`; these are labelled `phase4_config` or
`inferred`, never presented as dbt rules. Rules cannot contain SQL. Identifiers
are validated against Trino metadata and generated aggregate queries still pass
through the Phase 1 single-SELECT validator, timeout, and row limits.

`dq.summary` deliberately reuses `iceberg.silver.slv_pdm_dq_results` for the
repository's existing financial consistency, cross-domain relationship, and
payment reconciliation rules instead of recreating that logic. `dq.profile`
profiles at most five requested fields with aggregate row/null/distinct/min/max
statistics. Failure samples are absent by default; the separate
`can_view_data_quality_samples` permission exposes at most five values and all
values are masked. Recommendations are advisory only: no source correction,
delete/update, Kafka offset change/replay, dbt rebuild, or object-store write is
implemented.

The Insight Agent answers ranking and observed-change questions using the same
metadata and read-query tools as Analytics. It compares only the latest two
values of an actual discovered time field, preserves both periods and the Trino
query ID, calculates absolute/relative movement, and can show dimension ranking
movement. Its explainable anomaly methods are an absolute percentage-change
threshold (20% platform monitoring default, or an explicit request threshold)
and a population z-score flag (`abs(z) >= 2`) for cross-sectional rankings.
These are monitoring flags, not causal models. Output uses observed/associated
language and never invents a cause. In particular,
`cns_pdm_executive_monthly_trend.cohort_principal_repayment_rate` is always
warned as approval-cohort current state rather than reconstructed month-end
portfolio history. If two actual periods or a temporal field are unavailable,
the response returns a limitation and does not invent dates.

Phase 4 permissions add `can_run_data_quality_checks`,
`can_view_data_quality_samples`, and `can_generate_insights`; metadata,
read-query, and row-limit permissions remain mandatory. This is a lightweight
boundary designed for the Phase 5 Governance Agent to strengthen with data
classification and policy enforcement without changing the result contracts.

Endpoints:

```text
POST /agents/data-quality
POST /agents/insights
GET  /dq/rules?dataset=iceberg.silver.slv_pdm_loans
POST /dq/check
POST /dq/profile
```

The older Compose `dq-agent` service is retained unchanged for compatibility.
Inspection found that it only logs connectivity settings and a periodic
heartbeat; it has no rules, queries, findings, or remediation. It is therefore
not a competing production path. New request/response quality work belongs to
the Agent API; a later operational migration may remove the heartbeat service
after its users and deployment expectations are confirmed.

Known Phase 4 limits: dbt custom singular tests are documented but not compiled
into arbitrary agent SQL; source freshness has no repository declaration to
import; schema/volume rules require explicit configuration; monitoring is
request/response only; anomaly detection is deliberately simple; and there is
no causal inference, scheduler, alert delivery, remediation, PII policy engine,
or full governance enforcement. Run focused tests with
`python3 -m unittest tests.test_agent_phase4` in the Agent API environment.
