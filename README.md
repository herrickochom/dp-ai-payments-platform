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

Known limits: no builder artifact persistence, overlap detection, renderer, or
Superset adapter is implemented; joins are validated but single-primary-dataset
specifications are the Phase 2 execution boundary. Phase 3 may add bounded
Visualization and Dashboard Agents plus a Superset adapter without changing the
builder domain models.
