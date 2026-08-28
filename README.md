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

## Run

```bash
docker compose up -d                                    # core services
docker compose --profile dbt run --rm duckdb build      # build the lakehouse
docker compose --profile analytics up -d trino superset  # query and dashboards
```
