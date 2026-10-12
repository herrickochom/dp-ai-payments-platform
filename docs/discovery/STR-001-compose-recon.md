# STR-001 Docker Compose Reconnaissance

## Executive Summary

Five findings materially affect planning: `docker-compose.yaml` is the authoritative 37-service stack, with three opt-in overlay/fragment files and no Compose `include:` chain; the local default profiles are `analytics,superset,dashboard`; Kafka topic auto-creation is disabled and all 33 declared topics currently inherit `cleanup_policy: delete`, so no compacted topic exists; no Flink, Prometheus, Grafana, Kafka Exporter, or verified Prometheus-scrapable endpoint is configured; and the repository contains synthetic `TechnicalPaymentEvent` lifecycle telemetry with populated correlation fields, but no evidence that it represents real production telemetry. `.env` and `.secrets/` are ignored, protected workers use read-only mounted secrets, and no literal production secret was identified in tracked configuration during the static name-based scan (`docker-compose.yaml:1-1613`, `.env:153`, `platform/kafka/topics.yaml:5-227`, `.gitignore:83-85`, `.gitignore:130-131`, `services/payment-xml-generator/generator_common.py:364-478`).

## T1 — Compose file location and topology

- Compose definitions: `docker-compose.yaml`, `docker-compose.plugin-dev.yaml`, `docker-compose.production-tls.yaml`, and `docker-compose.transform-execution.yaml`; the repository inventory names the same four (`programme/tooling/programme_control/collectors/compose_inventory.py:24-29`).
- `docker-compose.yaml` is authoritative: the inventory explicitly derives its authoritative service list from it (`programme/tooling/programme_control/collectors/compose_inventory.py:113-115`).
- `docker-compose.production-tls.yaml` overlays Kafka and Schema Registry TLS settings (`docker-compose.production-tls.yaml:1-33`). `docker-compose.transform-execution.yaml` overlays two transform services and explicitly extends `platform-job-runner` from the main file (`docker-compose.transform-execution.yaml:1-30`). `docker-compose.plugin-dev.yaml` overlays three services and declares two development volumes (`docker-compose.plugin-dev.yaml:1-16`).
- No `include:` key exists. A repository script combines the main and transform files using repeated `-f` arguments (`scripts/audit_trino_migration.sh:163-166`).
- No `services.yaml`/`services.yml` exists in the repository; therefore no supplied `services.yaml` can be classified as canonical, fragment, or copy from repository evidence.
- Top-level keys: main file has `services`, `networks`, `volumes` (`docker-compose.yaml:1`, `docker-compose.yaml:1614-1634`); plugin-dev has `services`, `volumes` (`docker-compose.plugin-dev.yaml:1-16`); the other overlays have only `services` (`docker-compose.production-tls.yaml:1`, `docker-compose.transform-execution.yaml:1`). Profiles are service-level keys, and no top-level `secrets`, `configs`, or `x-*` anchor was found.

**Confidence: High.** All YAML files with a top-level `services:` key were statically enumerated.

## T2 — Profiles and activation

| Profile | Services using it | Activation evidence |
|---|---|---|
| `analytics` | nessie-rest-proxy, trino, agent-api, metabase, Superset services | Active through local `COMPOSE_PROFILES` (`.env:153`); CLI example (`README.md:171`). |
| `superset` | trino and Superset services | Active through local `COMPOSE_PROFILES` (`.env:153`). |
| `dashboard` | trino, metabase, Superset services | Active through local `COMPOSE_PROFILES` (`.env:153`). |
| `agents` | trino, agent-api | CLI example (`README.md:206`). |
| `trino`, `dbt` | trino | No activation site found beyond Compose declarations (`docker-compose.yaml:590-596`). |
| `metabase` | metabase | No activation site found beyond declaration (`docker-compose.yaml:801-804`). |
| `airflow` | transform and Airflow services | Declarations at `docker-compose.yaml:1022-1100`, `docker-compose.yaml:1169-1504`; deployment contracts also use `--profile` (`infra/terraform/targets/local/contracts/deployment_command.json:38`). |
| `ml`, `ai` | ML feature/scoring jobs | `docker-compose.yaml:661-704`. |
| `cdc` | pdm-source-postgres | `docker-compose.yaml:98-102`. |
| `pgadmin`, `kafka-ui` | corresponding management UI | Optional CLI activation documented at `docs/local-management-uis.md:14`. |
| `duckdb` | nessie-rest-proxy | `docker-compose.yaml:446-448`. |
| `generate-data` | payment-xml-generator | `docker-compose.yaml:458-480`. |

- Active local defaults are `analytics,superset,dashboard` (`.env:153`). Services with no profile are also enabled by Compose normally.
- Neither `streaming` nor `observability` occurs as a profile or service name. Their only planning-language occurrences are in the ADR (`docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md:73-179`).

**Confidence: High.** Profile declarations and repository activation references were exhaustively searched; shell-local overrides outside the repository remain unknowable.

## T3 — Kafka topics declaration

- `kafka-init` receives `/app/topics.yaml` through `TOPICS_CONFIG` (`docker-compose.yaml:279-299`), sourced from `platform/kafka/topics.yaml` by its image convention.
- Auto-creation is disabled (`docker-compose.yaml:254-256`).
- All topics inherit `cleanup_policy: delete`; none declares `compact` (`platform/kafka/topics.yaml:5-8`).
- Every topic has replication factor 1. Partition/retention details are in the complete declaration below.

```yaml
# ============================================================================
# Kafka Topics Configuration - PDM Platform
# ============================================================================

defaults:
  cleanup_policy: delete

topics:
  # ==========================================================================
  # 1. ICMN Topics (Interbank Customer Messaging Network)
  # ==========================================================================
  - name: icmn.vpm.pain001
    partitions: 6
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "VPM Business Payment Initiation"

  - name: icmn.pmn.pain001
    partitions: 6
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "PMN standalone TechnicalPaymentEvent (legacy topic name retained)"

  # ==========================================================================
  # 2. CPO Topics (Cloud Payment Orchestrator)
  # ==========================================================================
  - name: cpo.psn.pain002
    partitions: 6
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "PSN Business Status"

  - name: cpo.plm.pain002
    partitions: 6
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "PLM standalone TechnicalPaymentEvent (legacy topic name retained)"

  # ==========================================================================
  # 3. Wendi Topics (PostBank Digital Wallet)
  # ==========================================================================
  - name: wendi.camt053
    partitions: 4
    replication: 1
    retention_ms: 2592000000  # 30 days
    description: "End-of-Day Bank Statements"

  - name: wendi.camt052
    partitions: 4
    replication: 1
    retention_ms: 86400000    # 1 day
    description: "Intraday Account Reports"

  - name: wendi.camt054
    partitions: 8
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Transaction Notifications"

  - name: wendi.transactions
    partitions: 8
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Wendi Platform Transactions"

  - name: wendi.pain001
    partitions: 6
    replication: 1
    retention_ms: 604800000
    description: "Wendi Business Payment Initiations"

  - name: wendi.pain002
    partitions: 6
    replication: 1
    retention_ms: 604800000
    description: "Wendi Payment Status Reports"

  # ==========================================================================
  # 4. Mobile Networks Topics
  # ==========================================================================
  - name: mobile.mtn.pacs008
    partitions: 6
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "MTN Interbank Settlement"

  - name: mobile.mtn.pacs002
    partitions: 4
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "MTN Interbank Status"

  - name: mobile.airtel.pacs008
    partitions: 6
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Airtel Interbank Settlement"

  - name: mobile.airtel.pacs002
    partitions: 4
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Airtel Interbank Status"

  # ==========================================================================
  # 5. Agent Network Topics
  # ==========================================================================
  - name: agent.transactions
    partitions: 8
    replication: 1
    retention_ms: 2592000000  # 30 days
    description: "Agent Cash-out Transactions"

  - name: agent.profiles
    partitions: 4
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Agent Profiles"

  - name: agent.locations
    partitions: 4
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Agent Locations"

  # ==========================================================================
  # 6. PDMIS Topics (Government System)
  # ==========================================================================
  - name: pdmis.beneficiaries
    partitions: 8
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Beneficiary Data"

  - name: pdmis.loans
    partitions: 8
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Loan Data"

  - name: pdmis.repayments
    partitions: 8
    replication: 1
    retention_ms: 604800000
    description: "Repayment Event Data"

  - name: pdmis.saccos
    partitions: 4
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "SACCO Data"

  - name: pdmis.households
    partitions: 4
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Household Data"

  - name: pdmis.business_plans
    partitions: 4
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Business Plans"

  - name: pdmis.special_groups
    partitions: 4
    replication: 1
    retention_ms: 604800000
    description: "Special Group Reference Data"

  # Configured ordinary CDC topic; connector and Raw subscription remain dormant.
  - name: cdc.pdm_mdm.mdm.sacco_master_sources
    partitions: 4
    replication: 1
    retention_ms: 604800000
    description: "PDM MDM SACCO source changes (Debezium JSON)"

  # ==========================================================================
  # 7. Shared operational failure topics
  # ==========================================================================
  - name: payment-events.retry
    partitions: 8
    replication: 1
    retention_ms: 604800000   # 7 days
    description: "Audit trail for bounded transient processing retries"

  - name: payment-events.dlq
    partitions: 8
    replication: 1
    retention_ms: 2592000000  # 30 days
    description: "Terminal failures awaiting deliberate operator replay"

# ------------------------------------------------------------------
# MDM master-data topics
# Source: platform/mdm/contracts/mdm_kafka_topics.json
# Restricted topics require dedicated restricted-identity ACLs.
# ------------------------------------------------------------------

  - name: mdm.beneficiary.golden.restricted
    partitions: 3
    replication: 1
    retention_ms: 604800000   # 7 days; local POC policy, production Terraform-managed

  - name: mdm.beneficiary.identity-alert.restricted
    partitions: 3
    replication: 1
    retention_ms: 604800000   # 7 days; local POC policy, production Terraform-managed

  - name: mdm.crosswalk.restricted
    partitions: 3
    replication: 1
    retention_ms: 604800000   # 7 days; local POC policy, production Terraform-managed

  - name: mdm.sacco.golden
    partitions: 3
    replication: 1
    retention_ms: 604800000   # 7 days; local POC policy, production Terraform-managed

  - name: mdm.agent.golden
    partitions: 3
    replication: 1
    retention_ms: 604800000   # 7 days; local POC policy, production Terraform-managed

  - name: mdm.geography.golden
    partitions: 3
    replication: 1
    retention_ms: 604800000   # 7 days; local POC policy, production Terraform-managed
```

The block above is a compact YAML-preserving dump of `platform/kafka/topics.yaml:5-227`; comments were omitted but no declared property was omitted.

- `payment-consumer-events` list: `icmn.vpm.pain001,icmn.pmn.pain001,cpo.psn.pain002,cpo.plm.pain002,wendi.pain001,wendi.pain002,wendi.camt053,wendi.camt052,wendi.camt054,wendi.transactions,mobile.mtn.pacs008,mobile.mtn.pacs002,mobile.airtel.pacs008,mobile.airtel.pacs002,agent.transactions,agent.profiles,agent.locations,pdmis.beneficiaries,pdmis.loans,pdmis.repayments,pdmis.saccos,pdmis.households,pdmis.business_plans,pdmis.special_groups` (`docker-compose.yaml:557`).
- `platform-job-runner` list is identical (`docker-compose.yaml:1134`). Both are declared-topic subsets: they omit the dormant CDC topic, retry/DLQ, and MDM topics.

**Confidence: High.** The declaration and both runtime lists were compared directly.

## T4 — Schema Registry configuration

```sh
#!/bin/sh
set -eu
registry_url="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"
compatibility="${SCHEMA_COMPATIBILITY:-BACKWARD_TRANSITIVE}"
if [ -n "${SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO:-}" ]; then
  auth_header="Authorization: Basic $(printf '%s' "${SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO}" | base64)"
else
  auth_header="X-No-Authentication: local-development"
fi
curl --fail --silent --show-error -H "$auth_header" \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -X PUT "${registry_url}/config" \
  --data "{\"compatibility\":\"${compatibility}\"}"
echo "Schema Registry compatibility set to ${compatibility}"
```

- This is the complete `platform/kafka/configure-schema-registry.sh` (`platform/kafka/configure-schema-registry.sh:1-16`). It sets global compatibility with an idempotent HTTP `PUT`; it does **not** register subjects.
- Compose starts the script after Schema Registry is healthy and supplies `SCHEMA_COMPATIBILITY: BACKWARD_TRANSITIVE` (`docker-compose.yaml:331-354`).
- Producer code constructs `AvroSerializer` from schema text and Schema Registry client configuration (`services/payment-producer/kafka_producer.py:700-735`); the repository does not explicitly configure subject-name strategy or auto-registration behavior.
- Avro schemas live at `contracts/kafka/*.avsc`, `platform/kafka/*.avsc`, and `platform/cdc/contracts/cdc_raw_event.avsc`. JSON contracts live under `platform/*/contracts/` and `orchestration/transform_runtime/contracts/`.

**Confidence: High** for script behavior and schema locations; **Medium** for subject registration because library defaults are not pinned in repository configuration.

## T5 — Existing references to Flink / Prometheus / Grafana

- Exhaustive case-insensitive search for `flink`, `prometheus`, `grafana`, `jmx_exporter`, `kafka-exporter`, and `kafka_exporter` found only planning references in `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md:18-179`.
- No Flink, Prometheus, Grafana, or Kafka Exporter service/configuration exists in any Compose definition (`docker-compose.yaml:1-1613`; overlays: `docker-compose.plugin-dev.yaml:1-16`, `docker-compose.production-tls.yaml:1-33`, `docker-compose.transform-execution.yaml:1-30`).

**Confidence: High.** Search covered the repository except `.git/` and this report.

## T6 — Existing metrics exposure

- Kafka has heap configuration but no `KAFKA_OPTS`, `-javaagent`, JMX exporter, or sidecar in the base stack (`docker-compose.yaml:241-278`). The TLS overlay merely permits an externally supplied `KAFKA_OPTS` value (`docker-compose.production-tls.yaml:3-17`).
- Schema Registry config declares API listener/Kafka settings but no Prometheus/JMX exporter configuration (`docker-compose.yaml:300-330`). No repository evidence confirms an enabled Prometheus-format `/metrics` endpoint.
- Trino config contains HTTP/HTTPS and authentication settings but no metrics/exporter setting (`platform/trino/etc/config.properties:1-21`); Compose health checks `/v1/info`, not `/metrics` (`docker-compose.yaml:641-650`).
- No `postgres_exporter`, Redis exporter, MinIO scrape configuration, Nessie exporter, Superset exporter, Airflow Prometheus exporter, StatsD bridge, or scrape configuration was found.
- **Finding:** the stack has no repository-configured, verified Prometheus-scrapable endpoint. Product-default endpoints that might exist at runtime were not assumed.

**Confidence: High** for configured endpoints; **Medium** for runtime product defaults, which require a non-mutating runtime probe.

## T7 — Postgres init patterns

All scripts under `platform/postgres/initdb/` are reproduced below (`platform/docker/dockerfiles/Dockerfile.postgresql:6-10`).

```sh
# platform/postgres/initdb/20_create_superset_db.sh

#!/bin/bash

set -euo pipefail

echo "Creating Superset database if required..."

psql \
    -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "${POSTGRES_DB:-pdm_platform}" <<-'EOSQL'

SELECT 'CREATE DATABASE superset'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'superset'
)\gexec

EOSQL

echo "Superset database ready."

# platform/postgres/initdb/30_create_airflow_db.sh

#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_DB="${AIRFLOW_DB:-airflow}"

psql \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB:-pdm_platform}" \
  -v ON_ERROR_STOP=1 \
  -v airflow_db="${AIRFLOW_DB}" <<'EOSQL'
SELECT format('CREATE DATABASE %I', :'airflow_db')
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = :'airflow_db'
)\gexec
EOSQL

# platform/postgres/initdb/40_create_transform_ledger_roles.sh

#!/usr/bin/env bash
set -euo pipefail

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${TRANSFORM_LEDGER_APP_PASSWORD:?TRANSFORM_LEDGER_APP_PASSWORD is required}"
: "${TRANSFORM_LEDGER_MIGRATION_PASSWORD:?TRANSFORM_LEDGER_MIGRATION_PASSWORD is required}"

echo "Provisioning transform ledger PostgreSQL roles..."

psql \
  --username "${POSTGRES_USER}" \
  --dbname "${POSTGRES_DB}" \
  -v ON_ERROR_STOP=1 \
  -v app_password="${TRANSFORM_LEDGER_APP_PASSWORD}" \
  -v migration_password="${TRANSFORM_LEDGER_MIGRATION_PASSWORD}" <<'EOSQL'

SELECT format(
    'CREATE ROLE transform_migration LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
    :'migration_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'transform_migration'
)\gexec

SELECT format(
    'CREATE ROLE transform_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
    :'app_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'transform_app'
)\gexec

SELECT format(
    'ALTER ROLE transform_migration PASSWORD %L',
    :'migration_password'
)\gexec

SELECT format(
    'ALTER ROLE transform_app PASSWORD %L',
    :'app_password'
)\gexec

SELECT format(
    'GRANT CONNECT ON DATABASE %I TO transform_migration',
    current_database()
)\gexec

SELECT format(
    'GRANT CONNECT ON DATABASE %I TO transform_app',
    current_database()
)\gexec

GRANT USAGE, CREATE ON SCHEMA public TO transform_migration;
GRANT USAGE ON SCHEMA public TO transform_app;
REVOKE CREATE ON SCHEMA public FROM transform_app;

ALTER DEFAULT PRIVILEGES FOR ROLE transform_migration
    IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLES TO transform_app;

ALTER DEFAULT PRIVILEGES FOR ROLE transform_migration
    IN SCHEMA public
    GRANT USAGE, SELECT
    ON SEQUENCES TO transform_app;

EOSQL

echo "Transform ledger PostgreSQL roles ready."

# platform/postgres/initdb/init-multiple-db.sh

#!/bin/bash

set -euo pipefail

create_database() {
    local database="$1"

    echo "Creating database '$database'"

    psql \
        -v ON_ERROR_STOP=1 \
        --username "$POSTGRES_USER" \
        --dbname "${POSTGRES_DB:-postgres}" \
        --set database="$database" <<-'EOSQL'
            SELECT format('CREATE DATABASE %I', :'database')
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_database
                WHERE datname = :'database'
            )\gexec

            SELECT format(
                'GRANT ALL PRIVILEGES ON DATABASE %I TO %I',
                :'database',
                current_user
            )\gexec
EOSQL

    echo "Database '$database' ready"
}

if [ -n "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
    echo "Multiple database creation requested: ${POSTGRES_MULTIPLE_DATABASES}"

    IFS=',' read -ra DATABASES <<< "$POSTGRES_MULTIPLE_DATABASES"

    for db in "${DATABASES[@]}"; do
        db="$(echo "$db" | xargs)"

        if [ -n "$db" ]; then
            create_database "$db"
        fi
    done

    echo "Multiple databases created successfully"
fi
```

The complete scripts above contain variable references only; originals are `platform/postgres/initdb/20_create_superset_db.sh:1-21`, `30_create_airflow_db.sh:1-17`, `init-multiple-db.sh:1-46`, and `40_create_transform_ledger_roles.sh:1-70`.

- `POSTGRES_MULTIPLE_DATABASES: metabase` drives the generic comma-separated creator (`docker-compose.yaml:33-40`; `platform/postgres/initdb/init-multiple-db.sh:32-45`). Superset and Airflow also have dedicated idempotent creators.
- A future database would follow the existing generic variable or dedicated-script pattern; a schema would require a schema/role migration pattern. This is a convention finding, not an implementation proposal.
- Inference: `/docker-entrypoint-initdb.d/` scripts run during first initialization of the standard Postgres data directory; because `pgdata` is persistent, they do not automatically rerun for an already initialized volume (`platform/docker/dockerfiles/Dockerfile.postgresql:1-7`, `docker-compose.yaml:43-45`). Runtime volume state is unknown.
- `POSTGRES_USER` owns generic created databases. Transform uses separate `transform_migration` and `transform_app` roles with migration-vs-runtime privileges (`platform/postgres/initdb/40_create_transform_ledger_roles.sh:18-66`). No Grafana role/schema exists.

**Confidence: High** for scripts and privileges; **Medium** for current runtime initialization state.

## T8 — Dockerfile conventions

```dockerfile
# platform/docker/dockerfiles/Dockerfile.superset

# ==============================================================================
# Apache Superset 6.1.0 - PDM Executive Intelligence
# ==============================================================================
# Pinned release for reproducible builds. Avoid :latest in a controlled stack.
# ==============================================================================

FROM apache/superset:6.1.0

USER root

RUN . /app/.venv/bin/activate && \
    uv pip install --no-cache \
        psycopg2-binary \
        "trino[sqlalchemy]" \
        redis

COPY platform/superset/superset_config.py \
    /app/pythonpath/superset_config.py

COPY platform/superset/dashboards/ \
    /app/platform/superset/dashboards/

# F2: the shared secret provider is the platform's single secret-resolution
# contract. Provisioning resolves the publisher credential through it rather
# than reading the environment directly, so the same fail-closed semantics
# apply here as elsewhere in the platform. The whole package is copied because
# secret_provider imports runtime_security from within it.
COPY services/shared/security/ /app/services/shared/security/

RUN touch /app/services/__init__.py \
    && touch /app/services/shared/__init__.py \
    && touch /app/services/shared/security/__init__.py \
    && chown -R superset:superset \
    /app/pythonpath \
    /app/platform/superset \
    /app/services

ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py \
    PYTHONUNBUFFERED=1

USER superset

EXPOSE 8088

# platform/docker/dockerfiles/Dockerfile.kafka-broker

FROM apache/kafka:4.3.1

# Helper script used by other services / health checks
COPY --chmod=755 platform/kafka/wait-for-kafka.sh /wait-for-kafka.sh

ENV KAFKA_HEAP_OPTS="-Xmx512M"

EXPOSE 9092 9093

# apache/kafka supplies its own entrypoint/CMD
```

- Complete source: `platform/docker/dockerfiles/Dockerfile.superset:1-43`, `platform/docker/dockerfiles/Dockerfile.kafka-broker:1-10`.
- Base images use explicit tags, not digests, in these two Dockerfiles. Superset temporarily uses root then drops to named user `superset`; Kafka inherits the base image user and entrypoint/CMD.
- Neither is multi-stage and neither declares an entrypoint/CMD. Commands are supplied by Compose or inherited (`docker-compose.yaml:949-957`).
- `read_only`, `tmpfs`, `no-new-privileges`, and `cap_drop: ALL` are Compose-level hardening patterns used for transform services, not these two services (`docker-compose.yaml:1043-1049`, `docker-compose.yaml:1151-1158`, `docker-compose.yaml:1565-1572`).
- Planning convention input: explicit version pin, minimal additions, deliberate user, inherited or Compose-owned command, and Compose-level runtime hardening where compatible.

**Confidence: High.** Both requested Dockerfiles and relevant Compose hardening declarations were read completely.

## T9 — Secret handling

- `.env` and `.env.*` are ignored (`.gitignore:83-85`); `.secrets/` is ignored (`.gitignore:130-131`). The locally configured `PROTECTED_BOOTSTRAP_SECRET_DIR` is beneath `.secrets/` (`.env:150`, value intentionally omitted).
- Local `DP_SECRET_SOURCE` is `environment` and `DP_SECRET_DIR` is empty (`.env:8-9`). The provider supports `environment` and `mounted-files`; it reads one file per variable name for the latter and fails closed on invalid configuration (`services/shared/security/secret_provider.py:33-42`, `services/shared/security/secret_provider.py:69-127`).
- `platform-job-runner` and `platform-transform-worker` override this to `mounted-files`, use `/run/secrets/protected-bootstrap`, and mount `PROTECTED_BOOTSTRAP_SECRET_DIR` there read-only (`docker-compose.yaml:1104-1106`, `docker-compose.yaml:1145-1147`, `docker-compose.yaml:1514-1516`, `docker-compose.yaml:1558-1561`). The services run read-only with controlled tmpfs and dropped capabilities (`docker-compose.yaml:1151-1158`, `docker-compose.yaml:1565-1572`). Host ownership is not declared in Compose and is a known unknown.
- Required-variable guards use `${VARIABLE:?message}`. Examples: `OBJECT_STORE_BUCKET` (`docker-compose.yaml:181`), `TRINO_KEYSTORE_PASSWORD` (`docker-compose.yaml:606`), and `TRANSFORM_RUNTIME_RUNNER_TOKEN` (`docker-compose.yaml:1072`).
- Static tracked-file review found variable references, test fixtures, hashes, and placeholders but no identified literal production secret. This is not a substitute for a dedicated secret scanner.

**Confidence: High** for ignore/configuration behavior; **Medium** for absence of all committed secrets because detection was static and name-based.

## T10 — Volumes and name collisions

Main named volumes are `pgdata`, `pdm-source-pgdata`, `pgadmin-data`, `minio-data`, `nessie-data`, `kafka-data`, `kafka-metadata`, `redis-data`, `metabase-data`, `dbt-logs`, `superset-data`, `dev-logs`, `agent-audit-data`, `airflow-logs`, `airflow-config`, and `platform-job-audit-data` (`docker-compose.yaml:1618-1634`). Plugin-dev adds `pycache-worker` and `pycache-runner` (`docker-compose.plugin-dev.yaml:14-16`).

- With local `COMPOSE_PROJECT_NAME=dp-ai-payments-platform` (`.env:4`), Docker names these `dp-ai-payments-platform_<volume>`, unless a future explicit `name:` overrides that default.
- No collision exists with `flink-data`, `flink-checkpoints`, `prometheus-data`, or `grafana-data`.
- No named volume is declared `external: true` or given an explicit `name:`.

**Confidence: High.** All Compose top-level volume declarations were inspected.

## T11 — Network topology

- The main network key is `data-platform-network`, with explicit Docker name `${DOCKER_NETWORK:-data-platform-network}` and `bridge` driver (`docker-compose.yaml:1614-1617`). Local `DOCKER_NETWORK` resolves to `data-platform-network` (`.env:5`).
- Every service in the authoritative file joins that network (`docker-compose.yaml:1-1613`); the three overlays inherit/augment main services when merged.
- No `network_mode: host` or `network_mode: none` exists.
- A new service can declare the existing network without changing the network definition; whether it should do so is a later design decision.

**Confidence: High.** Static YAML parsing covered every service network and network mode.

## T12 — Health and dependency graph

| Service | Healthcheck | Dependencies |
|---|---|---|
| postgres | `pg_isready`; 5s/5s/20 | none (`docker-compose.yaml:28-54`) |
| kafka | TCP open on 9092; 5s/5s/30 | postgres-healthcheck completed; kafka-volume-init completed (`docker-compose.yaml:262-278`) |
| minio | `/minio/health/live`; 5s/5s/20 | postgres-healthcheck completed (`docker-compose.yaml:153-167`) |
| nessie | `/q/health/ready`; 5s/5s/30, 20s start | postgres-healthcheck completed; minio healthy; minio-init-iam completed (`docker-compose.yaml:423-440`) |
| trino | `/v1/info`; 30s/10s/3, 60s start | minio and nessie healthy (`docker-compose.yaml:641-655`) |
| superset | `/health`; 15s/5s/20, 45s start | superset-init completed; trino healthy (`docker-compose.yaml:944-967`) |

```text
postgres ─healthy→ postgres-healthcheck ─completed→ minio ─healthy→ minio-init-buckets
    │                                      │                         │
    │                                      └──────────────┐          └→ minio-init-iam
    └→ superset-init ← redis healthy ←─────┘              └→ nessie ←┘
kafka-volume-init ─completed→ kafka ─healthy→ kafka-init
                                  └→ schema-registry ─healthy→ schema-registry-init
minio + nessie ─healthy→ trino ─healthy→ superset-init ─completed→ superset
```

- One-shot services (`restart: no`): postgres-healthcheck, minio-init-buckets, minio-init-databases, kafka-volume-init, kafka-init, schema-registry-init, payment-xml-generator, payment-producer, ML feature/scoring jobs, superset-init, transform-ledger-migrate, airflow-init, and minio-init-iam (`docker-compose.yaml:55-73`, `docker-compose.yaml:168-235`, `docker-compose.yaml:279-354`, `docker-compose.yaml:458-516`, `docker-compose.yaml:656-736`, `docker-compose.yaml:834-904`, `docker-compose.yaml:1022-1050`, `docker-compose.yaml:1169-1210`, `docker-compose.yaml:1576-1613`). Other listed services are long-running or have no explicit restart policy.

**Confidence: High.** Health and dependency fields were parsed directly; the diagram intentionally shows the relevant backbone rather than every leaf consumer.

## T13 — Existing observability

- No Prometheus, Alertmanager, Loki, Tempo, OpenTelemetry collector, Elastic stack, or external monitoring agent service/configuration was found. The programme monitoring register is empty and awaiting population (`programme/registry/operations/monitoring.yaml:1-6`).
- No forbidden monitoring product name appears in repository content outside this requested cleanliness check.
- No Compose `logging:` driver or central log shipper exists. Superset explicitly sends Gunicorn access/error logs to stdout/stderr (`docker-compose.yaml:949-957`). Trino config sets `io.trino=INFO` without a file sink (`platform/trino/etc/log.properties:1`).
- Airflow persists logs in the shared `airflow-logs` volume (`docker-compose.yaml:1202`, `docker-compose.yaml:1263`, `docker-compose.yaml:1321`, `docker-compose.yaml:1377`, `docker-compose.yaml:1428`, `docker-compose.yaml:1486`). No central forwarding destination is configured.
- Agent API emits structured JSON through Python logging (`services/agent-api/observability.py:1-22`).

**Confidence: High** for repository configuration; external container-runtime log drivers are a known unknown.

## T14 — Payment lifecycle evidence

- Synthetic standalone lifecycle telemetry exists as `TechnicalPaymentEvent`, separate from ISO 20022 messages. PMN and PLM generators explicitly create it (`services/payment-xml-generator/icmn_generator.py:111-122`, `services/payment-xml-generator/cpo_generator.py:62-73`).
- Generated stages include payment routing, validation, mobile-money credit, and agent cash-out; statuses include completed, pending, delayed, failed, retrying, and timed out (`services/payment-xml-generator/generator_common.py:378-415`).
- The producer publishes PMN/PLM as technical-event topics (`services/payment-producer/kafka_producer.py:80-101`); the existing consumer ingests them to Raw alongside business events (`services/kafka-consumer-events/kafka_consumer_events.py:73-109`, `services/kafka-consumer-events/kafka_consumer_events.py:1409-1463`). No live lifecycle-state processor was found.
- Identifier evidence:
  - `InstrId`/`PaymentInstructionId`: populated from loan ID in VPM and technical events; referenced by PSN (`services/payment-xml-generator/icmn_generator.py:76-80`, `services/payment-xml-generator/cpo_generator.py:46-55`, `services/payment-xml-generator/generator_common.py:424-428`).
  - `EndToEndId`: populated across those generated stages (`services/payment-xml-generator/generator_common.py:425-428`).
  - `UETR`: deterministically populated for generated VPM/PSN/PMN/PLM (`services/payment-xml-generator/icmn_generator.py:78-80`, `services/payment-xml-generator/cpo_generator.py:52-55`, `services/payment-xml-generator/generator_common.py:428`).
  - `CorrelationId`: populated in technical events only (`services/payment-xml-generator/generator_common.py:418-430`).
  - `transaction_id`: nullable in the Avro contract but populated by the synthetic generator (`contracts/kafka/technical_payment_event.avsc:4-8`, `services/payment-xml-generator/generator_common.py:427`).
- **Finding:** repository lifecycle telemetry is demonstrably synthetic; no evidence proves real operational producers emit it.

**Confidence: High** for synthetic generation and fields; **Low** for production applicability because the repository contains no live-source evidence.

## T15 — Repository layout for new config

- Top-level platform areas are `cdc`, `config`, `docker`, `entrypoints`, `kafka`, `mdm`, `metabase`, `minio`, `nessie`, `pdm-source-postgres`, `postgres`, `source_registry`, `superset`, and `trino`. Second-level conventions include `platform/docker/dockerfiles/`, `platform/kafka/`, `platform/trino/etc/`, `platform/superset/`, `platform/postgres/initdb/`, and `platform/*/contracts/`.
- Natural planning locations, following the existing service-specific pattern, are `platform/flink/`, `platform/prometheus/`, and `platform/grafana/`, with any custom images under `platform/docker/dockerfiles/`. These are proposed paths only; none was created.
- `platform/trino/etc/rules.json` is Trino file-based access control, not payment rules (`platform/trino/etc/access-control.properties:1-2`, `platform/trino/etc/rules.json:1-340`). It is outside the proposed configuration locations.

**Confidence: High.** Directory structure was enumerated to two levels; proposed paths are explicitly an inference from current conventions.

## Known unknowns

- Whether a separate `services.yaml` was supplied outside the repository. Resolve by providing its path or content and comparing its digest/service list.
- Current Docker state, active command-line profiles, initialized volume state, and external Docker logging driver. Resolve with approved read-only `docker compose ps`, `docker inspect`, and volume metadata inspection.
- Runtime availability and format of product-default metrics endpoints, especially Schema Registry and MinIO. Resolve with authenticated, read-only endpoint probes and content-type checks.
- Schema Registry subject inventory and whether producer-library defaults auto-register subjects in the deployed version. Resolve through read-only `/subjects` and `/config` queries plus dependency-version confirmation.
- Host ownership/mode of `PROTECTED_BOOTSTRAP_SECRET_DIR`. Resolve with metadata-only `stat`; do not read secret files.
- Whether any literal secret escaped the name-based tracked-file scan. Resolve with an approved secret scanner that reports only path, line, rule, and variable name with values redacted.
- Real lifecycle telemetry source, volume, ordering, identifier uniqueness/stability, and production schema conformance. Resolve with source-owner contracts and redacted production samples.
- Production resource sizing, retention, authentication, TLS, availability, and data-protection requirements for the planned services. Resolve during the next planning workshop with platform/security/operations owners.

## Frozen set

The later implementation plan must not modify these existing routes or ownership boundaries:

- Lakehouse services and state: MinIO, Nessie, Trino, Superset, Raw ingestion, Iceberg/dbt models, their volumes, and their credentials. STR-001 treats the lakehouse as a separate concern (`docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md:10-20`, `docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md:114-122`).
- `platform/trino/etc/rules.json` and related Trino ACL/authentication files: they govern analytical access, not streaming business logic (`platform/trino/etc/access-control.properties:1-2`).
- Existing service-owned PostgreSQL databases/schemas and tables: PostgreSQL streaming projections must not overwrite service-owned tables (`docs/adr/ADR-STR-001-003-lambda-kappa-streaming.md:99-112`).
- Existing Kafka offsets, named volumes, retry/DLQ records, and historical data: discovery provides no authorization for mutation.
- Existing Kafka topic definitions and producer/consumer contracts until a separately reviewed streaming contract identifies additions; auto-create is disabled (`docker-compose.yaml:254-256`).
- Secret files and values under `.env`, `.secrets/`, and protected mounts; only variable names and metadata may be used (`.gitignore:83-85`, `.gitignore:130-131`).

## Proposed next actions

- Confirm the absent STR-001 source ticket and any external `services.yaml` before planning.
- Validate product-default metrics endpoints and current container/volume state through an approved read-only runtime session.
- Obtain the real lifecycle telemetry contract and canonical correlation-key evidence from source owners.
- Agree the planning inputs: profile ownership, dependency/health contracts, topic/schema governance, persistence/retention, security boundaries, resource budgets, and recovery acceptance criteria.
- Run a dedicated redacted secret scan before any implementation branch is opened.
