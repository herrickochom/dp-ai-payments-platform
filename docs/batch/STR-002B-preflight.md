# STR-002B preflight reconnaissance

No hard-stop condition was found. The repository has clear additive insertion points for Flink services and Kafka declarations, and the inventory collector already discovers new services without a service-specific code change. Full STR-002B delivery is not ready to start, however: the phased plan is still Draft, contract-dependent work awaits STR-002A, runtime facts U10/U11 require approved probes, and U12 requires approved non-functional requirements (`docs/streaming/phased-plan.md:98-113`; `docs/streaming/risk-register.md:30-32,43`).

## P1 — Compose insertion points

- `services:` spans lines 1–1613, followed by `networks:` at lines 1614–1617 and `volumes:` at lines 1618–1634 (`docker-compose.yaml:1,1614,1618`).
- Top-level services, in source order:

| Line | Service | Line | Service |
|---:|---|---:|---|
| 2 | `redis` | 28 | `postgres` |
| 55 | `postgres-healthcheck` | 74 | `pgadmin` |
| 98 | `pdm-source-postgres` | 139 | `minio` |
| 168 | `minio-init-buckets` | 195 | `minio-init-databases` |
| 222 | `kafka-volume-init` | 236 | `kafka` |
| 279 | `kafka-init` | 300 | `schema-registry` |
| 334 | `schema-registry-init` | 355 | `kafka-ui` |
| 385 | `nessie` | 441 | `nessie-rest-proxy` |
| 458 | `payment-xml-generator` | 481 | `payment-producer` |
| 517 | `payment-consumer-events` | 582 | `trino` |
| 656 | `pdm-ml-features` | 697 | `pdm-ml-scoring` |
| 737 | `agent-api` | 798 | `metabase` |
| 834 | `superset-init` | 905 | `superset` |
| 968 | `superset-worker` | 1022 | `transform-ledger-migrate` |
| 1051 | `transform-runtime` | 1098 | `platform-job-runner` |
| 1169 | `airflow-init` | 1230 | `airflow-apiserver` |
| 1288 | `airflow-scheduler` | 1344 | `airflow-dag-processor` |
| 1392 | `airflow-worker` | 1453 | `airflow-triggerer` |
| 1502 | `platform-transform-worker` | 1576 | `minio-init-iam` |

Source: `docker-compose.yaml:2-1613`.

- None of `flink`, `flink-jobmanager`, `flink-taskmanager`, `kafka-exporter`, `prometheus`, or `grafana` is a current service; the plan likewise records that these components do not yet exist (`docker-compose.yaml:2-1613`; `docs/streaming/phased-plan.md:100`).
- None of `flink-data`, `flink-checkpoints`, `prometheus-data`, or `grafana-data` is a current named volume (`docker-compose.yaml:1618-1634`).
- Profile syntax is a service-level sequence, for example `profiles:` followed by `- pgadmin` (`docker-compose.yaml:77-78`).

**Confidence: High.** These are direct structural observations from the complete Compose file.

## P2 — Kafka topic manifest

- `defaults:` is lines 5–7 and supplies `cleanup_policy: delete`; `topics:` starts at line 8 (`platform/kafka/topics.yaml:5-8`).
- A complete entry is:

```yaml
12  - name: icmn.vpm.pain001
13    partitions: 6
14    replication: 1
15    retention_ms: 604800000   # 7 days
16    description: "VPM Business Payment Initiation"
```

Source: `platform/kafka/topics.yaml:12-16`.

- There are 33 declared topics. No entry overrides `cleanup_policy`; all inherit the default (`platform/kafka/topics.yaml:5-8,12-218`).
- The file presents itself as the platform topic configuration, and `topic_admin.py` reads and reconciles it rather than generating it; it is therefore hand-maintained repository configuration (`platform/kafka/topics.yaml:1-8`; `platform/kafka/topic_admin.py:23-36,87-99`).

**Confidence: High.** The count and shape were checked across the complete manifest and its consumer.

## P3 — Compose inventory collector

- The collector does not parse YAML or invoke Compose. It scans text, enters the top-level `services` section, and treats keys at exactly two-space indentation as service names; this deliberately avoids materialising environment values (`programme/tooling/programme_control/collectors/compose_inventory.py:1-7,55-78`).
- For every present Compose file it stores a sorted service list, count and file digest as evidence; the main file is authoritative and its non-infrastructure services become the capability inventory (`programme/tooling/programme_control/collectors/compose_inventory.py:86-128`).
- A new service is picked up automatically. A code change is needed only if classification as an infrastructure helper is desired; otherwise it is counted as a capability (`programme/tooling/programme_control/collectors/compose_inventory.py:31-52,113-124`).
- It reads all four files: `docker-compose.yaml`, `docker-compose.production-tls.yaml`, `docker-compose.transform-execution.yaml`, and `docker-compose.plugin-dev.yaml` (`programme/tooling/programme_control/collectors/compose_inventory.py:24-29,88-95`).
- This makes the file-plan's unconditional-looking collector modification unnecessary under current behaviour, while the phased plan correctly qualifies it with “if” (`docs/streaming/file-plan.md:46`; `docs/streaming/phased-plan.md:110,122`).

**Confidence: High.** The collector is small, static, and was read in full.

## P4 — Dockerfile conventions

`Dockerfile.superset`, in full:

```dockerfile
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
```

Source: `platform/docker/dockerfiles/Dockerfile.superset:1-43`.

`Dockerfile.kafka-broker`, in full:

```dockerfile
FROM apache/kafka:4.3.1

# Helper script used by other services / health checks
COPY --chmod=755 platform/kafka/wait-for-kafka.sh /wait-for-kafka.sh

ENV KAFKA_HEAP_OPTS="-Xmx512M"

EXPOSE 9092 9093

# apache/kafka supplies its own entrypoint/CMD
```

Source: `platform/docker/dockerfiles/Dockerfile.kafka-broker:1-10`.

| File | Pinning | Runtime user | Entrypoint/command |
|---|---|---|---|
| Superset | Literal tag `6.1.0` | Temporarily root for build steps, then explicitly `superset` | None declared; inherited/supplied externally |
| Kafka broker | Literal tag `4.3.1` | No `USER`; inherited from base image | Explicitly inherited from base image |

Sources: `platform/docker/dockerfiles/Dockerfile.superset:7-9,41-43`; `platform/docker/dockerfiles/Dockerfile.kafka-broker:1-10`.

- Multi-stage builds exist in `Dockerfile.platform-job-runner` and `Dockerfile.minio-init-iam` (`platform/docker/dockerfiles/Dockerfile.platform-job-runner:1-3,31-32`; `platform/docker/dockerfiles/Dockerfile.minio-init-iam:3-7`).
- Build-context-relative repository sources are copied to absolute container paths, for example `platform/superset/...` to `/app/...` and `platform/kafka/...` to `/wait-for-kafka.sh` (`platform/docker/dockerfiles/Dockerfile.superset:17-28`; `platform/docker/dockerfiles/Dockerfile.kafka-broker:4`).

**Confidence: High.** Both requested files were read and reproduced in full, and all Dockerfiles were checked for stages.

## P5 — Schema-registration surface

`configure-schema-registry.sh`, in full:

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

Source: `platform/kafka/configure-schema-registry.sh:1-16`.

- It sets global Schema Registry compatibility; it does not register a subject (`platform/kafka/configure-schema-registry.sh:4-16`).
- No other repository script explicitly registers Avro or JSON Schema subjects. Production producer code constructs an `AvroSerializer`; explicit auto-registration appears only in a live test (`services/payment-producer/kafka_producer.py:700-735`; `tests/live_red_invariant_test.py:126-127`).
- Avro files exist at `contracts/kafka/payment_event.avsc`, `contracts/kafka/technical_payment_event.avsc`, `platform/kafka/payment_event.avsc`, and `platform/kafka/technical_payment_event.avsc`; no JSON schema file exists in those two directories.
- The repository does not pin a subject-name strategy in configuration or code; this is also recorded by discovery (`docs/discovery/STR-001-compose-recon.md:303-305`).

**Confidence: High.** Repository-wide searches covered registration terms, serializer settings, and both requested schema directories.

## P6 — Profiles and environment conventions

| Profile value | Declared/activated |
|---|---|
| `pgadmin` | Declared at `docker-compose.yaml:77-78`; CLI activation at `docs/local-management-uis.md:14`. |
| `cdc` | Declared at `docker-compose.yaml:101-102`; no activation example found. |
| `kafka-ui` | Declared at `docker-compose.yaml:358-359`; CLI activation at `docs/local-management-uis.md:14`. |
| `analytics` | Declared at `docker-compose.yaml:446-448,590-596,742-744,801-804,839-842,910-913,973-976`; active in local `COMPOSE_PROFILES` and README CLI example (`.env:153`; `README.md:171`). |
| `duckdb` | Declared at `docker-compose.yaml:446-448`; no activation example found. |
| `generate-data` | Declared at `docker-compose.yaml:459-460`; no activation example found. |
| `agents` | Declared at `docker-compose.yaml:590-596,742-744`; CLI activation at `README.md:206`. |
| `trino`, `dbt` | Declared at `docker-compose.yaml:590-596`; no activation example found. |
| `dashboard`, `superset` | Declared on analytics services; both active in local `COMPOSE_PROFILES` (`docker-compose.yaml:590-596,801-804,839-842,910-913,973-976`; `.env:153`). |
| `ml`, `ai` | Declared at `docker-compose.yaml:661-663,702-704`; no activation example found. |
| `metabase` | Declared at `docker-compose.yaml:801-804`; no activation example found. |
| `airflow` | Declared at `docker-compose.yaml:1023-1504`; no activation example found. |

The three overlay files contain no `profiles:` declaration; the complete profile-value set above comes from the main file.

- `.env` contains `COMPOSE_PROFILES=analytics,superset,dashboard` (`.env:153`).
- `streaming` and `observability` appear in planning documents as future profile names, but neither is a current Compose service or profile (`docs/streaming/phased-plan.md:109,125,296`; `docker-compose.yaml:1-1634`).
- `.env` groups variables under ordered `#` section headings (secret delivery, PostgreSQL, Airflow, object store, layout, object-store identities, Nessie, Trino, transform runtime, dbt, Superset, BI identities, pgAdmin, Kafka UI and CDC), then places profile activation at the end (`.env:1-153`).
- No variable name begins `FLINK_`, `PROMETHEUS_`, `GRAFANA_`, or `KAFKA_EXPORTER_` (`.env:1-153`).

**Confidence: High.** All four Compose files and environment variable names were checked; “no activation example” is limited to repository evidence.

## P7 — Platform layout

| Directory | Repository-evidenced purpose |
|---|---|
| `platform/cdc/` | CDC contracts and Debezium material (`platform/cdc/contracts/`; `platform/cdc/debezium/`). |
| `platform/config/` | Cross-platform governance and MinIO configuration (`platform/config/governance/`; `platform/config/minio/`). |
| `platform/docker/` | Dockerfiles and MinIO image material (`platform/docker/dockerfiles/`; `platform/docker/minio/`). |
| `platform/entrypoints/` | Shared entrypoint scripts (`platform/entrypoints/kafka-start.sh`). |
| `platform/kafka/` | Topic administration, schemas and Kafka helper scripts (file list below). |
| `platform/mdm/` | MDM contracts (`platform/mdm/contracts/`). |
| `platform/metabase/` | Metabase plugins (`platform/metabase/plugins/`). |
| `platform/minio/` | Object-store provisioning, policies and initialization (`platform/minio/policies/`; `platform/minio/provision_object_store_identities.py`). |
| `platform/nessie/` | Nessie configuration and REST proxy (`platform/nessie/config/`; `platform/nessie/iceberg_rest_proxy.py`). |
| `platform/pdm-source-postgres/` | Source database entrypoint and initialization (`platform/pdm-source-postgres/initdb/`; `platform/pdm-source-postgres/pdm-source-entrypoint.sh`). |
| `platform/postgres/` | Platform database initialization and health checks (`platform/postgres/initdb/`; `platform/postgres/healthchecks/`). |
| `platform/source_registry/` | Source-system contracts (`platform/source_registry/contracts/`). |
| `platform/superset/` | Superset configuration, provisioning and dashboard assets (`platform/superset/superset_config.py`; `platform/superset/dashboards/`). |
| `platform/trino/` | Trino catalogs and runtime configuration (`platform/trino/catalog/`; `platform/trino/etc/`). |

Files under `platform/kafka/`: `configure-schema-registry.sh`, `consumer-lag.sh`, `create-topics.sh`, `payment_event.avsc`, `technical_payment_event.avsc`, `topic_admin.py`, `topics.yaml`, `wait-for-kafka.sh`.

Files under `platform/docker/dockerfiles/`: `Dockerfile.agent-api`, `Dockerfile.airflow`, `Dockerfile.dbt`, `Dockerfile.dbt-docs`, `Dockerfile.duckdb`, `Dockerfile.kafka-broker`, `Dockerfile.kafka-init`, `Dockerfile.minio-init-iam`, `Dockerfile.minio-init-mc-buckets`, `Dockerfile.minio-init-mc-databases`, `Dockerfile.nessie`, `Dockerfile.nessie-rest-proxy`, `Dockerfile.payment-consumer-events`, `Dockerfile.payment-producer`, `Dockerfile.payment-xml-generator`, `Dockerfile.pdm-ml`, `Dockerfile.platform-job-runner`, `Dockerfile.postgres-healthcheck`, `Dockerfile.postgresql`, `Dockerfile.superset`, `Dockerfile.trino-coordinator`.

- `platform/flink/`, `platform/prometheus/`, and `platform/grafana/` do not exist; the plan identifies the substrate as absent (`docs/streaming/phased-plan.md:100,113`).
- Existing layouts use a service directory beneath `platform/`, with service-specific subdirectories where useful—for example `platform/trino/etc/`, `platform/trino/catalog/`, `platform/superset/`, and `platform/nessie/config/`.

**Confidence: High.** The complete depth-one directory and requested file inventories were read directly.

## P8 — Frozen-set boundaries

| Frozen item | Existence/result |
|---|---|
| `platform/trino/etc/rules.json` | Exists. |
| Trino access/authentication files under `platform/trino/etc/` | `access-control.properties`, password database, keystore and other configuration exist. |
| `platform/nessie/`, `platform/superset/` | Both exist. |
| `orchestration/transform_runtime/migrations/` | Exists. |
| `platform/pdm-source-postgres/initdb/` | Exists. |
| Existing `platform/kafka/topics.yaml` entries | File exists; entries are frozen while additions remain allowed. |
| `.env`, `.env.*`, `.secrets/`, mounted values | `.env` exists; protected values were not inspected. |
| Existing volumes/data, Kafka offsets, DLQ records, historical data | Runtime state, not repository paths; existence cannot be established statically. |
| `docs/discovery/STR-001-compose-recon.md` | Exists. |

The authoritative list and intent are at `docs/streaming/file-plan.md:116-131`.

- The STR-002B work can avoid frozen paths: new topic entries are extensions, new Flink files/services/volume are additive, environment variable additions are expressly allowed, and existing topic definitions, offsets, volumes and `KAFKA_TOPICS` remain unchanged (`docs/streaming/phased-plan.md:105-134,366-375`). Therefore no proposed change necessarily touches a frozen path.
- Ambiguity 1: the plan requests a Flink scrape placeholder in a not-yet-existing Prometheus configuration, while the file plan assigns creation ownership of `platform/prometheus/` to STR-008 (`docs/streaming/phased-plan.md:113`; `docs/streaming/file-plan.md:73,95-100`).
- Ambiguity 2: the file plan marks the inventory collector for modification, but current code automatically inventories new service keys and the phased plan says “if applicable” (`docs/streaming/file-plan.md:46`; `programme/tooling/programme_control/collectors/compose_inventory.py:55-78,88-124`; `docs/streaming/phased-plan.md:110,122`).
- Ambiguity 3: STR-002B is parallelized with STR-002A yet its topics, subjects, canonical-keyed job and “confirmed contract” depend on STR-002A output (`docs/streaming/phased-plan.md:98-113`).

**Confidence: Medium.** Repository paths and wording are verified, but runtime-state items intentionally remain unprobed.

## P9 — Inherited unknowns

| Unknown | Repository resolution | Required resolution |
|---|---|---|
| U10: runtime Compose state, CLI profiles, volume initialization, logging driver | Not repository-resolvable. Declared profiles are known, but active runtime state is not (`docs/streaming/risk-register.md:30`). | Approved read-only runtime inspection of running services, active profiles, volume initialization and logging-driver state. |
| U11: default metrics endpoints and Schema Registry runtime state | Not fully repository-resolvable. The repository has no configured Prometheus target, and the script only sets global compatibility; actual endpoints, subjects and compatibility state remain runtime facts (`docs/streaming/risk-register.md:31,37`; `platform/kafka/configure-schema-registry.sh:4-15`). | Authenticated read-only metrics probes and Schema Registry `/subjects` and `/config` inventory, plus dependency-version confirmation (`docs/discovery/STR-001-compose-recon.md:671`). |
| U12: sizing, cost, TLS/authentication, availability, data protection | Not repository-resolvable; these approved production requirements are absent (`docs/streaming/risk-register.md:32`). | Platform-operations and security/data-protection owner conversation producing approved NFRs and capacity assumptions. |

**Confidence: High.** The classification follows the risk register's explicit resolving actions and separates static declarations from runtime and owner decisions.

## Readiness verdict

**Not ready for full STR-002B changes.** Contract-independent reconnaissance is complete, but implementation should not begin while the phased plan remains Draft; contract-dependent topics, schemas and J1 await STR-002A; U10/U11 require approved runtime probes; and U12 requires owner-approved NFRs (`docs/streaming/risk-register.md:30-32,43`; `docs/streaming/phased-plan.md:100-113`). No frozen-path or collector hard stop was found.

## Known unknowns

- Active runtime services/profiles, existing volume initialization and logging driver remain unknown pending U10's read-only probe (`docs/streaming/risk-register.md:30`).
- Runtime Schema Registry subjects/compatibility and actual metrics endpoints remain unknown pending U11's authenticated probes (`docs/streaming/risk-register.md:31`).
- Production capacity, retention cost, security, availability and data-protection requirements remain unknown pending U12's owner conversation (`docs/streaming/risk-register.md:32`).
- Exact lifecycle topic/subject names and canonical key remain dependent on STR-002A (`docs/streaming/phased-plan.md:100-113`; `docs/streaming/risk-register.md:22-24`).
- Ownership/timing of the pre-STR-008 Prometheus placeholder remains ambiguous (`docs/streaming/phased-plan.md:113`; `docs/streaming/file-plan.md:73`).

## Proposed first change

After phased-plan approval and STR-002A confirmation, the smallest safe first change is to **append only the approved lifecycle input-topic declaration to `platform/kafka/topics.yaml`, without altering any existing entry**. This is the narrowest additive change, respects the frozen set, and is prerequisite infrastructure because Kafka auto-creation is disabled (`docs/streaming/phased-plan.md:105,131-134,366-375`; `docs/streaming/risk-register.md:33`; `docker-compose.yaml:254-256`).
