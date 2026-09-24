# Optional local management UIs

The `pgadmin` and `kafka-ui` profiles are local development and observability tools. Neither is part of the production data path. Both listen only on `127.0.0.1` on the Docker host.

Set these variables in your ignored `.env` file or shell before enabling a profile. Do not commit their values:

| Profile | Required variables | Default URL |
|---|---|---|
| `pgadmin` | `PGADMIN_DEFAULT_EMAIL`, `PGADMIN_DEFAULT_PASSWORD` | `http://127.0.0.1:5050` |
| `kafka-ui` | `KAFKA_UI_USERNAME`, `KAFKA_UI_PASSWORD` | `http://127.0.0.1:8086` |

The host ports can be changed with `PGADMIN_PORT` and `KAFKA_UI_PORT`.

Start a selected profile with `docker compose --profile pgadmin up -d pgadmin` or `docker compose --profile kafka-ui up -d kafka-ui` when local access is needed.

In pgAdmin, register two servers if both are needed:

| Server | Host and container port | Purpose | Credential variable names |
|---|---|---|---|
| Platform PostgreSQL | `postgres:5432` | Platform infrastructure | `POSTGRES_USER`, `POSTGRES_PASSWORD` |
| PDM source PostgreSQL | `pdm-source-postgres:5432` | PDM/MDM operational source | `PDM_SOURCE_USER`, `PDM_SOURCE_PASSWORD` |

Use the respective database names `POSTGRES_DB` and `PDM_SOURCE_DB`. The pgAdmin login is separate from either database login. No PostgreSQL credentials are injected into pgAdmin or preconfigured as saved servers. Registering the second server needs no pgAdmin Compose change. Prefer a suitable limited account for routine inspection.

Kafka UI connects to `kafka:9092` and `http://schema-registry:8081` on `data-platform-network`. It shows topics, partitions, offsets, messages, and Schema Registry subjects. When a governed Debezium connector eventually publishes CDC messages, those topics can be inspected. The UI does not activate CDC or connect to Kafka Connect, and receives no Debezium or PostgreSQL administrative credentials. Its cluster is configured read only, and its MCP and dynamic configuration endpoints are disabled. UI login alone does not enforce per-topic authorization; restricted beneficiary CDC must not be exposed through this UI merely because it is bound to localhost. The first CDC demonstration is limited to ordinary SACCO source data.

Unset UI login variables render as empty strings in base Compose configuration. Each UI's startup command checks for nonempty credentials and exits before serving if they are absent; no default password is provided.
