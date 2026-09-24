# PostgreSQL runtime image standard

Every repository-owned PostgreSQL runtime definition uses the single approved image `postgres:16.15-alpine`. The verified Docker Official Image tag is `16.15-alpine`; no floating or major-only tag is permitted.

| Service | Repository runtime definition | Image |
| --- | --- | --- |
| platform `postgres` | `platform/docker/dockerfiles/Dockerfile.postgresql` | `postgres:16.15-alpine` |
| `postgres-healthcheck` | `platform/docker/dockerfiles/Dockerfile.postgres-healthcheck` | `postgres:16.15-alpine` |
| `pdm-source-postgres` | `docker-compose.yaml` | `postgres:16.15-alpine` |

The architecture remains two separate PostgreSQL instances. The platform `postgres` service holds infrastructure and control databases and is not a CDC source. The optional `pdm-source-postgres` service holds the PDM/MDM operational source and is the only CDC source; it starts only under the `cdc` profile. Services, databases, credentials, and volumes stay separate: `pgdata` for the platform instance, `pdm-source-pgdata` for the PDM source. Only `pdm-source-postgres` carries logical WAL configuration (`wal_level=logical`, replication slots, publication `pdm_mdm_ordinary_pub`). The version contract test `tests/mdm/test_pdm_source_postgres_contract.py::test_postgresql_version_standard_and_instance_isolation` proves each of these properties.

## Major-version data directories are not interchangeable

A PostgreSQL data directory belongs to the major version that created it. PostgreSQL does not start against a data directory from an earlier major version, and changing a repository image definition migrates no data. Any move between major versions therefore requires either an explicit upgrade path (`pg_upgrade`) or a rebuild of the data from its sources; in-place reuse of an older data directory is never valid.

## This development environment is reinitialised, not migrated

The local Docker runtime is disposable. Once repository hardening is complete, this development environment is reinitialised from scratch: existing local containers, volumes, and generated data are discarded and the platform PostgreSQL service is initialised directly on `postgres:16.15-alpine`. No local PostgreSQL 15 data directory is carried forward and no local major-version migration is planned, designed, or required.

Preserving or migrating any existing local `pgdata` is explicitly **not** a prerequisite for that clean rebuild, and the rebuild must not depend on the previous local data directory in any way. Until the clean reinitialisation is performed, do not start the platform `postgres` service against an older local data directory; do not investigate, mount, inspect, dump, restore, upgrade, or delete that volume as part of this work.

## Operator guidance for the clean rebuild

Reinitialisation is a deliberate, separate operation performed only when hardening is complete:

- discard the previous local containers, volumes, and generated data
- initialise platform `postgres` fresh on `postgres:16.15-alpine`
- re-create the control databases and re-provision all identities from externally supplied secrets
- re-run initialization, provisioning, and bootstrap steps, then revalidate the affected contracts

The PDM source instance is unaffected because it was created directly on `postgres:16.15-alpine` and has never held a PostgreSQL 15 data directory.
