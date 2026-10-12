# STR-006 Pre-Approval Postgres Provisioning Evidence

- Status: Complete
- Date: 2026-10-12
- Context: Postgres provisioning executed ahead of STR-006 by explicit
  owner approval (see consolidation directive §9 and Kafka→Flink→Kafka
  directive §5). STR-006 still designs the tables, projection writer,
  and formal migration mechanism.

## What was created

- Database: `payments_operations` (owner: `payments_ops_migration`,
  template0)
- Schema: `payment_lifecycle` (owner: `payments_ops_migration`)
- Roles:
  - `payments_ops_migration` — LOGIN; schema and object owner
  - `payments_ops_writer` — LOGIN; writes projections
  - `payments_ops_reader` — NOLOGIN; read-only privilege holder
  - `grafana_payments_ro` — LOGIN; Grafana Postgres datasource
- No tables created. Table design is STR-006 work.

## Source of truth

- `platform/postgres/migrations/001_create_payments_operations.sql`

## Credentials

- `PAYMENTS_OPS_MIGRATION_PASSWORD` — `.env`, gitignored
- `PAYMENTS_OPS_WRITER_PASSWORD` — `.env`, gitignored
- `PAYMENTS_OPS_READER_PASSWORD` — `.env`, gitignored
- `GRAFANA_PAYMENTS_RO_PASSWORD` — `.env`, gitignored

## Verification

All checks passed on 2026-10-12:

1. Database exists.
2. Four roles exist with correct login status.
3. Schema exists, owned by `payments_ops_migration`.
4. `payments_ops_writer` can connect (`SELECT 1` → `1`).
5. `payments_ops_writer` cannot create tables
   (`CREATE TABLE` → `ERROR: permission denied for schema
   payment_lifecycle`).

## Notes for STR-006

- The `pgdata` volume is already initialised, so init scripts under
  `platform/postgres/initdb/` will not re-run. The migration mechanism
  for STR-006 must be a deliberate apply step, not an init script.
- The migration file is idempotent and safe to re-run, which makes it
  usable as the basis for STR-006's migration service.
- The passwords are currently in the local `.env`. They should be
  rotated before any non-development reuse.
