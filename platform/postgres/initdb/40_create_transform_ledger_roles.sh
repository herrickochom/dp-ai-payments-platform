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
