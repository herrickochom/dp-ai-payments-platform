-- Streaming-route Postgres setup: create the payments_operations database,
-- the payment_lifecycle schema, and the four least-privilege streaming roles.
-- Idempotent and safe to re-run; creates nothing that already exists.
-- This file must never contain a literal credential. Passwords are supplied at
-- apply time via psql variables (:migration_password, :writer_password,
-- :reader_password, :grafana_password) and are quoted with %L by format().
-- Database: payments_operations (owner: payments_ops_migration, template0)
-- Schema:    payment_lifecycle (owner: payments_ops_migration)
-- Roles:
--   payments_ops_migration — LOGIN, NOSUPERUSER, NOCREATEDB, NOCREATEROLE.
--     Owns the schema and all objects in it.
--   payments_ops_writer   — LOGIN, NOSUPERUSER, NOCREATEDB, NOCREATEROLE.
--     Writes projections; no CREATE privilege on the schema.
--   payments_ops_reader   — NOLOGIN, NOSUPERUSER, NOCREATEDB, NOCREATEROLE.
--     Privilege-holder for read-only grants.
--   grafana_payments_ro   — LOGIN, NOSUPERUSER, NOCREATEDB, NOCREATEROLE.
--     Read-only; consumed by Grafana's Postgres datasource.
-- No tables are created here; table creation is STR-006 work.
-- Applies against the platform Postgres server only (no new server).

-- 1. Create the payments_operations database if it does not exist.
--    Template template0 avoids inheriting any encoding or locale quirks.
SELECT 'CREATE DATABASE payments_operations'
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = 'payments_operations'
) \gexec

-- Switch to the new database so subsequent objects live there.
\connect payments_operations

-- 2. Create the four roles if they do not exist.
SELECT format(
    'CREATE ROLE payments_ops_migration LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
    :'migration_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'payments_ops_migration'
) \gexec

SELECT format(
    'CREATE ROLE payments_ops_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
    :'writer_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'payments_ops_writer'
) \gexec

SELECT format(
    'CREATE ROLE payments_ops_reader NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
    :'reader_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'payments_ops_reader'
) \gexec

SELECT format(
    'CREATE ROLE grafana_payments_ro LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD %L',
    :'grafana_password'
)
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'grafana_payments_ro'
) \gexec

-- 3. Set passwords for each role (idempotent, safe to re-run).
SELECT format(
    'ALTER ROLE payments_ops_migration PASSWORD %L',
    :'migration_password'
) \gexec

SELECT format(
    'ALTER ROLE payments_ops_writer PASSWORD %L',
    :'writer_password'
) \gexec

SELECT format(
    'ALTER ROLE payments_ops_reader PASSWORD %L',
    :'reader_password'
) \gexec

SELECT format(
    'ALTER ROLE grafana_payments_ro PASSWORD %L',
    :'grafana_password'
) \gexec

-- 4. Grant CONNECT on the database to all four roles.
SELECT format(
    'GRANT CONNECT ON DATABASE payments_operations TO payments_ops_migration'
) \gexec

SELECT format(
    'GRANT CONNECT ON DATABASE payments_operations TO payments_ops_writer'
) \gexec

SELECT format(
    'GRANT CONNECT ON DATABASE payments_operations TO payments_ops_reader'
) \gexec

SELECT format(
    'GRANT CONNECT ON DATABASE payments_operations TO grafana_payments_ro'
) \gexec

-- 5. Create the payment_lifecycle schema owned by payments_ops_migration.
CREATE SCHEMA IF NOT EXISTS payment_lifecycle
    AUTHORIZATION payments_ops_migration;

-- 6. Grant USAGE on payment_lifecycle to all four roles.
GRANT USAGE ON SCHEMA payment_lifecycle TO payments_ops_migration;
GRANT USAGE ON SCHEMA payment_lifecycle TO payments_ops_writer;
GRANT USAGE ON SCHEMA payment_lifecycle TO payments_ops_reader;
GRANT USAGE ON SCHEMA payment_lifecycle TO grafana_payments_ro;

-- 7. Grant CREATE on payment_lifecycle to payments_ops_migration only.
GRANT CREATE ON SCHEMA payment_lifecycle TO payments_ops_migration;

-- 8. Default privileges for objects created by payments_ops_migration in
--    payment_lifecycle: tables and sequences to the writer, tables to the
--    reader and grafana read-only role.
ALTER DEFAULT PRIVILEGES FOR ROLE payments_ops_migration
    IN SCHEMA payment_lifecycle
    GRANT SELECT, INSERT, UPDATE, DELETE
    ON TABLES TO payments_ops_writer;

ALTER DEFAULT PRIVILEGES FOR ROLE payments_ops_migration
    IN SCHEMA payment_lifecycle
    GRANT USAGE, SELECT
    ON SEQUENCES TO payments_ops_writer;

ALTER DEFAULT PRIVILEGES FOR ROLE payments_ops_migration
    IN SCHEMA payment_lifecycle
    GRANT SELECT
    ON TABLES TO payments_ops_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE payments_ops_migration
    IN SCHEMA payment_lifecycle
    GRANT SELECT
    ON TABLES TO grafana_payments_ro;
