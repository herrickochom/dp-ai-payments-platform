#!/bin/sh
set -eu
: "${PDM_CDC_USER:?PDM_CDC_USER is required}"
: "${PDM_CDC_PASSWORD:?PDM_CDC_PASSWORD is required}"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv db_name POSTGRES_DB
\getenv cdc_user PDM_CDC_USER
\getenv cdc_password PDM_CDC_PASSWORD
SELECT format('CREATE ROLE %I WITH LOGIN REPLICATION PASSWORD %L', :'cdc_user', :'cdc_password') \gexec
GRANT CONNECT ON DATABASE :"db_name" TO :"cdc_user";
GRANT USAGE ON SCHEMA mdm TO :"cdc_user";
GRANT SELECT ON mdm.sacco_master_sources TO :"cdc_user";
SELECT pg_create_logical_replication_slot('pdm_mdm_ordinary_slot', 'pgoutput');
SQL
