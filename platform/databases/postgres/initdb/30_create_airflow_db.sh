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
