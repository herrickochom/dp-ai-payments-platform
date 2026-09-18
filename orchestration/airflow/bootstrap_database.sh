#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_DB="${AIRFLOW_DB:-airflow}"

if ! [[ "$AIRFLOW_DB" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
    echo "Invalid AIRFLOW_DB name" >&2
    exit 2
fi

docker compose exec -T postgres \
  psql \
  --username "${POSTGRES_USER}" \
  --dbname postgres \
  -v ON_ERROR_STOP=1 \
  -tAc \
  "SELECT 1 FROM pg_database WHERE datname='${AIRFLOW_DB}'" |
grep -qx 1 && {
    echo "AIRFLOW_DATABASE_ALREADY_EXISTS=YES"
    exit 0
}

docker compose exec -T postgres \
  createdb \
  --username "${POSTGRES_USER}" \
  "${AIRFLOW_DB}"

echo "AIRFLOW_DATABASE_CREATED=YES"
