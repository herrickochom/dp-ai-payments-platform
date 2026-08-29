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