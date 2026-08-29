#!/bin/bash

set -euo pipefail

create_database() {
    local database="$1"

    echo "Creating database '$database'"

    psql \
        -v ON_ERROR_STOP=1 \
        --username "$POSTGRES_USER" \
        --dbname "${POSTGRES_DB:-postgres}" \
        --set database="$database" <<-'EOSQL'
            SELECT format('CREATE DATABASE %I', :'database')
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_database
                WHERE datname = :'database'
            )\gexec

            SELECT format(
                'GRANT ALL PRIVILEGES ON DATABASE %I TO %I',
                :'database',
                current_user
            )\gexec
EOSQL

    echo "Database '$database' ready"
}

if [ -n "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
    echo "Multiple database creation requested: ${POSTGRES_MULTIPLE_DATABASES}"

    IFS=',' read -ra DATABASES <<< "$POSTGRES_MULTIPLE_DATABASES"

    for db in "${DATABASES[@]}"; do
        db="$(echo "$db" | xargs)"

        if [ -n "$db" ]; then
            create_database "$db"
        fi
    done

    echo "Multiple databases created successfully"
fi