#!/bin/bash
set -e

echo "Running PostgreSQL healthcheck..."
pg_isready -h postgres -p 5432 -U "${POSTGRES_USER}" -d postgres || exit 1
echo "PostgreSQL is ready."
