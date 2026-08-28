#!/bin/bash
set -e

echo "Running PostgreSQL healthcheck..."
pg_isready -U ${POSTGRES_USER} || exit 1
echo "PostgreSQL is ready."
