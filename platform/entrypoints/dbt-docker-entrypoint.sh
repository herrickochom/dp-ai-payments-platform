#!/bin/sh
set -e

echo "Starting dbt..."
echo "DBT_PROFILES_DIR=${DBT_PROFILES_DIR:-/usr/app/dbt}"
echo "TRINO_HOST=${TRINO_HOST:-trino}"
echo "TRINO_PORT=${TRINO_PORT:-8080}"
echo "MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}"
echo "MINIO_BUCKET=${MINIO_BUCKET:-dp-ai-payment}"
echo "RAW_ROOT=${RAW_ROOT:-raw/v2}"
echo "NESSIE_ENDPOINT=${NESSIE_ENDPOINT:-http://nessie-rest-proxy:19121}"

exec dbt "$@"