#!/bin/sh
set -e

# Wait for MinIO to be ready
echo "Waiting for MinIO at ${MINIO_ENDPOINT}..."
until mc alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"; do
  sleep 1
done

echo "Creating buckets..."
mc mb local/dp-ai-payment || true

echo "Setting bucket policies..."
mc policy set download local/dp-ai-payment

echo "Buckets created successfully."
