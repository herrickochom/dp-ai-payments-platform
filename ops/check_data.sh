#!/bin/bash
echo "=== Data Lake Summary ==="
echo ""

# `mc` inside the minio container only knows its baked-in demo aliases
# (gcs/play/s3 plus a credential-less `local`), so (re)bind `local` to the
# running server with the container's own root credentials before listing.
if ! docker compose exec -T minio sh -c \
    'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"' >/dev/null; then
  echo "ERROR: could not reach the minio container (is the stack up? try: docker compose up -d minio)"
  exit 1
fi

# One recursive listing of the raw landing zone; greps run on the host because
# the minio image ships without grep (piping inside the container fails).
RAW_LISTING=$(docker compose exec -T minio sh -c 'mc ls --recursive local/dp-ai-payment/raw/v2/' 2>/dev/null)

echo "Raw Layer (Avro landing):"
echo "  Total raw files: $(echo "$RAW_LISTING" | grep -c '\.avro$')"
echo ""
echo "By source system:"
for system in $(echo "$RAW_LISTING" | grep -o 'source_system=[^/]*' | cut -d= -f2 | sort -u); do
  count=$(echo "$RAW_LISTING" | grep "source_system=$system/" | grep -c '\.avro$')
  if [ "$count" -gt 0 ]; then
    echo "  $system: $count"
  fi
done
echo ""
echo "Warehouse (Iceberg tables via Nessie):"
for ns in bronze silver gold staging consumption; do
  count=$(docker compose exec -T minio mc ls --recursive "local/dp-ai-payment/warehouse/$ns/" 2>/dev/null | grep -c '\.parquet$')
  if [ "$count" -gt 0 ]; then
    echo "  $ns: $count parquet data files"
  fi
done
