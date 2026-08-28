#!/bin/bash
echo "=== Data Lake Summary ==="
echo ""
echo "Events Layer (JSON):"
EVENTS=$(docker compose exec minio mc ls local/dp-ai-payment/events/landing/ --recursive 2>/dev/null | grep ".json" | wc -l)
echo "  Total events files: $EVENTS"
echo ""
echo "Bronze Layer (Parquet):"
BRONZE=$(docker compose exec minio mc ls local/dp-ai-payment/bronze/valid/ --recursive 2>/dev/null | grep ".parquet" | wc -l)
echo "  Total bronze files: $BRONZE"
echo ""
echo "By source system:"
for system in vpm psn pml api batch rtgs swift; do
  count=$(docker compose exec minio mc ls local/dp-ai-payment/bronze/valid/data/$system/ --recursive 2>/dev/null | grep ".parquet" | wc -l)
  if [ "$count" -gt 0 ]; then
    echo "  $system: $count"
  fi
done
