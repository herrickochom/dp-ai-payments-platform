#!/usr/bin/env bash
set -e

PROJECT_NAME="dp-ai-payments-platform"

echo "=============================================================="
echo "🧹 DP AI Payments Platform - Environment Cleanup"
echo "=============================================================="

echo "🛑 Stopping and removing all running/stopped containers..."
docker compose down --remove-orphans || true

echo "📦 Removing project-specific Docker volumes..."
VOLUMES=$(docker volume ls -q | grep "${PROJECT_NAME}_" || true)

if [ -n "$VOLUMES" ]; then
  echo "$VOLUMES" | xargs docker volume rm -f
  echo "✅ All project volumes removed."
else
  echo "ℹ️  No project volumes found."
fi

echo "🧽 Pruning ALL Docker resources (containers, images, networks, volumes)..."
docker system prune -a --volumes -f

echo "🗑️ Clearing backend logs..."
LOG_DIRS=(
  "backend/data/ingestion/logs"
  "backend/scripts/logs"
  "backend/trino/logs"
  "backend/spark/logs"
)

for dir in "${LOG_DIRS[@]}"; do
  if [ -d "$dir" ]; then
    rm -rf "${dir:?}"/*
    echo "🗑️  Cleared logs in $dir"
  fi
done

echo "=============================================================="
echo "✅ Cleanup complete!"
echo "🧾 Remaining volumes:"
docker volume ls

echo "🚀 Rebuild and restart with:"
echo "    docker compose up -d --build"
echo "=============================================================="
