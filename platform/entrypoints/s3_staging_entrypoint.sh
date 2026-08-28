#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# S3 Staging Ingestion Entrypoint (v1.0.4) - Fixed Resource Alignment
# ============================================================================

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] STAGING-ENTRY: $*"
}

log "🚀 Starting S3 Staging Ingestion Service (v1.0.4)..."

# ----------------------------------------------------------------------------
# Check if the main script exists
# ----------------------------------------------------------------------------
MAIN_SCRIPT="/app/scripts/s3_staging_ingestion/modular/orchestrators/main.py"
if [ ! -f "$MAIN_SCRIPT" ]; then
    log "❌ ERROR: Main script not found: $MAIN_SCRIPT"
    exit 1
fi
log "✅ Main script found: $MAIN_SCRIPT"

# ----------------------------------------------------------------------------
# Networking: Explicitly bind to the Static IP for Spark Handshake
# ----------------------------------------------------------------------------
if [ -n "${S3_STAGING_INGESTION_IP:-}" ]; then
    export SPARK_DRIVER_HOST="${S3_STAGING_INGESTION_IP}"
    log "Using Static IP as SPARK_DRIVER_HOST: ${SPARK_DRIVER_HOST}"
else
    export SPARK_DRIVER_HOST="$(hostname -i)"
    log "S3_STAGING_INGESTION_IP not found, using: ${SPARK_DRIVER_HOST}"
fi

# ----------------------------------------------------------------------------
# Resource Tuning: Ensuring it fits within the 4GB Worker limit
# ----------------------------------------------------------------------------
# Requesting 2GB + 1GB overhead = 3GB (Fits comfortably in 4GB Worker)
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
export SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
export SPARK_EXECUTOR_INSTANCES="${SPARK_EXECUTOR_INSTANCES:-1}"
export SPARK_MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
export SPARK_DRIVER_BIND_ADDRESS="${SPARK_DRIVER_BIND_ADDRESS:-0.0.0.0}"
export SPARK_DRIVER_PORT="${SPARK_DRIVER_PORT:-7078}"
export SPARK_BLOCK_MANAGER_PORT="${SPARK_BLOCK_MANAGER_PORT:-7079}"
export SPARK_DRIVER_MAX_RESULT_SIZE="${SPARK_DRIVER_MAX_RESULT_SIZE:-1g}"
export SPARK_DRIVER_MEMORY_OVERHEAD="${SPARK_DRIVER_MEMORY_OVERHEAD:-512m}"
export SPARK_EXECUTOR_MEMORY_OVERHEAD="${SPARK_EXECUTOR_MEMORY_OVERHEAD:-1g}"

# S3/AWS Defaults
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
export HADOOP_AWS_VERSION="${HADOOP_AWS_VERSION:-3.3.4}"
export AWS_SDK_BUNDLE_VERSION="${AWS_SDK_BUNDLE_VERSION:-1.12.262}"
export S3_STAGING_INGESTION_CONFIG="${S3_STAGING_INGESTION_CONFIG:-/app/config/s3_staging_ingestion/s3_staging_ingestion.yaml}"

# PYTHONPATH logic
export PYTHONPATH="/app/scripts:${PYTHONPATH:-}"

# ----------------------------------------------------------------------------
# Wait for Infrastructure
# ----------------------------------------------------------------------------
SPARK_MASTER_HOST_PORT="${SPARK_MASTER_URL#spark://}"
log "⏳ Waiting for Spark Master at ${SPARK_MASTER_HOST_PORT}..."
until nc -z "${SPARK_MASTER_HOST_PORT%:*}" "${SPARK_MASTER_HOST_PORT#*:}"; do sleep 2; done

MINIO_HOST_PORT="${S3_ENDPOINT#http://}"
log "⏳ Waiting for MinIO at ${MINIO_HOST_PORT}..."
until nc -z "${MINIO_HOST_PORT%:*}" "${MINIO_HOST_PORT#*:}"; do sleep 2; done

# ----------------------------------------------------------------------------
# Submit Spark Job
# ----------------------------------------------------------------------------
log "📡 Submitting S3 Staging ingestion job..."

SPARK_TOTAL_EXECUTOR_CORES=$((SPARK_EXECUTOR_CORES * SPARK_EXECUTOR_INSTANCES))

exec spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --deploy-mode client \
  --name "S3-Staging-Ingestion-v1.0.4" \
  --packages "org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION},com.amazonaws:aws-java-sdk-bundle:${AWS_SDK_BUNDLE_VERSION}" \
  --files "${S3_STAGING_INGESTION_CONFIG}" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY}" \
  --executor-cores "${SPARK_EXECUTOR_CORES}" \
  --num-executors "${SPARK_EXECUTOR_INSTANCES}" \
  --total-executor-cores "${SPARK_TOTAL_EXECUTOR_CORES}" \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
  --conf "spark.driver.bindAddress=${SPARK_DRIVER_BIND_ADDRESS}" \
  --conf "spark.driver.port=${SPARK_DRIVER_PORT}" \
  --conf "spark.blockManager.port=${SPARK_BLOCK_MANAGER_PORT}" \
  --conf "spark.driver.memoryOverhead=${SPARK_DRIVER_MEMORY_OVERHEAD}" \
  --conf "spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORY_OVERHEAD}" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}" \
  --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.sql.warehouse.dir=s3a://loyalty-marketing-warehouse/spark-warehouse" \
  --conf "spark.driver.extraJavaOptions=$SPARK_DRIVER_EXTRA_OPTS" \
  --conf "spark.executor.extraJavaOptions=$SPARK_DRIVER_EXTRA_OPTS" \
  --conf "spark.sql.adaptive.enabled=true" \
  "$MAIN_SCRIPT"

EXIT_CODE=$?
log "Staging Spark job exited with code $EXIT_CODE"
exit $EXIT_CODE