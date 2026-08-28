#!/usr/bin/env bash
set -eo pipefail

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] SILVER-ENTRY: $*"
}

log "🚀 Starting Silver Iceberg Ingestion Service (v6.1.1)..."

# 1. Wait for infrastructure
for svc in "${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}" "${NESSIE_HOST}:${NESSIE_PORT}" "${HIVE_METASTORE_HOST}:${HIVE_METASTORE_PORT}"; do
  log "⏳ Waiting for ${svc}..."
  until nc -z ${svc//:/ }; do sleep 2; done
done
log "✅ All infrastructure services are online."

# 2. Setup Ivy No-Op
mkdir -p /tmp/disable_ivy
cat > /tmp/disable_ivy/ivysettings.xml << 'EOF'
<ivysettings>
    <settings defaultResolver="noop"/><resolvers><ibiblio name="noop" m2compatible="true" root="file:///dev/null"/></resolvers>
</ivysettings>
EOF

# 3. Path Cleanup
export PYSPARK_SUBMIT_ARGS=""
export PYTHONPATH="/app/scripts:/app/scripts/s3_silver_ingestion:$PYTHONPATH"

# Define Jars (Targeting the exact files downloaded in Dockerfile)
JAR_LIST="/opt/spark/jars/iceberg-spark-runtime-3.4_2.12-1.4.3.jar,/opt/spark/jars/nessie-spark-extensions-3.4_2.12-0.77.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"

log "📡 Submitting Silver Iceberg Spark Job..."

exec spark-submit \
    --master "${SPARK_MASTER_URL}" \
    --deploy-mode client \
    --name "S3-Silver-Iceberg-Ingestion" \
    --jars "$JAR_LIST" \
    --py-files "/app/scripts/s3_silver_ingestion" \
    --conf "spark.jars.ivySettings=file:///tmp/disable_ivy/ivysettings.xml" \
    --conf "spark.jars.packages=" \
    --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
    --conf "spark.driver.bindAddress=${SPARK_DRIVER_BIND_ADDRESS}" \
    --driver-memory "${SPARK_DRIVER_MEMORY}" \
    --executor-memory "${SPARK_EXECUTOR_MEMORY}" \
    --executor-cores "${SPARK_EXECUTOR_CORES}" \
    "/app/scripts/s3_silver_ingestion/modular/orchestrators/main.py" \
    --config /app/config/s3_silver_ingestion/s3_silver_ingestion.yaml