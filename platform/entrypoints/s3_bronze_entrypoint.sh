#!/bin/bash
# First, fix and set JAVA_HOME properly
export JAVA_HOME=${JAVA_HOME:-/opt/java/openjdk}

# Print the correct banner
echo "================================================"
echo "🚀 Bronze Ingestion Service (Hudi) Initialized"
echo "SPARK_HOME : /opt/spark"
echo "JAVA_HOME  : $JAVA_HOME"
echo "================================================"

# Then continue with your script
set -euo pipefail

trap '' PIPE

# ============================================================================
# Bronze Hudi Ingestion Entrypoint
# Version: 1.3.0 (Enhanced with Diagnostics & Validation)
# ============================================================================

export SERVICE_VERSION="1.3.0"

log() {
  local level="$1"; shift
  printf "[$(date '+%Y-%m-%d %H:%M:%S')] %-7s BRONZE-ENTRY: %s\n" "$level" "$*"
}

log "INFO" "🚀 Starting Hardened Entrypoint Script v$SERVICE_VERSION..."

# ============================================================================
# 1️⃣ SIGNAL HANDLING FOR GRACEFUL SHUTDOWN
# ============================================================================
cleanup() {
    log "WARNING" "Received shutdown signal, cleaning up..."
    # Kill any stray processes
    pkill -f "spark" 2>/dev/null || true
    log "INFO" "Cleanup complete"
    exit 0
}

trap cleanup SIGINT SIGTERM

# ============================================================================
# 2️⃣ ENVIRONMENT ALIGNMENT (Updated to match your logs)
# ============================================================================
export JAVA_HOME=${JAVA_HOME:-/opt/java/openjdk}
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"

# ============================================================================
# 3️⃣ SYSTEM METRICS & DIAGNOSTICS
# ============================================================================
log "INFO" "Gathering system metrics..."
MEM_FREE=$(free -m | awk 'NR==2{printf "%.1f%%", $4*100/$2}')
DISK_FREE=$(df -h / | awk 'NR==2{print $4}')
CPU_CORES=$(nproc)
log "INFO" "System Status - Memory: $MEM_FREE free, Disk: $DISK_FREE free, Cores: $CPU_CORES"

# ============================================================================
# 4️⃣ VERSION COMPATIBILITY CHECK
# ============================================================================
log "INFO" "Checking component versions..."
java -version 2>&1 | head -3
python3 --version
if command -v spark-shell &> /dev/null; then
    spark-shell --version 2>&1 | head -1
fi

# ============================================================================
# 5️⃣ PYTHON PATHING
# ============================================================================
PY4J_ZIP=$(find "${SPARK_HOME}/python/lib" -name "py4j-*-src.zip" 2>/dev/null | head -n1)
export PYTHONPATH="/app/scripts:/app/scripts/s3_bronze_ingestion:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/pyspark.zip:${PY4J_ZIP}"

# ============================================================================
# 6️⃣ CLASSPATH PREPARATION - CRITICAL FIX WITH DIAGNOSTICS
# ============================================================================
BASE_JAR_DIR="/opt/spark/jars"
HIVE_JAR_DIR="/opt/hive-jars"

# Enhanced classpath diagnostic
log "INFO" "Classpath diagnostic..."
SPARK_JAR_COUNT=$(find "$BASE_JAR_DIR" -name "*.jar" 2>/dev/null | wc -l)
HIVE_JAR_COUNT=$(find "$HIVE_JAR_DIR" -name "*.jar" 2>/dev/null | wc -l)
log "INFO" "Total JARs in spark/jars: $SPARK_JAR_COUNT"
log "INFO" "Total JARs in hive-jars: $HIVE_JAR_COUNT"

# Build classpath with proper ordering
export SPARK_DIST_CLASSPATH=""
SPARK_JARS=$(find /opt/spark/jars -name "*.jar" -printf "%p:" | sort | tr -d '\n')
HIVE_JARS=$(find /opt/hive-jars -name "*.jar" 2>/dev/null -printf "%p:" | sort | tr -d '\n')
EXTRA_CP="${SPARK_JARS}${HIVE_JARS}"

# Ensure Hudi JAR is first in classpath for priority
HUDI_JAR=$(find /opt/spark/jars -name "*hudi*spark*.jar" | head -1)
if [ -n "$HUDI_JAR" ]; then
    # Remove Hudi JAR from its original position and prepend it
    EXTRA_CP="${HUDI_JAR}:${EXTRA_CP//${HUDI_JAR}:/}"
    log "INFO" "Prioritized Hudi JAR in classpath: $(basename "$HUDI_JAR")"
fi

export SPARK_DIST_CLASSPATH="${EXTRA_CP}"
log "INFO" "Classpath configured (length: ${#EXTRA_CP} chars)"

# ============================================================================
# 7️⃣ CONFIGURATION VALIDATION
# ============================================================================
validate_config() {
    local missing_vars=()
    
    # Required environment variables
    local required_vars=(
        "S3_ENDPOINT"
        "AWS_ACCESS_KEY_ID"
        "AWS_SECRET_ACCESS_KEY"
        "HIVE_METASTORE_URI"
        "S3_BRONZE_INGESTION_CONFIG"
    )
    
    for var in "${required_vars[@]}"; do
        if [ -z "${!var:-}" ]; then
            missing_vars+=("$var")
        fi
    done
    
    if [ ${#missing_vars[@]} -gt 0 ]; then
        log "ERROR" "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            log "ERROR" "  - $var"
        done
        return 1
    fi
    
    # Check config file exists
    if [ ! -f "${S3_BRONZE_INGESTION_CONFIG}" ]; then
        log "ERROR" "Config file not found: ${S3_BRONZE_INGESTION_CONFIG}"
        return 1
    fi
    
    log "INFO" "✅ All required configurations present"
    return 0
}

# ============================================================================
# 8️⃣ MEMORY VALIDATION
# ============================================================================
validate_memory() {
    local total_mem=$(free -m | awk '/^Mem:/{print $2}')
    # Parse memory settings (support formats: 2g, 4g, 2048m, etc.)
    local driver_mem_raw=${SPARK_DRIVER_MEMORY:-2g}
    local executor_mem_raw=${SPARK_EXECUTOR_MEMORY:-2g}
    
    # Convert to MB
    local driver_mem=$(echo "$driver_mem_raw" | sed -E 's/([0-9]+)([gGmM]?)/\1 \2/' | awk '
        $2 ~ /[gG]/ {print $1 * 1024}
        $2 ~ /[mM]/ {print $1}
        $2 == "" {print $1 * 1024}  # Default to GB if no unit
    ')
    
    local executor_mem=$(echo "$executor_mem_raw" | sed -E 's/([0-9]+)([gGmM]?)/\1 \2/' | awk '
        $2 ~ /[gG]/ {print $1 * 1024}
        $2 ~ /[mM]/ {print $1}
        $2 == "" {print $1 * 1024}
    ')
    
    # Add 20% overhead for Spark's own memory
    local driver_with_overhead=$((driver_mem + driver_mem / 5))
    
    if [ "$driver_with_overhead" -gt "$total_mem" ]; then
        log "ERROR" "Driver memory ($driver_mem_raw → ${driver_with_overhead}MB with overhead) exceeds system memory ($total_mem MB)"
        return 1
    fi
    
    log "INFO" "Memory validation passed: System=$total_mem MB, Driver=$driver_mem MB, Executor=$executor_mem MB"
    return 0
}

# ============================================================================
# 9️⃣ NETWORK & INFRASTRUCTURE CHECKS (Robust Parsing)
# ============================================================================
SPARK_DRIVER_HOST=${S3_BRONZE_INGESTION_IP:-$(hostname -i | awk '{print $1}')}
log "INFO" "Using Driver Host: $SPARK_DRIVER_HOST"

check_network_services() {
    # Services to check
    CHECK_MINIO="${S3_ENDPOINT:-minio:9000}"
    CHECK_HIVE="${HIVE_METASTORE_URI:-hive-metastore:9083}"
    CHECK_SPARK="${SPARK_MASTER_URL:-spark-master:7077}"
    
    local all_services_passed=true
    
    for svc in "$CHECK_MINIO" "$CHECK_HIVE" "$CHECK_SPARK"; do
        # Robustly strip protocols (http://, thrift://, spark://) and trailing paths
        clean_svc=$(echo "$svc" | sed -E 's|^.*://||; s|/.*$||')
        
        # Extract host and port
        host=${clean_svc%:*}
        port=${clean_svc##*:}
        
        # Default port if none found
        if [ "$host" == "$port" ]; then
            case "$svc" in
                *minio*) port=9000 ;;
                *hive*)  port=9083 ;;
                *spark*) port=7077 ;;
            esac
        fi

        local retries=5
        local success=false
        
        for ((i=1; i<=retries; i++)); do
            if timeout 5 nc -z -w 2 "$host" "$port" 2>/dev/null; then
                success=true
                log "INFO" "✅ Service $host:$port is reachable (attempt $i/$retries)"
                break
            else
                log "WARNING" "Waiting for $svc (parsed as $host:$port)... (attempt $i/$retries)"
                sleep 2
            fi
        done
        
        if [ "$success" = false ]; then
            log "ERROR" "❌ Service $host:$port is unreachable after $retries attempts"
            all_services_passed=false
        fi
    done
    
    [ "$all_services_passed" = true ]
    return $?
}

# ============================================================================
# 🔟 HUDI JAR VERIFICATION - CRITICAL CHECK
# ============================================================================
verify_hudi_jars() {
    log "INFO" "Verifying Hudi JAR availability..."
    HUDI_JAR_PATH="/opt/spark/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar"
    
    if [ -f "$HUDI_JAR_PATH" ]; then
        log "INFO" "✅ Hudi JAR found: $(basename "$HUDI_JAR_PATH")"
        HUDI_JAR_SIZE=$(du -h "$HUDI_JAR_PATH" | cut -f1)
        log "INFO" "   JAR Size: $HUDI_JAR_SIZE"
    else
        log "ERROR" "❌ Hudi JAR not found at $HUDI_JAR_PATH"
        log "ERROR" "Available Hudi JARs in /opt/spark/jars/:"
        ls -la /opt/spark/jars/hudi*.jar 2>/dev/null || log "ERROR" "No Hudi JARs found"
        return 1
    fi

    # Check all required JARs
    REQUIRED_JARS=(
        "hudi-spark3.4-bundle_2.12-0.14.1.jar"
        "hadoop-aws-3.3.4.jar"
        "aws-java-sdk-bundle-1.12.262.jar"
    )

    local missing_jars=0
    for jar in "${REQUIRED_JARS[@]}"; do
        if [ -f "/opt/spark/jars/$jar" ]; then
            log "INFO" "✅ Required JAR found: $jar"
        else
            log "WARNING" "⚠️  Missing JAR: $jar"
            missing_jars=$((missing_jars + 1))
        fi
    done
    
    [ "$missing_jars" -eq 0 ]
    return $?
}

# ============================================================================
# 1️⃣1️⃣ SPARK CONFIGURATION TEST - Quick validation
# ============================================================================
test_spark_config() {
    log "INFO" "Testing Spark configuration..."

    if python3 -c "
import os
import sys

os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'

try:
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName('ConfigTest') \
        .master('local[1]') \
        .config('spark.driver.extraClassPath', '/opt/spark/jars/*:/opt/hive-jars/*') \
        .config('spark.executor.extraClassPath', '/opt/spark/jars/*:/opt/hive-jars/*') \
        .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Test Hudi class loading
    try:
        spark._jvm.java.lang.Class.forName('org.apache.hudi.DefaultSource')
        print('✅ Spark session created and Hudi DefaultSource loaded')
    except Exception as e:
        print(f'⚠️  Hudi DefaultSource not found: {e}')
        # Try alternative classes
        alternatives = [
            'org.apache.hudi.DataSourceUtils',
            'org.apache.hudi.HoodieSparkSessionExtension',
            'org.apache.hudi.HoodieCatalog'
        ]
        for cls in alternatives:
            try:
                spark._jvm.java.lang.Class.forName(cls)
                print(f'✅ Hudi class loaded: {cls}')
                break
            except:
                continue
        else:
            print('❌ No Hudi classes could be loaded')
            spark.stop()
            sys.exit(1)
    
    # Test basic operations
    test_df = spark.range(10)
    count = test_df.count()
    if count == 10:
        print('✅ Basic DataFrame operations working')
    else:
        print(f'❌ DataFrame count mismatch: expected 10, got {count}')
    
    spark.stop()
    print('✅ Spark configuration test passed')
    
except Exception as e:
    print(f'❌ Spark session creation failed: {e}')
    import traceback
    traceback.print_exc()
    sys.exit(1)
"; then
        log "INFO" "✅ Spark configuration test passed"
        return 0
    else
        log "ERROR" "❌ Spark configuration test failed"
        return 1
    fi
}

# ============================================================================
# 1️⃣2️⃣ FINAL PRE-FLIGHT CHECK
# ============================================================================
log "INFO" "🧪 Performing final pre-flight checks..."

PREFLIGHT_CHECKS=(
    validate_config
    validate_memory
    check_network_services
    verify_hudi_jars
    test_spark_config
)

all_passed=true
for check in "${PREFLIGHT_CHECKS[@]}"; do
    log "INFO" "Running check: $check..."
    if ! $check; then
        log "ERROR" "❌ Check failed: $check"
        all_passed=false
    else
        log "INFO" "✅ Check passed: $check"
    fi
done

if ! $all_passed; then
    log "ERROR" "❌ Pre-flight checks failed. Aborting."
    exit 1
fi

log "INFO" "✅ All pre-flight checks passed!"
log "INFO" "🎯 Ready for launch..."

# ============================================================================
# 1️⃣3️⃣ EXECUTE SPARK SUBMIT - WITH FIXED CLASSPATH
# ============================================================================
log "INFO" "Launching Spark Submit with Hudi support..."

exec spark-submit \
  --master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
  --deploy-mode client \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.driver.host=${SPARK_DRIVER_HOST}" \
  --conf "spark.jars.packages=" \
  --conf "spark.driver.port=7078" \
  --conf "spark.blockManager.port=7079" \
  --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
  --conf "spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar" \
  --conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
  --conf "spark.sql.catalogImplementation=hive" \
  --conf "spark.driver.extraClassPath=${EXTRA_CP}" \
  --conf "spark.executor.extraClassPath=${EXTRA_CP}" \
  --conf "spark.jars=/opt/spark/jars/hudi-spark3.4-bundle_2.12-0.14.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar" \
  --conf "spark.hadoop.hive.metastore.uris=${HIVE_METASTORE_URI}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}" \
  --conf "spark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.hadoop.fs.s3a.connection.maximum=100" \
  --conf "spark.hadoop.fs.s3a.fast.upload=true" \
  --conf "spark.hadoop.fs.s3a.multipart.size=104857600" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS:-200}" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
  --conf "spark.sql.adaptive.coalescePartitions.minPartitionSize=64MB" \
  --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB" \
  --conf "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dlog4j2.configurationFile=/opt/spark/conf/log4j2.properties -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dlog4j2.configurationFile=/opt/spark/conf/log4j2.properties -XX:+ExitOnOutOfMemoryError" \
  --conf "spark.driverEnv.PYTHONPATH=${PYTHONPATH}" \
  --conf "spark.executorEnv.PYTHONPATH=${PYTHONPATH}" \
  --conf "spark.driverEnv.SPARK_DIST_CLASSPATH=${EXTRA_CP}" \
  --conf "spark.executorEnv.SPARK_DIST_CLASSPATH=${EXTRA_CP}" \
  --files "${S3_BRONZE_INGESTION_CONFIG}" \
  "/app/scripts/s3_bronze_ingestion/modular/orchestrators/main.py"

# Exit with success (though exec should replace this process)
log "INFO" "Spark submit command executed"
exit 0