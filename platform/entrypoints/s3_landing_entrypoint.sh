#!/bin/bash
# =====================================================
# Entrypoint: S3 Landing Ingestion - Production Ready
# =====================================================

set -euo pipefail

# --- Configuration ---
readonly MAIN_SCRIPT_PATH="/app/scripts/s3_landing_ingestion/s3_landing_ingestion.py"
readonly CONFIG_DIR="/app/config/s3_data_ingestion/landing/yaml"
readonly LOG_DIR="/app/logs/s3_landing_ingestion"
readonly REPORT_DIR="/app/reports/s3_landing_ingestion"
readonly DATA_DIR="/app/data/landing"

# Default values
DEFAULT_S3_ENDPOINT="http://minio:9000"
DEFAULT_WAREHOUSE_BUCKET="loyalty-marketing-warehouse"
DEFAULT_LOG_LEVEL="INFO"
DEFAULT_MAX_WORKERS=8
DEFAULT_MAX_RETRIES=3
DEFAULT_RETRY_DELAY=10
DEFAULT_MINIO_HOST="minio"
DEFAULT_MINIO_PORT=9000

# --- Logging Function ---
log() {
    local level=$1; shift
    echo "[$(date '+%Y-%m-%d %H:%M:%S.%3N')] [$level] $*" >&2
}

# --- Signal Handling ---
handle_signal() {
    local signal=$1
    log "WARNING" "Received $signal, initiating graceful shutdown..."
    exit 143
}

trap 'handle_signal SIGTERM' TERM
trap 'handle_signal SIGINT' INT
trap 'handle_signal SIGQUIT' QUIT

# --- Load Environment Variables ---
load_env() {
    # Set defaults if not provided
    export S3_ENDPOINT="${S3_ENDPOINT:-$DEFAULT_S3_ENDPOINT}"
    export WAREHOUSE_BUCKET="${WAREHOUSE_BUCKET:-$DEFAULT_WAREHOUSE_BUCKET}"
    export LOG_LEVEL="${LOG_LEVEL:-$DEFAULT_LOG_LEVEL}"
    export INGESTION_MAX_WORKERS="${INGESTION_MAX_WORKERS:-$DEFAULT_MAX_WORKERS}"
    export MAX_RETRY_ATTEMPTS="${MAX_RETRY_ATTEMPTS:-$DEFAULT_MAX_RETRIES}"
    export RETRY_DELAY_SECONDS="${RETRY_DELAY_SECONDS:-$DEFAULT_RETRY_DELAY}"
    export ENVIRONMENT="${ENVIRONMENT:-production}"
    export MINIO_HOST="${MINIO_HOST:-$DEFAULT_MINIO_HOST}"
    export MINIO_PORT="${MINIO_PORT:-$DEFAULT_MINIO_PORT}"
    
    # AWS credentials
    export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
    export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
    export AWS_REGION="${AWS_REGION:-eu-west-1}"
    
    # Spark configuration
    export SPARK_MASTER="${SPARK_MASTER:-local[*]}"
    export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
    export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"
    
    # Set MC_CONFIG_DIR to a writable location
    export MC_CONFIG_DIR="/tmp/.mc"
    
    log "DEBUG" "Environment loaded:"
    log "DEBUG" "  S3_ENDPOINT=$S3_ENDPOINT"
    log "DEBUG" "  WAREHOUSE_BUCKET=$WAREHOUSE_BUCKET"
    log "DEBUG" "  ENVIRONMENT=$ENVIRONMENT"
    log "DEBUG" "  LOG_LEVEL=$LOG_LEVEL"
    log "DEBUG" "  MINIO_HOST=$MINIO_HOST"
    log "DEBUG" "  MINIO_PORT=$MINIO_PORT"
    log "DEBUG" "  MC_CONFIG_DIR=$MC_CONFIG_DIR"
}

# --- Create Writable Directories ---
create_writable_dirs() {
    log "INFO" "Creating writable directories..."
    
    # Create directories with proper permissions
    mkdir -p "$LOG_DIR" "$REPORT_DIR" "$DATA_DIR" "$MC_CONFIG_DIR" "/tmp/spark-events"
    
    # Set permissions for temp directories
    chmod 777 /tmp 2>/dev/null || true
    chmod 777 /tmp/spark-events 2>/dev/null || true
    chmod 777 "$MC_CONFIG_DIR" 2>/dev/null || true
    
    log "INFO" "✅ Writable directories created"
}

# --- Validate Dependencies ---
validate_dependencies() {
    log "INFO" "Validating dependencies..."
    
    # Check if main script exists
    if [ ! -f "$MAIN_SCRIPT_PATH" ]; then
        log "ERROR" "Main application script not found: $MAIN_SCRIPT_PATH"
        log "INFO" "Checking /app directory structure:"
        find /app -name "*.py" -type f 2>/dev/null | head -20 || true
        exit 1
    fi
    
    # Check if script is executable
    if [ ! -x "$MAIN_SCRIPT_PATH" ]; then
        log "WARNING" "Main script is not executable, fixing..."
        chmod +x "$MAIN_SCRIPT_PATH" 2>/dev/null || true
    fi
    
    # Check Python installation
    if ! python3 --version &>/dev/null; then
        log "ERROR" "Python3 is not available"
        exit 1
    fi
    
    # Check Spark installation
    if [ ! -f "/opt/spark/bin/spark-submit" ]; then
        log "ERROR" "Spark not found at /opt/spark/bin/spark-submit"
        exit 1
    fi
    
    log "INFO" "✅ All dependencies validated."
}

# --- Wait for Services ---
wait_for_service() {
    local service_name="$1" host="$2" port="$3"
    local max_attempts="${4:-30}" attempt=1
    
    log "INFO" "Waiting for ${service_name} at ${host}:${port}..."
    
    while ! nc -z "$host" "$port" 2>/dev/null; do
        if [ $attempt -gt $max_attempts ]; then
            log "ERROR" "Service ${service_name} did not become available after ${max_attempts} attempts. Exiting."
            return 1
        fi
        
        log "INFO" "  -> ${service_name} not ready (attempt ${attempt}/${max_attempts}). Retrying in 5 seconds..."
        sleep 5
        ((attempt++))
    done
    
    log "INFO" "✅ ${service_name} is available at ${host}:${port}"
    return 0
}

# --- Test MinIO Connection ---
test_minio_connection() {
    log "INFO" "Testing MinIO connection..."
    
    # First check if we can connect to the endpoint
    if ! timeout 10 curl -s -f "${S3_ENDPOINT}/minio/health/live" > /dev/null 2>&1; then
        log "ERROR" "Cannot connect to MinIO at ${S3_ENDPOINT}"
        return 1
    fi
    
    # Configure MinIO client using MC_CONFIG_DIR
    MC_CONFIG_DIR="$MC_CONFIG_DIR" mc alias set myminio \
        "$S3_ENDPOINT" \
        "$AWS_ACCESS_KEY_ID" \
        "$AWS_SECRET_ACCESS_KEY" \
        --api "s3v4" \
        --path "auto" 2>/dev/null || {
        log "WARNING" "Failed to configure MinIO alias, trying alternative method..."
    }
    
    # Test connection by listing buckets with MC_CONFIG_DIR
    if MC_CONFIG_DIR="$MC_CONFIG_DIR" mc ls myminio > /dev/null 2>&1; then
        log "INFO" "✅ MinIO connection successful."
        return 0
    else
        # Try alternative connection test using Python
        log "WARNING" "MC client failed, testing with Python..."
        if python_test_minio_connection; then
            log "INFO" "✅ MinIO connection successful (via Python)."
            return 0
        else
            log "ERROR" "Failed to connect to MinIO"
            return 1
        fi
    fi
}

# --- Test MinIO Connection with Python ---
python_test_minio_connection() {
    python3 -c "
import boto3
from botocore.client import Config
import sys

try:
    # Test basic connectivity
    import socket
    from urllib.parse import urlparse
    
    endpoint = '$S3_ENDPOINT'
    parsed = urlparse(endpoint)
    hostname = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == 'https' else 80)
    
    # Create socket connection test
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex((hostname, port))
    sock.close()
    
    if result != 0:
        print(f'Cannot connect to {hostname}:{port}')
        sys.exit(1)
    
    print('Network connectivity OK')
    
    # Test S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id='$AWS_ACCESS_KEY_ID',
        aws_secret_access_key='$AWS_SECRET_ACCESS_KEY',
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path'}
        )
    )
    
    # Try to list buckets
    response = s3.list_buckets()
    print(f'Connected to S3. Found {len(response.get(\"Buckets\", []))} buckets')
    sys.exit(0)
    
except Exception as e:
    print(f'Connection test failed: {e}')
    sys.exit(1)
" > /tmp/minio_test.log 2>&1
    
    local result=$?
    if [ $result -eq 0 ]; then
        cat /tmp/minio_test.log
        return 0
    else
        log "ERROR" "Python connection test failed:"
        cat /tmp/minio_test.log >&2
        return 1
    fi
}

# --- Validate S3 Bucket ---
validate_s3_bucket() {
    local bucket="$1"
    
    log "INFO" "Validating S3 bucket: $bucket"
    
    python3 -c "
import boto3
from botocore.client import Config
import sys

try:
    s3 = boto3.client(
        's3',
        endpoint_url='$S3_ENDPOINT',
        aws_access_key_id='$AWS_ACCESS_KEY_ID',
        aws_secret_access_key='$AWS_SECRET_ACCESS_KEY',
        region_name='$AWS_REGION',
        config=Config(
            signature_version='s3v4',
            s3={'addressing_style': 'path' if '$S3_ENDPOINT'.startswith('http://') else 'auto'}
        )
    )
    
    # List all buckets
    response = s3.list_buckets()
    buckets = [b['Name'] for b in response.get('Buckets', [])]
    
    if '$bucket' in buckets:
        print('Bucket exists: $bucket')
        sys.exit(0)
    else:
        # Try to create the bucket (without location constraint for MinIO)
        try:
            s3.create_bucket(Bucket='$bucket')
            print('Bucket created: $bucket')
            sys.exit(0)
        except Exception as e:
            # If bucket already exists (race condition) or other error
            if 'BucketAlreadyOwnedByYou' in str(e) or 'BucketAlreadyExists' in str(e):
                print('Bucket already exists: $bucket')
                sys.exit(0)
            else:
                print(f'Failed to create bucket: {e}')
                sys.exit(1)
            
except Exception as e:
    print(f'S3 connection error: {e}')
    sys.exit(1)
" > /tmp/s3_test.log 2>&1
    
    local result=$?
    if [ $result -eq 0 ]; then
        log "INFO" "✅ S3 bucket validation successful"
        cat /tmp/s3_test.log
        return 0
    else
        log "ERROR" "❌ S3 bucket validation failed:"
        cat /tmp/s3_test.log >&2
        return 1
    fi
}

# --- Check for Data Files ---
check_for_data_files() {
    log "INFO" "Checking for data files in: $DATA_DIR"
    
    if [ ! -d "$DATA_DIR" ]; then
        log "WARNING" "Data directory does not exist: $DATA_DIR"
        return 1
    fi
    
    # Count files in data directory
    local file_count=$(find "$DATA_DIR" -type f \( -name "*.parquet" -o -name "*.csv" -o -name "*.json" \) 2>/dev/null | wc -l)
    
    if [ "$file_count" -eq 0 ]; then
        log "WARNING" "No data files found in $DATA_DIR"
        
        # Check if directory is empty or has subdirectories
        local dir_count=$(find "$DATA_DIR" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l)
        if [ "$dir_count" -gt 0 ]; then
            log "INFO" "Found $dir_count subdirectories in $DATA_DIR"
            find "$DATA_DIR" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | head -5 | while read dir; do
                log "INFO" "  - $(basename "$dir")"
            done
        fi
        
        return 1
    else
        log "INFO" "Found $file_count data files in $DATA_DIR"
        
        # Show sample of files
        log "INFO" "Sample files:"
        find "$DATA_DIR" -type f \( -name "*.parquet" -o -name "*.csv" -o -name "*.json" \) 2>/dev/null | head -5 | while read file; do
            local size=$(du -h "$file" 2>/dev/null | cut -f1 || echo 'unknown')
            log "INFO" "  - $(basename "$file") ($size)"
        done
        
        return 0
    fi
}

# --- Build Spark Command ---
build_spark_command() {
    local args=()
    
    # Base Spark submit command
    args+=("/opt/spark/bin/spark-submit")
    
    # Spark configuration
    args+=("--master" "$SPARK_MASTER")
    args+=("--conf" "spark.driver.memory=$SPARK_DRIVER_MEMORY")
    args+=("--conf" "spark.executor.memory=$SPARK_EXECUTOR_MEMORY")
    args+=("--conf" "spark.sql.adaptive.enabled=true")
    args+=("--conf" "spark.sql.shuffle.partitions=200")
    
    # Logging configuration
    if [ -f "/opt/spark/conf/log4j.properties" ]; then
        args+=("--conf" "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties")
        args+=("--conf" "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties")
    fi
    
    # S3 configuration for Spark
    args+=("--conf" "spark.hadoop.fs.s3a.endpoint=$S3_ENDPOINT")
    args+=("--conf" "spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID")
    args+=("--conf" "spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY")
    args+=("--conf" "spark.hadoop.fs.s3a.path.style.access=true")
    args+=("--conf" "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem")
    args+=("--conf" "spark.hadoop.fs.s3a.connection.ssl.enabled=false")
    args+=("--conf" "spark.hadoop.fs.s3a.fast.upload=true")
    args+=("--conf" "spark.hadoop.fs.s3a.fast.upload.buffer=disk")
    
    # Main script
    args+=("$MAIN_SCRIPT_PATH")
    
    # Script arguments
    args+=("--log-level" "$LOG_LEVEL")
    
    # Add config file if exists
    local config_file=$(find "$CONFIG_DIR" -name "*.yaml" -o -name "*.yml" -o -name "*.yaml.example" -o -name "*.yml.example" 2>/dev/null | head -1)
    if [ -n "$config_file" ] && [ -f "$config_file" ]; then
        args+=("--config" "$config_file")
        log "INFO" "Using config file: $config_file"
    else
        log "WARNING" "No config file found in $CONFIG_DIR, using defaults"
    fi
    
    # Add data source path
    if [ -d "$DATA_DIR" ]; then
        args+=("--source" "$DATA_DIR")
    fi
    
    # Add max workers if specified
    if [ -n "${INGESTION_MAX_WORKERS:-}" ]; then
        args+=("--max-workers" "$INGESTION_MAX_WORKERS")
    fi
    
    # Add dry-run for development
    if [ "$ENVIRONMENT" = "development" ] || [ "${DRY_RUN:-false}" = "true" ]; then
        args+=("--dry-run")
        log "INFO" "Running in DRY-RUN mode"
    fi
    
    echo "${args[@]}"
}

# --- Run Ingestion ---
run_ingestion() {
    local cmd="$1"
    local start_time=$(date +%s)
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local log_file="$LOG_DIR/ingestion_${timestamp}.log"
    
    log "INFO" "Starting ingestion process..."
    log "INFO" "Command: $cmd"
    log "INFO" "Log file: $log_file"
    
    # Create log directory if it doesn't exist
    mkdir -p "$(dirname "$log_file")"
    
    # Set timeout (default 1 hour)
    local timeout="${INGESTION_TIMEOUT:-3600}"
    
    # Run the command with timeout
    set +e
    log "INFO" "Executing Spark job..."
    
    # Convert command string to array
    eval "local cmd_array=($cmd)"
    
    # Execute with timeout
    timeout $timeout "${cmd_array[@]}" 2>&1 | tee "$log_file"
    local exit_code=${PIPESTATUS[0]}
    set -e
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Analyze exit code
    case $exit_code in
        0)
            log "INFO" "✅ Ingestion completed successfully in ${duration}s"
            ;;
        124)
            log "ERROR" "❌ Ingestion timed out after ${timeout}s"
            exit_code=1
            ;;
        130)
            log "WARNING" "⚠️  Ingestion interrupted by user"
            exit_code=130
            ;;
        143)
            log "WARNING" "⚠️  Ingestion terminated by signal"
            exit_code=143
            ;;
        *)
            log "ERROR" "❌ Ingestion failed with exit code $exit_code after ${duration}s"
            # Show last 20 lines of log for debugging
            if [ -f "$log_file" ]; then
                log "ERROR" "Last 20 lines of log:"
                tail -20 "$log_file" >&2
            fi
            ;;
    esac
    
    return $exit_code
}

# --- Generate Report ---
generate_report() {
    local exit_code=$1 duration=$2
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local report_file="$REPORT_DIR/ingestion_summary_${timestamp}.json"
    
    mkdir -p "$REPORT_DIR"
    
    # Get current user and hostname
    local current_user=$(whoami 2>/dev/null || echo "unknown")
    local hostname=$(hostname 2>/dev/null || echo "unknown")
    
    cat > "$report_file" << EOF
{
  "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "service": "s3-landing-ingestion",
  "environment": "$ENVIRONMENT",
  "status": "$([ $exit_code -eq 0 ] && echo "SUCCESS" || echo "FAILED")",
  "exit_code": $exit_code,
  "duration_seconds": $duration,
  "s3_endpoint": "$S3_ENDPOINT",
  "s3_bucket": "$WAREHOUSE_BUCKET",
  "source_directory": "$DATA_DIR",
  "config_directory": "$CONFIG_DIR",
  "log_directory": "$LOG_DIR",
  "report_file": "$report_file",
  "hostname": "$hostname",
  "user": "$current_user",
  "spark_master": "$SPARK_MASTER"
}
EOF
    
    log "INFO" "Report generated: $report_file"
    
    # Upload to S3 if successful and if upload is enabled
    if [ $exit_code -eq 0 ] && [ "${UPLOAD_REPORTS_TO_S3:-true}" = "true" ]; then
        upload_to_s3 "$report_file" "landing/reports/$(basename $report_file)"
    fi
}

# --- Upload to S3 ---
upload_to_s3() {
    local file_path="$1"
    local s3_key="$2"
    
    log "INFO" "Uploading to S3: s3://$WAREHOUSE_BUCKET/$s3_key"
    
    python3 -c "
import boto3
from botocore.client import Config
import sys
import os

try:
    # Read file content
    with open('$file_path', 'rb') as f:
        content = f.read()
    
    # Create S3 client
    s3 = boto3.client(
        's3',
        endpoint_url='$S3_ENDPOINT',
        aws_access_key_id='$AWS_ACCESS_KEY_ID',
        aws_secret_access_key='$AWS_SECRET_ACCESS_KEY',
        config=Config(signature_version='s3v4')
    )
    
    # Upload file
    s3.put_object(
        Bucket='$WAREHOUSE_BUCKET',
        Key='$s3_key',
        Body=content,
        ContentType='application/json',
        Metadata={
            'service': 's3-landing-ingestion',
            'environment': '$ENVIRONMENT',
            'timestamp': '$(date -u +"%Y-%m-%dT%H:%M:%SZ")'
        }
    )
    
    print(f'Upload successful: s3://$WAREHOUSE_BUCKET/$s3_key')
    sys.exit(0)
    
except Exception as e:
    print(f'Upload failed: {e}')
    sys.exit(1)
" > /tmp/s3_upload.log 2>&1
    
    if [ $? -eq 0 ]; then
        log "INFO" "✅ Upload successful"
        cat /tmp/s3_upload.log
    else
        log "WARNING" "⚠️  Upload failed:"
        cat /tmp/s3_upload.log >&2
    fi
}

# --- Cleanup Old Logs ---
cleanup_old_logs() {
    local retention_days="${LOG_RETENTION_DAYS:-30}"
    
    log "INFO" "Cleaning up logs older than $retention_days days..."
    
    # Clean logs
    find "$LOG_DIR" -name "*.log" -type f -mtime +$retention_days -delete 2>/dev/null || true
    find "$REPORT_DIR" -name "*.json" -type f -mtime +$retention_days -delete 2>/dev/null || true
    
    # Clean temp files
    find "/tmp" -name "spark-*" -type d -mtime +1 -exec rm -rf {} + 2>/dev/null || true
    find "/tmp" -name "*.log" -type f -mtime +1 -delete 2>/dev/null || true
    
    log "INFO" "Cleanup completed"
}

# --- Setup Environment ---
setup_environment() {
    log "INFO" "Setting up environment..."
    
    # Set Python path
    export PYTHONPATH="/app:$PYTHONPATH"
    export PYSPARK_PYTHON=python3
    export PYSPARK_DRIVER_PYTHON=python3
    
    # Set Spark home
    export SPARK_HOME=/opt/spark
    
    # Create temporary directory for Spark
    export SPARK_LOCAL_DIRS=/tmp/spark-local
    mkdir -p /tmp/spark-local
    chmod 777 /tmp/spark-local 2>/dev/null || true
    
    log "INFO" "✅ Environment setup complete"
}

# --- Main Execution ---
main() {
    log "INFO" "=========================================="
    log "INFO" "S3 LANDING INGESTION - STARTING"
    log "INFO" "Version: $(date +%Y%m%d)"
    log "INFO" "=========================================="
    
    # 1. Load environment variables
    load_env
    
    # 2. Create writable directories
    create_writable_dirs
    
    # 3. Setup environment
    setup_environment
    
    # 4. Validate dependencies
    validate_dependencies
    
    # 5. Wait for MinIO service
    if ! wait_for_service "MinIO" "$MINIO_HOST" "$MINIO_PORT"; then
        log "ERROR" "MinIO service not available, exiting"
        exit 1
    fi
    
    # 6. Test MinIO connection (with fallback to Python)
    if ! test_minio_connection; then
        log "ERROR" "MinIO connection test failed, exiting"
        exit 1
    fi
    
    # 7. Validate S3 bucket
    if ! validate_s3_bucket "$WAREHOUSE_BUCKET"; then
        log "ERROR" "S3 bucket validation failed, exiting"
        exit 1
    fi
    
    # 8. Check for data files (warning only, not fatal)
    if ! check_for_data_files; then
        log "WARNING" "No data files found, but continuing for discovery/validation..."
    fi
    
    # 9. Build Spark command
    local spark_cmd=$(build_spark_command)
    
    # 10. Run ingestion
    local start_time=$(date +%s)
    run_ingestion "$spark_cmd"
    local exit_code=$?
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # 11. Generate report
    generate_report $exit_code $duration
    
    # 12. Cleanup old logs
    cleanup_old_logs
    
    log "INFO" "=========================================="
    log "INFO" "S3 LANDING INGESTION - COMPLETED"
    log "INFO" "=========================================="
    log "INFO" "Total duration: ${duration}s"
    log "INFO" "Exit code: $exit_code"
    log "INFO" "Logs: $LOG_DIR"
    log "INFO" "Reports: $REPORT_DIR"
    
    exit $exit_code
}

# Set error handling
set -o pipefail
set -o errexit

# Execute main function with error trapping
main "$@"