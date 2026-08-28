#!/bin/bash
# =====================================================
# Landing → Kafka Ingestion Entrypoint
# =====================================================
# Orchestrates: FS Landing → Kafka → Contract Enforcer → Preprocessor
# Logs all output to file + stdout
# =====================================================

set -Eeuo pipefail

# -------------------------
# Paths & Config
# -------------------------
SCRIPT_PATH="${SCRIPT_PATH:-/app/scripts/landing_to_kafka/orchestration/main.py}"
LOG_DIR="${LOG_DIR:-/app/logs/landing_to_kafka}"
REPORT_DIR="${REPORT_DIR:-/app/reports/landing_to_kafka}"

# Default FS landing directory for pipeline
FS_LANDING_DIR="${FS_LANDING_DIR:-/app/data/landing/incoming}"

# Logging helper
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

# -------------------------
# Initialize container
# -------------------------
log "🔹 Initializing container user: $(id)"

# Validate main.py exists
if [[ ! -f "$SCRIPT_PATH" ]]; then
    log "❌ ERROR: main.py not found at $SCRIPT_PATH"
    exit 1
fi

# Create logs and reports directories (volume may override permissions)
mkdir -p "$LOG_DIR" "$REPORT_DIR" || true

# Fix permissions (best-effort)
chmod -R 777 "$LOG_DIR" "$REPORT_DIR" || true

# Test write access for logging
if ! touch "$LOG_DIR/.write_test" 2>/dev/null; then
    log "⚠ WARNING: Cannot write to $LOG_DIR, disabling tee logging."
    USE_TEE=false
else
    rm -f "$LOG_DIR/.write_test"
    USE_TEE=true
fi

# -------------------------
# Environment Info
# -------------------------
log "📁 FS Landing Directory: $FS_LANDING_DIR"
log "📝 Logs Directory: $LOG_DIR"
log "📊 Reports Directory: $REPORT_DIR"
log "🐍 Using Python: $(python3 --version)"
log "🚀 Starting Landing → Kafka → Contract Enforcer → Preprocessor pipeline..."

# -------------------------
# Run pipeline
# -------------------------
LOG_FILE="$LOG_DIR/main_pipeline_$(date +%Y%m%d_%H%M%S).log"

if [[ "$USE_TEE" == "true" ]]; then
    python3 "$SCRIPT_PATH" 2>&1 | tee "$LOG_FILE"
    PIPE_RC=${PIPESTATUS[0]}
else
    python3 "$SCRIPT_PATH"
    PIPE_RC=$?
fi

# -------------------------
# Exit Status Logging
# -------------------------
if [[ $PIPE_RC -eq 0 ]]; then
    log "✅ Pipeline finished successfully"
else
    log "❌ Pipeline failed with exit code $PIPE_RC"
fi

exit $PIPE_RC
