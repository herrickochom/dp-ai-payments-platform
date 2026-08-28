#!/bin/bash

# ====================================================================================
# Nessie Namespace Initialization Script
# ====================================================================================

set -e

echo "[NESSIE INIT] Starting Nessie namespace initialization"

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
NESSIE_URL="${SPARK_SQL_NESSIE_URI:-http://localhost:19120/api/v1}"
DEFAULT_BRANCH="${NESSIE_DEFAULT_BRANCH:-main}"
NAMESPACES=(
    "landing"
    "staging" 
    "bronze"
    "silver"
    "gold"
    "data_vault"
    "iceberg"
    "hudi"
)

# ------------------------------------------------------------
# Wait for Nessie API to be ready
# ------------------------------------------------------------
echo "[NESSIE INIT] Checking Nessie API availability at ${NESSIE_URL}"

MAX_RETRIES=10
RETRY_INTERVAL=3

for i in $(seq 1 ${MAX_RETRIES}); do
    if curl -s -f "${NESSIE_URL}/config" > /dev/null 2>&1; then
        echo "✔ Nessie API is available"
        break
    fi
    
    if [ ${i} -eq ${MAX_RETRIES} ]; then
        echo "❌ ERROR: Nessie API not available after ${MAX_RETRIES} attempts"
        exit 1
    fi
    
    echo "⏳ Nessie API not ready (attempt ${i}/${MAX_RETRIES}) — retrying in ${RETRY_INTERVAL}s"
    sleep ${RETRY_INTERVAL}
done

# ------------------------------------------------------------
# Create default branch if it doesn't exist
# ------------------------------------------------------------
echo "[NESSIE INIT] Ensuring default branch '${DEFAULT_BRANCH}' exists"

if ! curl -s -f "${NESSIE_URL}/trees/${DEFAULT_BRANCH}" > /dev/null 2>&1; then
    echo "  - Creating default branch '${DEFAULT_BRANCH}'"
    curl -X POST "${NESSIE_URL}/trees" \
        -H "Content-Type: application/json" \
        -d "{\"ref\": \"${DEFAULT_BRANCH}\", \"type\": \"BRANCH\"}" \
        --fail --silent --show-error
    
    if [ $? -eq 0 ]; then
        echo "✔ Default branch '${DEFAULT_BRANCH}' created"
    else
        echo "⚠ Warning: Failed to create default branch"
    fi
else
    echo "✔ Default branch '${DEFAULT_BRANCH}' already exists"
fi

# ------------------------------------------------------------
# Create namespaces
# ------------------------------------------------------------
echo "[NESSIE INIT] Creating namespaces"

for NAMESPACE in "${NAMESPACES[@]}"; do
    echo "  - Processing namespace: ${NAMESPACE}"
    
    # Check if namespace exists
    if curl -s -f "${NESSIE_URL}/trees/${DEFAULT_BRANCH}/namespaces/${NAMESPACE}" > /dev/null 2>&1; then
        echo "    ✓ Namespace '${NAMESPACE}' already exists"
        continue
    fi
    
    # Create namespace
    echo "    + Creating namespace '${NAMESPACE}'"
    curl -X POST "${NESSIE_URL}/trees/${DEFAULT_BRANCH}/namespaces" \
        -H "Content-Type: application/json" \
        -d "{\"name\": {\"elements\": [\"${NAMESPACE}\"]}}" \
        --fail --silent --show-error
    
    if [ $? -eq 0 ]; then
        echo "    ✓ Namespace '${NAMESPACE}' created"
    else
        echo "    ⚠ Warning: Failed to create namespace '${NAMESPACE}'"
    fi
done

# ------------------------------------------------------------
# Create Iceberg catalog namespace
# ------------------------------------------------------------
echo "[NESSIE INIT] Setting up Iceberg catalog"

ICEBERG_NAMESPACE="iceberg"
if ! curl -s -f "${NESSIE_URL}/trees/${DEFAULT_BRANCH}/namespaces/${ICEBERG_NAMESPACE}" > /dev/null 2>&1; then
    echo "  - Creating Iceberg catalog namespace"
    curl -X POST "${NESSIE_URL}/trees/${DEFAULT_BRANCH}/namespaces" \
        -H "Content-Type: application/json" \
        -d "{\"name\": {\"elements\": [\"${ICEBERG_NAMESPACE}\"]}}" \
        --fail --silent --show-error
fi

# ------------------------------------------------------------
# Final validation
# ------------------------------------------------------------
echo "[NESSIE INIT] Validating namespaces"

VALIDATION_PASSED=true
for NAMESPACE in "${NAMESPACES[@]}"; do
    if curl -s -f "${NESSIE_URL}/trees/${DEFAULT_BRANCH}/namespaces/${NAMESPACE}" > /dev/null 2>&1; then
        echo "  ✓ ${NAMESPACE}: OK"
    else
        echo "  ✗ ${NAMESPACE}: MISSING"
        VALIDATION_PASSED=false
    fi
done

if [[ "${VALIDATION_PASSED}" == "true" ]]; then
    echo "=========================================================="
    echo "[NESSIE INIT] SUCCESS: All namespaces initialized"
    echo "  - Default Branch: ${DEFAULT_BRANCH}"
    echo "  - Namespaces: ${NAMESPACES[*]}"
    echo "  - Nessie URL: ${NESSIE_URL}"
    echo "=========================================================="
else
    echo "=========================================================="
    echo "[NESSIE INIT] WARNING: Some namespaces failed to initialize"
    echo "=========================================================="
fi