#!/bin/bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}"
MAX_RETRIES=30
RETRY_INTERVAL=3

# Find kafka-topics command
if command -v kafka-topics >/dev/null 2>&1; then
    KAFKA_CMD="kafka-topics"
elif command -v kafka-topics.sh >/dev/null 2>&1; then
    KAFKA_CMD="kafka-topics.sh"
elif [ -f "/usr/bin/kafka-topics" ]; then
    KAFKA_CMD="/usr/bin/kafka-topics"
elif [ -f "/usr/bin/kafka-topics.sh" ]; then
    KAFKA_CMD="/usr/bin/kafka-topics.sh"
elif [ -f "/opt/kafka/bin/kafka-topics.sh" ]; then
    KAFKA_CMD="/opt/kafka/bin/kafka-topics.sh"
else
    echo "❌ kafka-topics command not found!"
    exit 1
fi

echo "=========================================="
echo "Kafka Topic Initialization - PDM Platform"
echo "=========================================="
echo "Bootstrap Server: $BOOTSTRAP_SERVER"
echo "Kafka Command: $KAFKA_CMD"
echo "=========================================="

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
for i in $(seq 1 $MAX_RETRIES); do
    if $KAFKA_CMD --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; then
        echo "✅ Kafka is ready!"
        break
    fi
    echo "Attempt $i/$MAX_RETRIES: Kafka not ready yet..."
    sleep $RETRY_INTERVAL
    if [ $i -eq $MAX_RETRIES ]; then
        echo "❌ Kafka not ready after $MAX_RETRIES attempts"
        exit 1
    fi
done

# Function to create topic
create_topic() {
    local name="$1"
    local partitions="${2:-6}"
    local replication="${3:-1}"
    local retention_ms="${4:-604800000}"

    echo ""
    echo "Checking topic: $name"
    
    if $KAFKA_CMD --bootstrap-server "$BOOTSTRAP_SERVER" --list | grep -q "^$name$"; then
        echo "  ⏭️  Topic already exists: $name"
        return 0
    fi

    echo "  Creating topic: $name (partitions=$partitions, replication=$replication)"
    $KAFKA_CMD \
        --bootstrap-server "$BOOTSTRAP_SERVER" \
        --create \
        --topic "$name" \
        --partitions "$partitions" \
        --replication-factor "$replication" \
        --config "retention.ms=$retention_ms" \
        --config "cleanup.policy=delete"
    
    echo "  ✅ Topic created: $name"
}

echo ""
echo "=========================================="
echo "1. ICMN Topics (Interbank Messaging)"
echo "=========================================="

create_topic "icmn.vpm.pain001" 6 1
create_topic "icmn.pmn.pain001" 6 1

echo ""
echo "=========================================="
echo "2. CPO Topics (Cloud Payment Orchestrator)"
echo "=========================================="

create_topic "cpo.psn.pain002" 6 1   # ✅ Correct - PSN = pain.002
create_topic "cpo.plm.pain002" 6 1   # ✅ Correct - PLM = pain.002

echo ""
echo "=========================================="
echo "3. Wendi Topics (PostBank Digital Wallet)"
echo "=========================================="

create_topic "wendi.camt053" 4 1 2592000000
create_topic "wendi.camt052" 4 1 86400000
create_topic "wendi.camt054" 8 1 604800000
create_topic "wendi.transactions" 8 1

echo ""
echo "=========================================="
echo "4. Mobile Networks Topics"
echo "=========================================="

create_topic "mobile.mtn.pacs008" 6 1
create_topic "mobile.mtn.pacs002" 4 1
create_topic "mobile.airtel.pacs008" 6 1
create_topic "mobile.airtel.pacs002" 4 1

echo ""
echo "=========================================="
echo "5. Agent Network Topics"
echo "=========================================="

create_topic "agent.transactions" 8 1 2592000000
create_topic "agent.profiles" 4 1
create_topic "agent.locations" 4 1

echo ""
echo "=========================================="
echo "6. PDMIS Topics (Government System)"
echo "=========================================="

create_topic "pdmis.beneficiaries" 8 1
create_topic "pdmis.loans" 8 1
create_topic "pdmis.saccos" 4 1
create_topic "pdmis.households" 4 1
create_topic "pdmis.business_plans" 4 1

echo ""
echo "=========================================="
echo "7. Reconciliation Topics"
echo "=========================================="

create_topic "reconciliation.links" 8 1 2592000000
create_topic "reconciliation.status" 8 1

echo ""
echo "=========================================="
echo "✅ All topics created/verified!"
echo "=========================================="
echo ""
echo "📊 Summary by System:"
echo "  ICMN  : 2 topics (vpm.pain001, pmn.pain001)"
echo "  CPO   : 2 topics (psn.pain002, plm.pain002)"
echo "  Wendi : 4 topics"
echo "  Mobile: 4 topics"
echo "  Agent : 3 topics"
echo "  PDMIS : 5 topics"
echo "  Recon : 2 topics"
echo "  TOTAL : 22 topics"
echo ""
echo "Available topics:"
$KAFKA_CMD --bootstrap-server "$BOOTSTRAP_SERVER" --list | sort
echo "=========================================="