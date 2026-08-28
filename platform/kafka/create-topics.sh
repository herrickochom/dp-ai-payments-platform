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
    echo "Searching for it..."
    find / -name "kafka-topics*" 2>/dev/null || echo "Not found"
    exit 1
fi

echo "=========================================="
echo "Kafka Topic Initialization"
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
    local partitions="$2"
    local replication="$3"

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
        --config "retention.ms=604800000" \
        --config "cleanup.policy=delete"
    
    echo "  ✅ Topic created: $name"
}

# Create all 8 topics
create_topic "icmn.vpm.pain001" 6 1
create_topic "icmn.vpm.pain002" 6 1
create_topic "icmn.pmn.pain001" 6 1
create_topic "icmn.pmn.pain002" 6 1
create_topic "cpo.psn.pain001" 6 1
create_topic "cpo.psn.pain002" 6 1
create_topic "cpo.plm.pain001" 6 1
create_topic "cpo.plm.pain002" 6 1

echo ""
echo "=========================================="
echo "All topics created/verified!"
echo "=========================================="
echo ""
echo "Available topics:"
$KAFKA_CMD --bootstrap-server "$BOOTSTRAP_SERVER" --list | grep -E "icmn|cpo"

echo "=========================================="
