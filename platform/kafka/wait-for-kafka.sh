# infrastructure/kafka/wait-for-kafka.sh
#!/bin/bash
# ============================================================================
# Wait for Kafka to be ready
# ============================================================================

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}"
MAX_RETRIES=30
RETRY_INTERVAL=2

echo "Waiting for Kafka at $BOOTSTRAP_SERVER..."

for i in $(seq 1 $MAX_RETRIES); do
    if kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVER" --list >/dev/null 2>&1; then
        echo "✅ Kafka is ready!"
        exit 0
    fi
    echo "Attempt $i/$MAX_RETRIES: Kafka not ready yet..."
    sleep $RETRY_INTERVAL
done

echo "❌ Kafka not ready after $MAX_RETRIES attempts"
exit 1
