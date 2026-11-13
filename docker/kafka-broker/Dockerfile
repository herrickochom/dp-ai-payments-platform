FROM apache/kafka:4.3.1

# Helper script used by other services / health checks
COPY --chmod=755 platform/kafka/wait-for-kafka.sh /wait-for-kafka.sh

ENV KAFKA_HEAP_OPTS="-Xmx512M"

EXPOSE 9092 9093

# apache/kafka supplies its own entrypoint/CMD