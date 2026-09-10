#!/usr/bin/env bash
# Standard Kafka view: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG.
set -Eeuo pipefail
bootstrap="${KAFKA_BOOTSTRAP_SERVER:-kafka:9092}"
group="${KAFKA_GROUP_ID:-payment-events-consumer}"
exec kafka-consumer-groups --bootstrap-server "$bootstrap" --group "$group" --describe
