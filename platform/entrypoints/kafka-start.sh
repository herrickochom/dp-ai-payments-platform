#!/bin/bash
set -e

# Ensure Kafka broker directories exist
mkdir -p /kafka/broker/logs
mkdir -p /kafka/connect
mkdir -p /kafka/tmp

echo "📌 Starting Kafka..."
exec /etc/confluent/docker/run
