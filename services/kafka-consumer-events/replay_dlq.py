#!/usr/bin/env python3
"""Deliberately replay one Kafka DLQ record by exact partition and offset."""
import argparse
import base64
import json
import os
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, TopicPartition


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--partition", required=True, type=int)
    result.add_argument("--offset", required=True, type=int)
    result.add_argument("--reason", required=True, help="Auditable operator reason")
    result.add_argument("--execute", action="store_true", help="Publish; otherwise only inspect")
    return result


def main() -> int:
    args = parser().parse_args()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "payment-events.dlq")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "dlq-replay-inspector",
                         "enable.auto.commit": False, "security.protocol": os.getenv(
                             "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")})
    consumer.assign([TopicPartition(dlq_topic, args.partition, args.offset)])
    message = consumer.poll(10)
    consumer.close()
    if message is None or message.error() or message.offset() != args.offset:
        print("Requested DLQ record was not found", file=sys.stderr)
        return 2
    envelope = json.loads(message.value())
    summary = {key: envelope.get(key) for key in (
        "original_topic", "original_partition", "original_offset", "original_event_id",
        "business_key", "error_type", "failure_reason", "retry_count")}
    print(json.dumps(summary, indent=2))
    if not args.execute:
        print("Inspection only. Re-run with --execute to publish this one record.")
        return 0

    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True,
                         "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")})
    delivery = {"error": None}
    producer.produce(
        envelope["original_topic"],
        key=base64.b64decode(envelope["original_key_base64"]),
        value=base64.b64decode(envelope["original_payload_base64"]),
        headers={"x-dlq-replay": "true", "x-dlq-replay-reason": args.reason,
                 "x-dlq-replay-at": datetime.now(timezone.utc).isoformat()},
        callback=lambda error, _message: delivery.update(error=error),
    )
    remaining = producer.flush(30)
    if remaining or delivery["error"]:
        print(f"Replay delivery failed: {delivery['error']}", file=sys.stderr)
        return 1
    print("Replay delivered. The DLQ record remains as an immutable audit record.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
