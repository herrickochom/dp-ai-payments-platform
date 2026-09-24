#!/usr/bin/env python3
"""Deliberately replay one Kafka DLQ record by exact partition and offset.

The DLQ record's envelope carries the deterministic failure identity
``failure_id = <original_topic>:<original_partition>:<original_offset>``.

Duplicate-safe replay:
---------------------
A source record whose DLQ publication was acknowledged but whose source commit
was lost (consumer crash between DLQ ack and commit) is replayed by Kafka and can
produce a second DLQ envelope with the *same* ``failure_id``.  This tool scans the
DLQ topic, groups envelopes by ``failure_id``, and refuses ``--execute`` on an
identity that has duplicate envelopes unless ``--allow-duplicate-replay`` is
passed, so the same source record cannot be re-injected repeatedly by accident.

This is duplicate-safe replay, NOT physical exactly-once publication: on a
``cleanup.policy=delete`` topic Kafka can still hold two records with the same
deterministic key.  The deterministic identity makes those duplicate envelopes
identifiable and the replay operation idempotent under that identity.
"""
import argparse
import base64
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from services.shared.security.runtime_security import validate_kafka_security
from services.shared.security.secret_provider import resolve_secret

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition



def kafka_security_config() -> Dict[str, str]:
    """Build and validate the Kafka security configuration."""
    security_protocol = os.getenv(
        "KAFKA_SECURITY_PROTOCOL",
        "PLAINTEXT",
    )
    ssl_ca_location = os.getenv("KAFKA_SSL_CA_LOCATION")
    sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    sasl_username = os.getenv("KAFKA_SASL_USERNAME")
    sasl_password = resolve_secret("KAFKA_SASL_PASSWORD")

    validate_kafka_security(
        security_protocol=security_protocol,
        ssl_ca_location=ssl_ca_location,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
    )

    config = {
        "security.protocol": security_protocol,
    }

    optional = {
        "ssl.ca.location": ssl_ca_location,
        "ssl.certificate.location": os.getenv(
            "KAFKA_SSL_CERTIFICATE_LOCATION"
        ),
        "ssl.key.location": os.getenv(
            "KAFKA_SSL_KEY_LOCATION"
        ),
        "sasl.mechanism": sasl_mechanism,
        "sasl.username": sasl_username,
        "sasl.password": sasl_password,
    }

    config.update(
        {
            key: value
            for key, value in optional.items()
            if value
        }
    )

    return config

def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--partition", required=True, type=int)
    result.add_argument("--offset", required=True, type=int)
    result.add_argument("--reason", required=True, help="Auditable operator reason")
    result.add_argument("--execute", action="store_true", help="Publish; otherwise only inspect")
    result.add_argument("--allow-duplicate-replay", action="store_true",
                        help="Explicitly accept a duplicate source replay when the same "
                             "failure identity has more than one DLQ envelope")
    return result


def failure_id_from_envelope(envelope: Dict) -> str:
    """Deterministic failure identity of a DLQ envelope.

    Prefers the explicit ``failure_id`` and reconstructs the same value for legacy
    envelopes that predate the identity field, so replay deduplication is uniform.
    """
    failure_id = envelope.get("failure_id")
    if failure_id:
        return str(failure_id)
    return f"{envelope.get('original_topic')}:{envelope.get('original_partition')}:{envelope.get('original_offset')}"


def group_by_failure_id(records: List[Tuple[int, int, Dict]]) -> Dict[str, List[Tuple[int, int, Dict]]]:
    """Group ``(dlq_partition, dlq_offset, envelope)`` records by failure identity."""
    groups: Dict[str, List[Tuple[int, int, Dict]]] = {}
    for record in records:
        dlq_partition, dlq_offset, envelope = record
        groups.setdefault(failure_id_from_envelope(envelope), []).append(
            (dlq_partition, dlq_offset, envelope))
    return groups


def duplicate_groups(records: List[Tuple[int, int, Dict]]) -> Dict[str, List[Tuple[int, int, Dict]]]:
    """Return only the failure identities that have more than one DLQ envelope."""
    return {fid: recs for fid, recs in group_by_failure_id(records).items() if len(recs) > 1}


def scan_dlq_envelopes(bootstrap: str, dlq_topic: str,
                       timeout_s: float = 20.0) -> List[Tuple[int, int, Dict]]:
    """Consume every DLQ envelope across all partitions (bounded by timeout).

    The DLQ is keyed by ``failure_id`` since the consumer fix, so duplicates share
    a partition; scanning every partition also catches legacy business-key-keyed
    duplicates.  A unique group id makes each scan read from ``earliest``.
    """
    config = {
        "bootstrap.servers": bootstrap,
        "group.id": f"dlq-replay-inspector-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    config.update(kafka_security_config())
    consumer = Consumer(config)
    try:
        try:
            metadata = consumer.list_topics(dlq_topic, timeout=10)
            partitions = sorted(metadata.topics[dlq_topic].partitions.keys())
        except Exception as exc:
            print(f"DLQ topic {dlq_topic!r} is not available: {exc}", file=sys.stderr)
            return []
        consumer.assign([TopicPartition(dlq_topic, partition) for partition in partitions])
        records: List[Tuple[int, int, Dict]] = []
        end_of_partition: set = set()
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                if len(end_of_partition) == len(partitions):
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    end_of_partition.add(message.partition())
                continue
            try:
                envelope = json.loads(message.value())
            except (TypeError, json.JSONDecodeError):
                continue
            records.append((message.partition(), message.offset(), envelope))
        return records
    finally:
        consumer.close()


def main() -> int:
    args = parser().parse_args()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "payment-events.dlq")
    consumer_config = {
        "bootstrap.servers": bootstrap,
        "group.id": "dlq-replay-inspector",
        "enable.auto.commit": False,
    }
    consumer_config.update(kafka_security_config())
    consumer = Consumer(consumer_config)
    consumer.assign([TopicPartition(dlq_topic, args.partition, args.offset)])
    message = consumer.poll(10)
    consumer.close()
    if message is None or message.error() or message.offset() != args.offset:
        print("Requested DLQ record was not found", file=sys.stderr)
        return 2
    envelope = json.loads(message.value())
    failure_id = failure_id_from_envelope(envelope)
    summary = {key: envelope.get(key) for key in (
        "failure_id", "original_topic", "original_partition", "original_offset",
        "original_event_id", "business_key", "error_type", "failure_reason", "retry_count")}
    print(json.dumps(summary, indent=2))

    # Duplicate detection: every DLQ envelope sharing this source-record identity.
    duplicates = duplicate_groups(scan_dlq_envelopes(bootstrap, dlq_topic)).get(failure_id, [])
    if duplicates:
        print(f"⚠️  Duplicate DLQ envelopes exist for failure identity {failure_id!r} "
              "(the same source record was DLQ'd more than once, e.g. after a crash "
              "between DLQ ack and source commit):")
        for dlq_partition, dlq_offset, _dup in duplicates:
            print(f"  - DLQ partition={dlq_partition} offset={dlq_offset}")

    if not args.execute:
        print("Inspection only. Re-run with --execute to publish this one record.")
        return 0

    if duplicates and not args.allow_duplicate_replay:
        print(f"❌ Refusing to replay failure {failure_id!r}: duplicate DLQ envelopes exist.",
              file=sys.stderr)
        print("   Replaying the same source record more than once would inject duplicate",
              file=sys.stderr)
        print("   source records downstream. Inspect the duplicates listed above and re-run",
              file=sys.stderr)
        print("   with --allow-duplicate-replay only if you explicitly accept a duplicate",
              file=sys.stderr)
        print("   source replay.", file=sys.stderr)
        return 3

    producer_config = {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
    }
    producer_config.update(kafka_security_config())
    producer = Producer(producer_config)
    delivery = {"error": None}
    producer.produce(
        envelope["original_topic"],
        key=base64.b64decode(envelope["original_key_base64"]),
        value=base64.b64decode(envelope["original_payload_base64"]),
        headers={"x-dlq-replay": "true", "x-dlq-replay-reason": args.reason,
                 "x-dlq-replay-at": datetime.now(timezone.utc).isoformat(),
                 "x-dlq-replay-failure-id": failure_id},
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
