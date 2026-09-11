#!/usr/bin/env python3
"""Focused LIVE test of the RED Kafka invariants against the real local stack.

Proves, using the REAL ``kafka_consumer_events.py`` entrypoint (real Kafka
commit log, real MinIO, real Schema Registry):

    Phase A  poison -> DLQ publication fails -> consumer stops (exit != 0)
             -> source offset of the poison record stays UNCOMMITTED.

    Phase B  DLQ restored -> consumer restarts in the same group
             -> Kafka replays the same source record -> DLQ publication
             succeeds -> source offset commits, then later offsets commit.

Requires the running compose stack.  Endpoints are overridable via env:
    KAFKA_BOOTSTRAP_SERVERS (default localhost:9094)
    SCHEMA_REGISTRY_URL     (default http://localhost:8081)
    S3_ENDPOINT             (default http://localhost:9000)
    MINIO_ROOT_USER/_PASSWORD, MINIO_BUCKET.

Run:  python tests/live_red_invariant_test.py
"""

import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

import boto3
import requests
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

ROOT = Path(__file__).resolve().parents[1]

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
MINIO = os.getenv("S3_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "dp-ai-payment")
DLQ_REAL = os.getenv("KAFKA_DLQ_TOPIC", "payment-events.dlq")

RUN_ID = uuid.uuid4().hex[:8]
SOURCE_TOPIC = f"red-invariant-{RUN_ID}"
GROUP = f"red-group-{RUN_ID}"
FAKE_DLQ = f"red-no-dlq-{RUN_ID}"

SCHEMA = {
    "type": "record",
    "name": "RedInvariantEvent",
    "namespace": "com.test.red",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "message_id", "type": ["null", "string"], "default": None},
        {"name": "timestamp", "type": "string"},
        {"name": "source_system", "type": "string"},
        {"name": "message_type", "type": "string"},
        {"name": "instructed_amount", "type": ["double", "null"], "default": None},
        {"name": "currency", "type": ["string", "null"], "default": None},
        {"name": "event_data", "type": ["null", "string"], "default": None},
        {"name": "parsed_event_data", "type": ["null", "string"], "default": None},
        {"name": "x_attributes", "type": ["null", {"type": "map", "values": "string"}], "default": None},
    ],
}

FAILURES = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {name} {detail}")
    if not condition:
        FAILURES.append(name)


def preflight():
    try:
        requests.get(f"{SR_URL}/subjects", timeout=10).raise_for_status()
    except Exception as exc:  # noqa: BLE001 - live preflight
        print(f"⚠  Schema Registry unreachable ({exc}) - is the stack running?", file=sys.stderr)
        return False
    try:
        AdminClient({"bootstrap.servers": KAFKA}).list_topics(timeout=10)
    except Exception as exc:  # noqa: BLE001
        print(f"⚠  Kafka unreachable ({exc}) - is the stack running?", file=sys.stderr)
        return False
    try:
        s3 = boto3.client("s3", endpoint_url=MINIO, aws_access_key_id=MINIO_USER,
                          aws_secret_access_key=MINIO_PASS, region_name="us-east-1")
        s3.head_bucket(Bucket=BUCKET)
    except Exception as exc:  # noqa: BLE001
        print(f"⚠  MinIO bucket {BUCKET!r} unreachable ({exc}) - is the stack running?",
              file=sys.stderr)
        return False
    return True


def create_topic(admin, name):
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])
    for _ in range(30):
        time.sleep(1)
        try:
            if name in admin.list_topics(timeout=5).topics:
                return
        except Exception:  # noqa: BLE001
            pass
    raise RuntimeError(f"topic {name} not created in time")


def produce_records():
    schema_registry = SchemaRegistryClient({"url": SR_URL})
    serializer = AvroSerializer(schema_registry, json.dumps(SCHEMA),
                                conf={"auto.register.schemas": True})
    producer = Producer({"bootstrap.servers": KAFKA, "acks": "all", "enable.idempotence": True})

    def event(event_id, amount):
        return {
            "event_id": event_id, "message_id": None, "timestamp": "2026-09-11T00:00:00Z",
            "source_system": "red-test", "message_type": "red-test",
            "instructed_amount": amount, "currency": "UGX", "event_data": None,
            "parsed_event_data": None, "x_attributes": {},
        }

    def callback(err, _msg):
        if err is not None:
            raise RuntimeError(f"produce failed: {err}")

    ctx = SerializationContext(SOURCE_TOPIC, MessageField.VALUE)
    # offset 0 - valid (stored + committed in Phase A).
    producer.produce(SOURCE_TOPIC, key=b"red-key-0",
                     value=serializer(event("red-valid-0", 10.0), ctx), callback=callback)
    # offset 1 - poison (garbage bytes that cannot be Avro-deserialized).
    producer.produce(SOURCE_TOPIC, key=b"red-key-1",
                     value=b"\xca\xfe\xba\xbe-poison\x00\xff", callback=callback)
    # offset 2 - valid (must NOT be processed in Phase A; processed in Phase B).
    producer.produce(SOURCE_TOPIC, key=b"red-key-2",
                     value=serializer(event("red-valid-2", 20.0), ctx), callback=callback)
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} record(s) not produced")


def consumer_env(dlq_topic):
    env = dict(os.environ)
    env.update({
        "PYTHONUNBUFFERED": "1",
        "KAFKA_BOOTSTRAP_SERVERS": f"PLAINTEXT://{KAFKA}",
        "KAFKA_TOPICS": SOURCE_TOPIC,
        "KAFKA_GROUP_ID": GROUP,
        "KAFKA_DLQ_TOPIC": dlq_topic,
        "KAFKA_RETRY_TOPIC": "payment-events.retry",
        "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT",
        "MAX_PROCESSING_RETRIES": "0",
        "S3_ENDPOINT": MINIO,
        "MINIO_ROOT_USER": MINIO_USER,
        "MINIO_ROOT_PASSWORD": MINIO_PASS,
        "MINIO_BUCKET": BUCKET,
        "RAW_PREFIX": "raw/v2",
        "SCHEMA_REGISTRY_URL": SR_URL,
    })
    return env


def committed_offset():
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": GROUP,
                  "enable.auto.commit": False, "security.protocol": "PLAINTEXT"})
    try:
        result = c.committed([TopicPartition(SOURCE_TOPIC, 0)], timeout=10)
        return result[0].offset if result else -1
    finally:
        c.close()


def run_phase(dlq_topic, idle_timeout_s):
    """Run the real consumer entrypoint as a subprocess until it exits (fail-stop)
    or until `idle_timeout_s` consecutive quiet seconds pass."""
    script = ROOT / "services" / "kafka-consumer-events" / "kafka_consumer_events.py"
    proc = subprocess.Popen([sys.executable, str(script)],
                            env=consumer_env(dlq_topic),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    lines = []

    def _read():
        for raw in iter(proc.stdout.readline, ""):
            lines.append(raw.rstrip())

    threading.Thread(target=_read, daemon=True).start()
    prev_len = 0
    quiet_since = time.monotonic()
    while proc.poll() is None:
        if len(lines) != prev_len:
            prev_len = len(lines)
            quiet_since = time.monotonic()
        elif time.monotonic() - quiet_since >= idle_timeout_s:
            break
        time.sleep(0.5)
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    return proc, lines


def drain_dlq_failure_id():
    """Return {failure_id: key} for DLQ envelopes referencing our source topic."""
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"red-dlq-reader-{RUN_ID}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest",
                  "security.protocol": "PLAINTEXT"})
    found = {}
    try:
        metadata = c.list_topics(DLQ_REAL, timeout=10)
        partitions = list(metadata.topics[DLQ_REAL].partitions.keys())
        c.assign([TopicPartition(DLQ_REAL, p) for p in partitions])
        deadline = time.monotonic() + 30
        eof = set()
        while time.monotonic() < deadline:
            msg = c.poll(1.0)
            if msg is None:
                if len(eof) == len(partitions):
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof.add(msg.partition())
                continue
            try:
                envelope = json.loads(msg.value())
            except (TypeError, json.JSONDecodeError):
                continue
            if envelope.get("original_topic") == SOURCE_TOPIC:
                fid = envelope.get("failure_id")
                raw_key = msg.key()
                found[fid] = raw_key.decode("utf-8", errors="replace") if raw_key else None
        return found
    finally:
        c.close()


def main() -> int:
    print("=" * 78)
    print("LIVE RED invariant test against the real local stack")
    print(f"  kafka={KAFKA}  SR={SR_URL}  minio={MINIO}")
    print(f"  source topic={SOURCE_TOPIC}  group={GROUP}")
    print("=" * 78)

    if not preflight():
        print("❌ Live test cannot run: stack endpoint(s) unreachable. "
              "Start `docker compose up` first (or override the *_ENDPOINT env vars).",
              file=sys.stderr)
        return 2

    print("Creating topic and producing offset0=valid, offset1=poison, offset2=valid ...\n")
    admin = AdminClient({"bootstrap.servers": KAFKA})
    create_topic(admin, SOURCE_TOPIC)
    produce_records()

    # ------------------------------------------------------------------ Phase A
    print(f"--- Phase A: DLQ topic = {FAKE_DLQ!r} (does not exist; auto-create disabled) ---")
    before = committed_offset()
    proc, lines = run_phase(FAKE_DLQ, idle_timeout_s=40)
    exit_code = proc.returncode
    for line in lines[-8:]:
        print(f"  consumer> {line}")
    after = committed_offset()

    check("consumer fails-stop with non-zero exit code", exit_code != 0,
          f"(exit_code={exit_code})")
    check("after fail-stop only offset0 is committed (poison record and offset2 are NOT)",
          after == 1, f"(before={before} after={after}; expected after=1)")

    # ------------------------------------------------------------------ Phase B
    print(f"\n--- Phase B: DLQ restored to {DLQ_REAL!r}, same consumer group restarts ---")
    proc_b, lines_b = run_phase(DLQ_REAL, idle_timeout_s=25)
    for line in lines_b[-8:]:
        print(f"  consumerB> {line}")

    final = committed_offset()
    deadline = time.monotonic() + 90
    while final < 3 and time.monotonic() < deadline:
        final = committed_offset()
        if final < 3:
            time.sleep(2)
    if proc_b.poll() is None:
        proc_b.send_signal(signal.SIGTERM)
        try:
            proc_b.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc_b.kill()
            proc_b.wait(timeout=5)

    check("all three source offsets committed after DLQ restored",
          final == 3, f"(committed={final}; expected 3)")

    expected_fid = f"{SOURCE_TOPIC}:0:1"
    dlq_records = drain_dlq_failure_id()
    check(f"exactly the poison failure_id {expected_fid!r} appears in the DLQ",
          dlq_records == {expected_fid: expected_fid},
          f"(found={dlq_records})")

    print()
    print("=" * 78)
    if FAILURES:
        print(f"❌ LIVE RED invariant test FAILED: {len(FAILURES)} check(s)")
        for item in FAILURES:
            print(f"  - {item}")
        print("=" * 78)
        return 1
    print("✅ LIVE RED invariant test passed: fail-stop keeps the poison offset "
          "uncommitted, restart replays it, and only then does the source offset "
          "advance (at-least-once semantics).")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())