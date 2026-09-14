#!/usr/bin/env python3
"""
Payment Events Consumer - Reads source-domain Kafka topics and stores immutable
raw events in MinIO/S3 with date partitioning in Avro format.

Consumes the 24 source-ingestion topics for ICMN, CPO, Wendi, Mobile Networks,
Agent Network and PDMIS. Reconciliation topics are downstream-derived and excluded.
"""

import os
import sys
import json
import uuid
import io
import logging
import base64
import time
from datetime import datetime
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaError, Producer
from botocore.exceptions import ClientError, BotoCoreError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import boto3
from botocore.client import Config as BotoConfig
import pytz

# Avro imports
import avro.schema
from avro.io import DatumWriter
from avro.datafile import DataFileWriter

from avro.errors import AvroException

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Settings
# ------------------------------------------------------------------------------
class Settings:
    # MinIO
    MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "dp-ai-payment")
    RAW_PREFIX = os.getenv("RAW_PREFIX", "raw/v2").strip("/")
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "").split(",") if os.getenv("KAFKA_TOPICS") else [
        # ICMN
        "icmn.vpm.pain001",
        "icmn.pmn.pain001",
        # CPO
        "cpo.psn.pain002",
        "cpo.plm.pain002",
        # Wendi
        "wendi.camt052",
        "wendi.camt053",
        "wendi.camt054",
        "wendi.transactions",
        "wendi.pain001",
        "wendi.pain002",
        # Mobile Networks
        "mobile.mtn.pacs008",
        "mobile.mtn.pacs002",
        "mobile.airtel.pacs008",
        "mobile.airtel.pacs002",
        # Agent Network
        "agent.transactions",
        "agent.profiles",
        "agent.locations",
        # PDMIS
        "pdmis.beneficiaries",
        "pdmis.business_plans",
        "pdmis.households",
        "pdmis.loans",
        "pdmis.repayments",
        "pdmis.saccos",
        "pdmis.special_groups",
    ]
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "payment-events-consumer")
    KAFKA_RETRY_TOPIC = os.getenv("KAFKA_RETRY_TOPIC", "payment-events.retry")
    KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "payment-events.dlq")
    MAX_PROCESSING_RETRIES = int(os.getenv("MAX_PROCESSING_RETRIES", "3"))
    RETRY_INITIAL_DELAY_SECONDS = float(os.getenv("RETRY_INITIAL_DELAY_SECONDS", "1"))
    RETRY_BACKOFF_MULTIPLIER = float(os.getenv("RETRY_BACKOFF_MULTIPLIER", "2"))
    RETRY_MAX_DELAY_SECONDS = float(os.getenv("RETRY_MAX_DELAY_SECONDS", "30"))
    KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    KAFKA_SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM")
    KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME")
    KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD")
    KAFKA_SSL_CA_LOCATION = os.getenv("KAFKA_SSL_CA_LOCATION")
    KAFKA_SSL_CERTIFICATE_LOCATION = os.getenv("KAFKA_SSL_CERTIFICATE_LOCATION")
    KAFKA_SSL_KEY_LOCATION = os.getenv("KAFKA_SSL_KEY_LOCATION")
    
    # Schema Registry
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO = os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO")


def kafka_client_config() -> Dict[str, Any]:
    result: Dict[str, Any] = {"security.protocol": Settings.KAFKA_SECURITY_PROTOCOL}
    optional = {"sasl.mechanism": Settings.KAFKA_SASL_MECHANISM,
                "sasl.username": Settings.KAFKA_SASL_USERNAME,
                "sasl.password": Settings.KAFKA_SASL_PASSWORD,
                "ssl.ca.location": Settings.KAFKA_SSL_CA_LOCATION,
                "ssl.certificate.location": Settings.KAFKA_SSL_CERTIFICATE_LOCATION,
                "ssl.key.location": Settings.KAFKA_SSL_KEY_LOCATION}
    result.update({key: value for key, value in optional.items() if value})
    return result


class PermanentProcessingError(Exception):
    """A poison record that will not succeed when retried unchanged."""


class DlqPublishFailedError(RuntimeError):
    """A poison record was neither stored durably nor DLQ'd durably.

    Fail-stop semantics: the source offset must NOT advance, later offsets in the
    same partition must NOT be processed, and the consumer must terminate cleanly
    so Docker's restart policy restarts it and Kafka replays the uncommitted
    offset.  This is the only safe behaviour when a failed record can be laid down
    in neither Raw nor the DLQ.
    """

    def __init__(self, envelope: Dict[str, Any]):
        super().__init__(
            "Final DLQ publication failed; source offset left uncommitted and consumer "
            "stopping so the record is replayed on restart"
        )
        self.envelope = envelope


def deterministic_failure_id(topic: str, partition: int, offset: int) -> str:
    """Deterministic DLQ identity derived only from the original Kafka coordinates.

    The exact same source record (same topic/partition/offset) always maps to the
    exact same identity, including when Kafka replays an uncommitted offset after a
    consumer restart.  Format: ``<topic>:<partition>:<offset>``.

    Note: a deterministic key does NOT give physical exactly-once DLQ publication
    on a ``cleanup.policy=delete`` topic; it makes duplicate DLQ events
    deterministically identifiable and replay duplicate-safe.
    """
    return f"{topic}:{partition}:{offset}"


def deterministic_s3_key(topic: str, partition: int, offset: int, timestamp: datetime) -> str:
    info = parse_topic(topic)
    return (
        f"{Settings.RAW_PREFIX}/category={info['category']}/source_group={info['source_group']}/"
        f"source_system={info['system']}/year={timestamp:%Y}/month={timestamp:%m}/day={timestamp:%d}/"
        f"topic={topic}/partition={partition}/offset={offset}/record.avro"
    )

# ------------------------------------------------------------------------------
# Avro Schema Definition
# ------------------------------------------------------------------------------
# This schema is used when storing events as Avro Object Container Files (OCF)
PAYMENT_EVENT_AVRO_SCHEMA = avro.schema.parse("""
{
    "type": "record",
    "name": "PaymentEvent",
    "namespace": "com.dp.ai.payment",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "message_id", "type": ["null", "string"], "default": null},
        {"name": "timestamp", "type": "string"},
        {"name": "source_system", "type": "string"},
        {"name": "message_type", "type": "string"},
        {"name": "event_family", "type": "string", "default": "PAYMENT_BUSINESS_EVENT"},
        {"name": "payload", "type": {
            "type": "record",
            "name": "Payload",
            "fields": [
                {"name": "amount", "type": ["double", "null"]},
                {"name": "currency", "type": ["string", "null"]},
                {"name": "debtor", "type": ["string", "null"]},
                {"name": "creditor", "type": ["string", "null"]},
                {"name": "status", "type": ["string", "null"]},
                {"name": "x_attributes", "type": {
                    "type": "map",
                    "values": "string"
                }, "default": {}}
            ]
        }},
        {"name": "event_data", "type": ["null", "string"], "default": null},
        {"name": "parsed_event_data", "type": ["null", "string"], "default": null},
        {"name": "_kafka_metadata", "type": {
            "type": "record",
            "name": "KafkaMetadata",
            "fields": [
                {"name": "topic", "type": "string"},
                {"name": "partition", "type": "int"},
                {"name": "offset", "type": "long"},
                {"name": "timestamp", "type": "string"},
                {"name": "category", "type": ["string", "null"]},
                {"name": "source_group", "type": ["string", "null"]},
                {"name": "source_system", "type": ["string", "null"]}
            ]
        }}
    ]
}
""")

# ------------------------------------------------------------------------------
# Topic to System Mapping
# ------------------------------------------------------------------------------
def parse_topic(topic: str) -> Dict[str, str]:
    """Return canonical source-domain metadata for a configured topic."""
    topic_map = {
        "icmn.vpm.pain001": {"category": "icmn", "source_group": "icmn", "system": "vpm", "msg_type": "pain001"},
        "icmn.pmn.pain001": {"category": "icmn", "source_group": "icmn", "system": "pmn", "msg_type": "technical_payment_event"},
        "cpo.psn.pain002": {"category": "cpo", "source_group": "cpo", "system": "psn", "msg_type": "pain002"},
        "cpo.plm.pain002": {"category": "cpo", "source_group": "cpo", "system": "plm", "msg_type": "technical_payment_event"},
        "wendi.camt052": {"category": "wendi", "source_group": "wendi", "system": "wendi", "msg_type": "camt052"},
        "wendi.camt053": {"category": "wendi", "source_group": "wendi", "system": "wendi", "msg_type": "camt053"},
        "wendi.camt054": {"category": "wendi", "source_group": "wendi", "system": "wendi", "msg_type": "camt054"},
        "wendi.transactions": {"category": "wendi", "source_group": "wendi", "system": "wendi", "msg_type": "transactions"},
        "wendi.pain001": {"category": "wendi", "source_group": "wendi", "system": "wendi", "msg_type": "pain001"},
        "wendi.pain002": {"category": "wendi", "source_group": "wendi", "system": "wendi", "msg_type": "pain002"},
        "mobile.mtn.pacs008": {"category": "mobile_networks", "source_group": "mobile", "system": "mtn", "msg_type": "pacs008"},
        "mobile.mtn.pacs002": {"category": "mobile_networks", "source_group": "mobile", "system": "mtn", "msg_type": "pacs002"},
        "mobile.airtel.pacs008": {"category": "mobile_networks", "source_group": "mobile", "system": "airtel", "msg_type": "pacs008"},
        "mobile.airtel.pacs002": {"category": "mobile_networks", "source_group": "mobile", "system": "airtel", "msg_type": "pacs002"},
        "agent.transactions": {"category": "agent_network", "source_group": "agent", "system": "agent", "msg_type": "transactions"},
        "agent.profiles": {"category": "agent_network", "source_group": "agent", "system": "agent", "msg_type": "profiles"},
        "agent.locations": {"category": "agent_network", "source_group": "agent", "system": "agent", "msg_type": "locations"},
        "pdmis.beneficiaries": {"category": "pdmis", "source_group": "pdmis", "system": "pdmis", "msg_type": "beneficiaries"},
        "pdmis.business_plans": {"category": "pdmis", "source_group": "pdmis", "system": "pdmis", "msg_type": "business_plans"},
        "pdmis.households": {"category": "pdmis", "source_group": "pdmis", "system": "pdmis", "msg_type": "households"},
        "pdmis.loans": {"category": "pdmis", "source_group": "pdmis", "system": "pdmis", "msg_type": "loans"},
        "pdmis.repayments": {"category": "pdmis", "source_group": "pdmis", "system": "pdmis", "msg_type": "repayments"},
        "pdmis.saccos": {"category": "pdmis", "source_group": "pdmis", "system": "pdmis", "msg_type": "saccos"},
        "pdmis.special_groups": {"category": "pdmis", "source_group": "pdmis", "system": "pdmis", "msg_type": "special_groups"},
    }
    if topic in topic_map:
        return topic_map[topic]

    parts = topic.split(".")
    return {
        "category": parts[0] if parts else "unknown",
        "source_group": parts[0] if parts else "unknown",
        "system": parts[1] if len(parts) > 2 else parts[0] if parts else "unknown",
        "msg_type": parts[-1] if parts else "unknown",
    }


# ------------------------------------------------------------------------------
# MinIO Client
# ------------------------------------------------------------------------------
def get_minio_client():
    """Get MinIO/S3 client"""
    return boto3.client(
        "s3",
        endpoint_url=Settings.MINIO_ENDPOINT,
        aws_access_key_id=Settings.MINIO_ACCESS_KEY,
        aws_secret_access_key=Settings.MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        verify=False,
    )

# ------------------------------------------------------------------------------
# Avro Serialization
# ------------------------------------------------------------------------------
def serialize_to_avro(event: Dict[str, Any]) -> bytes:
    """
    Serialize an event as an Avro Object Container File (OCF).

    DuckDB read_avro() requires Avro OCF files. A valid Avro OCF starts
    with the four magic bytes: b"Obj\\x01".
    """
    buffer = io.BytesIO()
    writer = None

    try:
        writer = DataFileWriter(
            buffer,
            DatumWriter(),
            PAYMENT_EVENT_AVRO_SCHEMA,
        )

        writer.append(event)
        writer.flush()

        avro_data = buffer.getvalue()

        expected_magic = bytes((0x4F, 0x62, 0x6A, 0x01))
        actual_magic = avro_data[:4]

        if actual_magic != expected_magic:
            raise AvroException(
                "Serialized output is not a valid Avro Object Container File: "
                f"expected magic={expected_magic!r}, actual={actual_magic!r}"
            )

        logger.debug(
            "Serialized Avro OCF successfully: %d bytes, magic=%r",
            len(avro_data),
            actual_magic,
        )

        return avro_data

    except AvroException as exc:
        logger.error("Avro serialization error: %s", exc)
        raise

    except Exception as exc:
        logger.exception("Unexpected error during Avro serialization: %s", exc)
        raise

    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                logger.debug("Ignoring Avro writer close error", exc_info=True)



# ------------------------------------------------------------------------------
# Event Storage
# ------------------------------------------------------------------------------
def store_event_to_s3(
    event: Dict[str, Any],
    topic: str,
    partition: int,
    offset: int,
    timestamp: datetime
) -> Optional[str]:
    """
    Store event in MinIO/S3 as Avro with date partitioning.
    
    Path: raw/v2/category={category}/source_group={source_group}/source_system={system}/
          year=YYYY/month=MM/day=DD/topic={topic}/partition={p}/offset={o}/{uuid}.avro
    """
    minio_client = get_minio_client()
    
    # Parse topic to get category/system/msg_type
    topic_info = parse_topic(topic)
    category = topic_info['category']
    source_group = topic_info['source_group']
    source_system = topic_info['system']
    
    # Date partitioning
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    
    s3_key = deterministic_s3_key(topic, partition, offset, timestamp)
    
    event_data = event.get("event_data")
    parsed_event_data = event.get("parsed_event_data")
    try:
        parsed_event = json.loads(parsed_event_data) if parsed_event_data else {}
    except (TypeError, json.JSONDecodeError):
        parsed_event = {}
    parsed_payload = parsed_event.get("payload", {})

    # Raw Avro keeps the original XML in event_data and parsed JSON separately.
    # The fixed payload record remains a compact compatibility projection.
    enriched_event = {
        "event_id": event.get("event_id", str(uuid.uuid4())),
        "message_id": event.get("message_id"),
        "timestamp": event.get("timestamp", timestamp.isoformat()),
        "source_system": event.get("source_system") or source_system,
        "message_type": event.get("message_type", topic_info['msg_type']),
        "event_family": event.get("event_family", "PAYMENT_BUSINESS_EVENT"),
        "payload": {
            "amount": event.get("instructed_amount"),
            "currency": event.get("currency"),
            "debtor": parsed_payload.get("debtor"),
            "creditor": parsed_payload.get("creditor"),
            "status": parsed_payload.get("transaction_status") or parsed_payload.get("status"),
            "x_attributes": event.get("x_attributes") or parsed_event.get("x_attributes", {}),
        },
        "event_data": event_data,
        "parsed_event_data": parsed_event_data,
        "_kafka_metadata": {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "timestamp": timestamp.isoformat(),
            "category": category,
            "source_group": source_group,
            "source_system": source_system,
        }
    }
    
    try:
        try:
            minio_client.head_object(Bucket=Settings.MINIO_BUCKET, Key=s3_key)
            logger.info("Raw object already exists; replay is idempotent: s3://%s/%s",
                        Settings.MINIO_BUCKET, s3_key)
            return s3_key
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
                raise

        avro_data = serialize_to_avro(enriched_event)
        
        # Store in MinIO
        minio_client.put_object(
            Bucket=Settings.MINIO_BUCKET,
            Key=s3_key,
            Body=avro_data,
            ContentType="application/avro",
        )
        
        logger.info(f"✅ Stored (Avro): s3://{Settings.MINIO_BUCKET}/{s3_key}")
        return s3_key
        
    except (ClientError, BotoCoreError, OSError) as e:
        logger.error(f"❌ Failed to store event: {e}")
        raise
    except (AvroException, TypeError, ValueError) as e:
        raise PermanentProcessingError(str(e)) from e

# ------------------------------------------------------------------------------
# Dead Letter Queue (Stored as JSON for readability)
# ------------------------------------------------------------------------------
def store_to_dlq(
    event: Dict[str, Any],
    topic: str,
    partition: int,
    offset: int,
    error: str
) -> None:
    """
    Store failed events to Dead Letter Queue as JSON.
    
    Path: bronze/invalid/{category}/year=YYYY/month=MM/day=DD/topic={topic}/partition={p}/offset={o}/{uuid}.json
    """
    minio_client = get_minio_client()
    
    topic_info = parse_topic(topic)
    category = topic_info['category']
    source_group = topic_info['source_group']
    source_system = topic_info['system']
    
    now = datetime.now(pytz.UTC)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    
    file_id = uuid.uuid4()
    s3_key = (
        f"bronze/invalid/category={category}/source_group={source_group}/source_system={source_system}/"
        f"year={year}/month={month}/day={day}/topic={topic}/partition={partition}/offset={offset}/"
        f"{file_id}.json"
    )
    
    dlq_event = {
        "original_topic": topic,
        "original_partition": partition,
        "original_offset": offset,
        "error": error,
        "timestamp": now.isoformat(),
        "event": event,
    }
    
    try:
        minio_client.put_object(
            Bucket=Settings.MINIO_BUCKET,
            Key=s3_key,
            Body=json.dumps(dlq_event, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.warning(f"⚠️  Sent to DLQ (JSON): s3://{Settings.MINIO_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"❌ Failed to store to DLQ: {e}")


def failure_envelope(msg, event: Optional[Dict[str, Any]], error: Exception,
                     retry_count: int, first_failure: str) -> Dict[str, Any]:
    x_attributes = (event or {}).get("x_attributes") or {}
    key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
    return {
        "failure_id": deterministic_failure_id(msg.topic(), msg.partition(), msg.offset()),
        "original_topic": msg.topic(), "original_partition": msg.partition(),
        "original_offset": msg.offset(), "original_event_id": (event or {}).get("event_id"),
        "business_key": key, "failure_reason": str(error), "error_type": type(error).__name__,
        "retry_count": retry_count, "first_failure_timestamp": first_failure,
        "last_failure_timestamp": datetime.now(pytz.UTC).isoformat(),
        "correlation_id": x_attributes.get("x-correlationId"),
        "trace_id": x_attributes.get("x-traceId"),
        "original_key_base64": base64.b64encode(msg.key() or b"").decode("ascii"),
        "original_payload_base64": base64.b64encode(msg.value() or b"").decode("ascii"),
    }


def publish_failure(producer: Producer, topic: str, envelope: Dict[str, Any],
                    key: Optional[str] = None, headers: Optional[Dict[str, str]] = None) -> bool:
    result = {"called": False, "error": None}
    metadata_key = envelope.get("business_key") if key is None else key
    producer.produce(topic, key=metadata_key,
                     value=json.dumps(envelope, separators=(",", ":")),
                     headers=headers,
                     callback=lambda error, _message: result.update(called=True, error=error))
    remaining = producer.flush(30)
    return remaining == 0 and result["called"] and result["error"] is None


def retry_delay(attempt: int) -> float:
    return min(Settings.RETRY_INITIAL_DELAY_SECONDS *
               (Settings.RETRY_BACKOFF_MULTIPLIER ** max(0, attempt - 1)),
               Settings.RETRY_MAX_DELAY_SECONDS)


def should_send_to_dlq(error: Exception, attempt: int) -> bool:
    return isinstance(error, PermanentProcessingError) or attempt >= Settings.MAX_PROCESSING_RETRIES


def publish_dlq_then_commit(producer, consumer, msg, envelope: Dict[str, Any]) -> bool:
    """Advance a poison record only after Kafka acknowledges its final DLQ copy.

    The DLQ record is keyed by the deterministic failure identity
    ``<original_topic>:<original_partition>:<original_offset>`` so re-processing the
    same source record (e.g. after a Kafka replay of an uncommitted offset) always
    produces the same DLQ key and the same ``x-failure-id`` header.

    This is at-least-once publication: a deterministic key does not by itself give
    physical exactly-once delivery on a ``cleanup.policy=delete`` topic.  Duplicate
    DLQ envelopes for the same source record remain identifiable via ``failure_id``
    and replay tooling is duplicate-safe.
    """
    failure_id = envelope.get("failure_id")
    if not publish_failure(producer, Settings.KAFKA_DLQ_TOPIC, envelope,
                           key=failure_id,
                           headers={"x-failure-id": failure_id}):
        return False
    consumer.commit(msg, asynchronous=False)
    return True


def store_then_commit(consumer, msg, event: Dict[str, Any], timestamp: datetime) -> str:
    """Durably store first, then synchronously commit the consumed coordinate."""
    key = store_event_to_s3(event, msg.topic(), msg.partition(), msg.offset(), timestamp)
    consumer.commit(msg, asynchronous=False)
    return key


def process_message(consumer, failure_producer, avro_deserializer, msg) -> str:
    """Process one source record: deserialize, store/boundary-retry, and commit.

    Returns ``"ok"`` when Raw storage succeeded durably and the source offset was
    committed, or ``"dlq"`` when the record was published to ``payment-events.dlq``
    and the source offset was then committed.

    Raises :class:`DlqPublishFailedError` when a permanent failure can be neither
    stored durably nor DLQ'd durably.  In that case the source offset is NOT
    committed, later offsets in the partition are NOT processed, and the consumer
    fails-stop so Docker restart policy restarts it and Kafka replays the
    uncommitted record (offset invariant: the source offset advances only after Raw
    storage OR DLQ publication succeeds durably).
    """
    event = None
    first_failure = datetime.now(pytz.UTC).isoformat()
    for attempt in range(Settings.MAX_PROCESSING_RETRIES + 1):
        try:
            if event is None:
                try:
                    event = avro_deserializer(
                        msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
                    )
                except Exception as exc:
                    raise PermanentProcessingError(str(exc)) from exc

            timestamp_data = msg.timestamp()
            if timestamp_data and timestamp_data[1] is not None and timestamp_data[1] >= 0:
                timestamp = datetime.fromtimestamp(timestamp_data[1] / 1000, pytz.UTC)
            else:
                # A stable epoch fallback keeps object identity deterministic
                # even for legacy records that have no Kafka timestamp.
                timestamp = datetime.fromtimestamp(0, pytz.UTC)

            store_then_commit(consumer, msg, event, timestamp)
            return "ok"
        except Exception as exc:
            if not should_send_to_dlq(exc, attempt):
                envelope = failure_envelope(msg, event, exc, attempt + 1, first_failure)
                publish_failure(failure_producer, Settings.KAFKA_RETRY_TOPIC, envelope)
                time.sleep(retry_delay(attempt + 1))
                continue

            envelope = failure_envelope(msg, event, exc, attempt, first_failure)
            try:
                published = publish_dlq_then_commit(failure_producer, consumer, msg, envelope)
            except Exception as dlq_exc:
                # The DLQ producer itself raised (e.g. no such topic with
                # auto-create disabled).  Fail-stop with the same invariant.
                raise DlqPublishFailedError(envelope) from dlq_exc
            if published:
                logger.error(json.dumps({"service": "payment-consumer-events",
                                         "event": "sent_to_dlq", **envelope}, default=str))
                return "dlq"
            # DLQ acknowledged nothing: do NOT commit, do NOT continue to later
            # offsets.  Fail the consumer; Docker restarts it and Kafka replays
            # this uncommitted source record.
            raise DlqPublishFailedError(envelope) from exc

# ------------------------------------------------------------------------------
# Main Consumer
# ------------------------------------------------------------------------------
def main():
    """Main consumer loop"""

    # Fail fast if the runtime Avro library cannot produce a valid OCF.
    self_test_now = datetime.now(pytz.UTC).isoformat()
    self_test_event = {
        "event_id": "avro-ocf-self-test",
        "message_id": None,
        "timestamp": self_test_now,
        "source_system": "self-test",
        "message_type": "self-test",
        "payload": {
            "amount": None,
            "currency": None,
            "debtor": None,
            "creditor": None,
            "status": None,
            "x_attributes": {},
        },
        "event_data": None,
        "parsed_event_data": None,
        "_kafka_metadata": {
            "topic": "self-test",
            "partition": 0,
            "offset": 0,
            "timestamp": self_test_now,
            "category": "self-test",
            "source_group": "self-test",
            "source_system": "self-test",
        },
    }

    self_test_bytes = serialize_to_avro(self_test_event)
    if self_test_bytes[:4] != bytes((0x4F, 0x62, 0x6A, 0x01)):
        raise RuntimeError("Avro OCF startup self-test failed")

    logger.info("✅ Avro OCF startup self-test passed")
    logger.info("=" * 80)
    logger.info("🚀 Payment Events Consumer (Raw Layer - Avro Storage)")
    logger.info("=" * 80)
    logger.info(f"📋 Topics: {', '.join(Settings.KAFKA_TOPICS)}")
    logger.info(f"📦 Consumer Group: {Settings.KAFKA_GROUP_ID}")
    logger.info(f"🪣 MinIO Bucket: {Settings.MINIO_BUCKET}")
    logger.info(f"📡 Schema Registry: {Settings.SCHEMA_REGISTRY_URL}")
    logger.info("📄 Storage Format: AVRO OCF (JSON fallback on serialization/storage failure)")
    logger.info("=" * 80)
    
    # Initialize Schema Registry client
    sr_config = {"url": Settings.SCHEMA_REGISTRY_URL}
    if Settings.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO:
        sr_config["basic.auth.user.info"] = Settings.SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO
    schema_registry_client = SchemaRegistryClient(sr_config)
    
    # Initialize Avro deserializer
    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client
    )
    
    # Initialize Kafka consumer
    consumer_config = {
        "bootstrap.servers": Settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": Settings.KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    }
    consumer_config.update(kafka_client_config())
    consumer = Consumer(consumer_config)
    failure_producer_config = {"bootstrap.servers": Settings.KAFKA_BOOTSTRAP_SERVERS,
                               "acks": "all", "enable.idempotence": True}
    failure_producer_config.update(kafka_client_config())
    failure_producer = Producer(failure_producer_config)
    
    # Subscribe to all topics
    consumer.subscribe(Settings.KAFKA_TOPICS)
    logger.info(f"✅ Subscribed to {len(Settings.KAFKA_TOPICS)} topics")
    
    processed_count = 0
    error_count = 0
    
    try:
        try:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition: {msg.topic()} [{msg.partition()}]")
                    else:
                        logger.error(f"❌ Kafka error: {msg.error()}")
                    continue

                outcome = process_message(consumer, failure_producer, avro_deserializer, msg)
                if outcome == "ok":
                    processed_count += 1
                    if processed_count % 100 == 0:
                        logger.info(f"📊 Processed {processed_count} events")
                else:
                    error_count += 1
        except DlqPublishFailedError as exc:
            envelope = exc.envelope
            logger.critical(json.dumps({
                "service": "payment-consumer-events",
                "event": "fail_stop_dlq_publish_failed",
                "failure_id": envelope.get("failure_id"),
                "original_topic": envelope.get("original_topic"),
                "original_partition": envelope.get("original_partition"),
                "original_offset": envelope.get("original_offset"),
                "business_key": envelope.get("business_key"),
            }, default=str))
            logger.critical(
                "🔴 FATAL: a poison record could be neither stored in Raw nor DLQ'd. "
                "Its source offset is left uncommitted and later offsets are not "
                "processed. The consumer now exits so Docker restarts it and Kafka "
                "replays the uncommitted record."
            )
            raise SystemExit(1)

    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
    finally:
        # Report statistics
        logger.info("=" * 80)
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   ✅ Successfully processed (Avro): {processed_count}")
        logger.info(f"   ❌ Errors: {error_count}")
        logger.info("   📄 Storage Format: Avro OCF")
        logger.info("=" * 80)
        consumer.close()
        logger.info("✅ Consumer closed")

if __name__ == "__main__":
    main()
