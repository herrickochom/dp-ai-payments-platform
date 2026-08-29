#!/usr/bin/env python3
"""
Payment Events Consumer - Reads source-domain Kafka topics and stores immutable
raw events in MinIO/S3 with date partitioning in Avro format.

Consumes the 23 source-ingestion topics for ICMN, CPO, Wendi, Mobile Networks,
Agent Network and PDMIS. Reconciliation topics are downstream-derived and excluded.
"""

import os
import sys
import json
import uuid
import io
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaError
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
        "pdmis.saccos",
        "pdmis.special_groups",
    ]
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "payment-events-consumer")
    
    # Schema Registry
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

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
        "icmn.pmn.pain001": {"category": "icmn", "source_group": "icmn", "system": "pmn", "msg_type": "pain001"},
        "cpo.psn.pain002": {"category": "cpo", "source_group": "cpo", "system": "psn", "msg_type": "pain002"},
        "cpo.plm.pain002": {"category": "cpo", "source_group": "cpo", "system": "plm", "msg_type": "pain002"},
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
    
    # Generate unique filename
    file_id = uuid.uuid4()
    filename = f"{file_id}.avro"
    
    # Construct S3 key
    s3_key = (
        f"{Settings.RAW_PREFIX}/category={category}/source_group={source_group}/"
        f"source_system={source_system}/year={year}/month={month}/day={day}/"
        f"topic={topic}/partition={partition}/offset={offset}/{filename}"
    )
    
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
        # Serialize to Avro
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
        
    except Exception as e:
        logger.error(f"❌ Failed to store event: {e}")
        # Fallback: Store as JSON if Avro fails
        try:
            json_key = s3_key.replace(".avro", ".json")
            minio_client.put_object(
                Bucket=Settings.MINIO_BUCKET,
                Key=json_key,
                Body=json.dumps(enriched_event, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
            logger.warning(f"⚠️  Stored JSON fallback (not Avro): s3://{Settings.MINIO_BUCKET}/{json_key}")
            return json_key
        except Exception as fallback_error:
            logger.error(f"❌ Fallback storage also failed: {fallback_error}")
            return None

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
    schema_registry_client = SchemaRegistryClient({
        "url": Settings.SCHEMA_REGISTRY_URL
    })
    
    # Initialize Avro deserializer
    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client
    )
    
    # Initialize Kafka consumer
    consumer = Consumer({
        "bootstrap.servers": Settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": Settings.KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    })
    
    # Subscribe to all topics
    consumer.subscribe(Settings.KAFKA_TOPICS)
    logger.info(f"✅ Subscribed to {len(Settings.KAFKA_TOPICS)} topics")
    
    processed_count = 0
    error_count = 0
    
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
            
            # Process message
            try:
                # Deserialize Avro message
                event = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )
                
                # Get timestamp
                if msg.timestamp() is not None:
                    ts_type, ts_ms = msg.timestamp()
                    if ts_type == 0:  # CreateTime
                        timestamp = datetime.fromtimestamp(ts_ms / 1000, pytz.UTC)
                    else:
                        timestamp = datetime.now(pytz.UTC)
                else:
                    timestamp = datetime.now(pytz.UTC)
                
                # Store in S3 as Avro
                s3_key = store_event_to_s3(
                    event,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    timestamp
                )
                
                if s3_key:
                    # Commit offset after successful storage
                    consumer.commit(msg, asynchronous=False)
                    processed_count += 1
                    
                    if processed_count % 100 == 0:
                        logger.info(f"📊 Processed {processed_count} events")
                else:
                    # Store to DLQ if storage failed
                    store_to_dlq(
                        event,
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        "Failed to store to S3 (Avro and JSON fallback both failed)"
                    )
                    error_count += 1
                    
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")
                error_count += 1
                
                # Store raw message to DLQ
                try:
                    store_to_dlq(
                        {"raw_value": str(msg.value())},
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        str(e)
                    )
                except Exception as dlq_error:
                    logger.error(f"❌ Failed to store to DLQ: {dlq_error}")
    
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
    finally:
        # Report statistics
        logger.info("=" * 80)
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   ✅ Successfully processed (Avro): {processed_count}")
        logger.info(f"   ❌ Errors: {error_count}")
        logger.info("   📄 Storage Format: Avro OCF (JSON fallback on failure)")
        logger.info("=" * 80)
        consumer.close()
        logger.info("✅ Consumer closed")

if __name__ == "__main__":
    main()