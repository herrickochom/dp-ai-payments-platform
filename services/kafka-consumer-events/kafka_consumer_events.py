#!/usr/bin/env python3
"""
Payment Events Consumer - Reads from system-specific Kafka topics
and stores raw events in MinIO/S3 with date partitioning in Avro format.

Topics consumed:
- icmn.vpm.pain001, icmn.vpm.pain002
- icmn.pmn.pain001, icmn.pmn.pain002
- cpo.psn.pain001, cpo.psn.pain002
- cpo.plm.pain001, cpo.plm.pain002
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
from avro.io import DatumWriter, BinaryEncoder
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
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_TOPICS = os.getenv("KAFKA_TOPICS", "").split(",") if os.getenv("KAFKA_TOPICS") else [
        "icmn.vpm.pain001",
        "icmn.vpm.pain002",
        "icmn.pmn.pain001",
        "icmn.pmn.pain002",
        "cpo.psn.pain001",
        "cpo.psn.pain002",
        "cpo.plm.pain001",
        "cpo.plm.pain002",
    ]
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "payment-events-consumer")
    
    # Schema Registry
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

# ------------------------------------------------------------------------------
# Avro Schema Definition
# ------------------------------------------------------------------------------
# This schema is used when storing events in Avro format
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
                {"name": "category", "type": ["string", "null"]}
            ]
        }}
    ]
}
""")

# ------------------------------------------------------------------------------
# Topic to System Mapping
# ------------------------------------------------------------------------------
def parse_topic(topic: str) -> Dict[str, str]:
    """
    Parse topic name to extract system information.
    
    Topic format: {category}.{system}.{msg_type}
    Example: icmn.vpm.pain001 → {'category': 'icmn', 'system': 'vpm', 'msg_type': 'pain001'}
    """
    parts = topic.split('.')
    if len(parts) == 3:
        return {
            'category': parts[0],      # icmn or cpo
            'system': parts[1],        # vpm, pmn, psn, plm
            'msg_type': parts[2]       # pain001, pain002, etc.
        }
    else:
        return {
            'category': 'unknown',
            'system': 'unknown',
            'msg_type': 'unknown'
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
    Serialize event to Avro binary format.
    
    Args:
        event: Dictionary event to serialize
        
    Returns:
        bytes: Avro serialized data
    """
    try:
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer = DatumWriter(PAYMENT_EVENT_AVRO_SCHEMA)
        writer.write(event, encoder)
        return bytes_writer.getvalue()
    except AvroException as e:
        logger.error(f"Avro serialization error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during Avro serialization: {e}")
        raise

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
    
    Path: raw/{category}/year=YYYY/month=MM/day=DD/topic={topic}/partition={p}/offset={o}/{uuid}.avro
    """
    minio_client = get_minio_client()
    
    # Parse topic to get category/system/msg_type
    topic_info = parse_topic(topic)
    category = topic_info['category']
    
    # Date partitioning
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    
    # Generate unique filename
    file_id = uuid.uuid4()
    filename = f"{file_id}.avro"
    
    # Construct S3 key
    s3_key = (
        f"raw/{category}/year={year}/month={month}/day={day}/"
        f"topic={topic}/partition={partition}/offset={offset}/"
        f"{filename}"
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
        "source_system": event.get("source_system", topic_info['system']),
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
            logger.warning(f"⚠️  Stored as JSON fallback: s3://{Settings.MINIO_BUCKET}/{json_key}")
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
    
    now = datetime.now(pytz.UTC)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    
    file_id = uuid.uuid4()
    s3_key = (
        f"bronze/invalid/{category}/year={year}/month={month}/day={day}/"
        f"topic={topic}/partition={partition}/offset={offset}/"
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
    logger.info("=" * 80)
    logger.info("🚀 Payment Events Consumer (Raw Layer - Avro Storage)")
    logger.info("=" * 80)
    logger.info(f"📋 Topics: {', '.join(Settings.KAFKA_TOPICS)}")
    logger.info(f"📦 Consumer Group: {Settings.KAFKA_GROUP_ID}")
    logger.info(f"🪣 MinIO Bucket: {Settings.MINIO_BUCKET}")
    logger.info(f"📡 Schema Registry: {Settings.SCHEMA_REGISTRY_URL}")
    logger.info(f"📄 Storage Format: AVRO (with JSON fallback)")
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
        logger.info(f"   📄 Storage Format: Avro (with JSON fallback)")
        logger.info("=" * 80)
        consumer.close()
        logger.info("✅ Consumer closed")

if __name__ == "__main__":
    main()
