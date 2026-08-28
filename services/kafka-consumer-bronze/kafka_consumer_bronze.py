#!/usr/bin/env python3
"""
Payment Bronze Consumer - Validates events and stores in bronze layer.

Consumes from system-specific topics and writes Parquet files to:
- bronze/valid/{category}/year=YYYY/month=MM/day=DD/topic={topic}/partition={p}/offset={o}/{event_id}.parquet
- bronze/invalid/{category}/year=YYYY/month=MM/day=DD/topic={topic}/partition={p}/offset={o}/{event_id}.parquet
"""

import os
import sys
import json
import uuid
import logging
import datetime
from typing import Dict, Any, Optional, List, Tuple
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError
import pytz
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import redis

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Configuration
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
    KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "payment-bronze-consumer")
    
    # Schema Registry
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    
    # Redis (for idempotency)
    REDIS_HOST = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    IDEMPOTENCY_TTL = int(os.getenv("IDEMPOTENCY_TTL", "604800"))  # 7 days
    
    # Validation
    VALIDATION_ENABLED = os.getenv("VALIDATION_ENABLED", "true").lower() == "true"

# ------------------------------------------------------------------------------
# Topic Parser
# ------------------------------------------------------------------------------
def parse_topic(topic: str) -> Dict[str, str]:
    """Parse topic name to extract system information"""
    parts = topic.split('.')
    if len(parts) == 3:
        return {
            'category': parts[0],
            'system': parts[1],
            'msg_type': parts[2]
        }
    return {'category': 'unknown', 'system': 'unknown', 'msg_type': 'unknown'}

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
# Redis Client (Idempotency)
# ------------------------------------------------------------------------------
class IdempotencyCache:
    """Redis-based idempotency cache for deduplication"""
    
    def __init__(self):
        self.client = None
        try:
            self.client = redis.Redis(
                host=Settings.REDIS_HOST,
                port=Settings.REDIS_PORT,
                db=Settings.REDIS_DB,
                decode_responses=True
            )
            self.client.ping()
            logger.info(f"✅ Redis connected for idempotency")
        except Exception as e:
            logger.warning(f"⚠️ Redis not available: {e}, idempotency disabled")
            self.client = None
    
    def is_processed(self, event_id: str, layer: str = "bronze") -> bool:
        """Check if event already processed"""
        if self.client is None:
            return False
        key = f"idempotent:{layer}:{event_id}"
        return self.client.exists(key) > 0
    
    def mark_processed(self, event_id: str, layer: str = "bronze"):
        """Mark event as processed"""
        if self.client is None:
            return
        key = f"idempotent:{layer}:{event_id}"
        self.client.setex(key, Settings.IDEMPOTENCY_TTL, "1")
    
    def get_processed_count(self, layer: str = "bronze") -> int:
        """Get count of processed events"""
        if self.client is None:
            return 0
        pattern = f"idempotent:{layer}:*"
        keys = self.client.keys(pattern)
        return len(keys)

# ------------------------------------------------------------------------------
# Validation
# ------------------------------------------------------------------------------
def validate_event(event: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate event against business rules.
    Returns: (is_valid, list_of_errors)
    """
    errors = []
    
    # Required fields
    required_fields = ["event_id", "message_id", "source_system", "event_type"]
    for field in required_fields:
        if not event.get(field):
            errors.append(f"Missing required field: {field}")
    
    # Amount validation
    amount = event.get("instructed_amount")
    if amount is not None:
        try:
            amount = float(amount)
            if amount <= 0:
                errors.append(f"Amount must be positive: {amount}")
            if amount > 10000000:  # 10M max
                errors.append(f"Amount exceeds limit: {amount}")
        except (ValueError, TypeError):
            errors.append(f"Invalid amount: {amount}")
    
    # Currency validation
    currency = event.get("currency", "")
    valid_currencies = ["GBP", "EUR", "USD", "CHF", "JPY"]
    if currency not in valid_currencies:
        errors.append(f"Invalid currency: {currency}")
    
    # Event type validation
    event_type = event.get("event_type", "")
    valid_types = ["pain001", "pain002", "pacs002", "pacs004", "pacs008", "pacs009", "camt053"]
    if event_type.lower() not in valid_types:
        errors.append(f"Invalid event type: {event_type}")
    
    # Source system validation
    source_system = event.get("source_system", "")
    valid_systems = ["vpm", "pmn", "psn", "plm"]
    if source_system.lower() not in valid_systems:
        errors.append(f"Invalid source system: {source_system}")
    
    return len(errors) == 0, errors

# ------------------------------------------------------------------------------
# S3 Storage (Parquet)
# ------------------------------------------------------------------------------
def store_to_bronze(
    event: Dict[str, Any],
    topic: str,
    partition: int,
    offset: int,
    timestamp: datetime.datetime,
    valid: bool,
    errors: List[str]
) -> Optional[str]:
    """
    Store validated event to bronze layer as Parquet.
    
    Path: bronze/valid|invalid/{category}/year=YYYY/month=MM/day=DD/topic={topic}/partition={p}/offset={o}/{event_id}.parquet
    """
    minio_client = get_minio_client()
    
    # Parse topic
    topic_info = parse_topic(topic)
    category = topic_info['category']
    
    # Date partitioning
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    
    # Enrich event with metadata
    enriched_event = event.copy()
    enriched_event.update({
        "_validation": {
            "valid": valid,
            "errors": errors,
            "validated_at": timestamp.isoformat(),
        },
        "_kafka_metadata": {
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "timestamp": timestamp.isoformat(),
            "category": category,
        }
    })
    
    # Determine path
    status = "valid" if valid else "invalid"
    event_id = event.get("event_id", str(uuid.uuid4()))
    
    s3_key = (
        f"bronze/{status}/{category}/year={year}/month={month}/day={day}/"
        f"topic={topic}/partition={partition}/offset={offset}/"
        f"{event_id}.parquet"
    )
    
    try:
        # Convert to Parquet
        df = pd.DataFrame([enriched_event])
        table = pa.Table.from_pandas(df)
        
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        # The object key contains the Kafka topic, partition, and offset, so a
        # retry can safely overwrite the same immutable event object.  Boto3's
        # PutObject model for this client does not support an If-None-Match
        # parameter.
        minio_client.put_object(
            Bucket=Settings.MINIO_BUCKET,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/parquet",
        )
        
        logger.info(f"✅ Stored {status} Parquet: s3://{Settings.MINIO_BUCKET}/{s3_key}")
        return s3_key
        
    except ClientError as e:
        logger.error(f"❌ Failed to store: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Failed to store: {e}")
        return None

# ------------------------------------------------------------------------------
# Dead Letter Queue (JSON for readability)
# ------------------------------------------------------------------------------
def store_to_dlq(
    event: Dict[str, Any],
    topic: str,
    partition: int,
    offset: int,
    error: str
) -> None:
    """Store failed events to DLQ as JSON"""
    minio_client = get_minio_client()
    topic_info = parse_topic(topic)
    category = topic_info['category']
    
    now = datetime.datetime.now(pytz.UTC)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    
    event_id = event.get("event_id", str(uuid.uuid4()))
    
    s3_key = (
        f"bronze/invalid/{category}/year={year}/month={month}/day={day}/"
        f"topic={topic}/partition={partition}/offset={offset}/"
        f"{event_id}_dlq.json"
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
        logger.warning(f"⚠️ Sent to DLQ: s3://{Settings.MINIO_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"❌ Failed to store DLQ: {e}")

# ------------------------------------------------------------------------------
# Main Consumer
# ------------------------------------------------------------------------------
def main():
    """Main consumer loop"""
    logger.info("=" * 80)
    logger.info("🏗️ Bronze Layer Consumer (Validator)")
    logger.info("=" * 80)
    logger.info(f"📋 Topics: {', '.join(Settings.KAFKA_TOPICS)}")
    logger.info(f"📦 Consumer Group: {Settings.KAFKA_GROUP_ID}")
    logger.info(f"🪣 MinIO Bucket: {Settings.MINIO_BUCKET}")
    logger.info(f"📡 Schema Registry: {Settings.SCHEMA_REGISTRY_URL}")
    logger.info(f"🔒 Idempotency: Enabled (Redis)")
    logger.info(f"✅ Validation: {Settings.VALIDATION_ENABLED}")
    logger.info("=" * 80)
    
    # Initialize Schema Registry
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
    
    # Initialize idempotency cache
    idempotency = IdempotencyCache()
    
    # Statistics
    processed_count = 0
    valid_count = 0
    invalid_count = 0
    duplicate_count = 0
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
                # Deserialize Avro
                event = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )
                
                event_id = event.get("event_id", str(uuid.uuid4()))
                
                # Idempotency check
                if idempotency.is_processed(event_id, "bronze"):
                    logger.debug(f"⏭️ Event {event_id} already processed, skipping")
                    duplicate_count += 1
                    consumer.commit(msg, asynchronous=False)
                    continue
                
                # Validate event
                valid, errors = validate_event(event)
                
                # Get timestamp
                timestamp = datetime.datetime.now(pytz.UTC)
                
                # Store to bronze
                s3_key = store_to_bronze(
                    event,
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    timestamp,
                    valid,
                    errors
                )
                
                if s3_key:
                    # Mark as processed (idempotency)
                    idempotency.mark_processed(event_id, "bronze")
                    
                    # Commit offset
                    consumer.commit(msg, asynchronous=False)
                    
                    processed_count += 1
                    if valid:
                        valid_count += 1
                    else:
                        invalid_count += 1
                    
                    if processed_count % 100 == 0:
                        logger.info(f"📊 Processed {processed_count} events "
                                   f"(valid: {valid_count}, invalid: {invalid_count}, duplicates: {duplicate_count})")
                else:
                    # Storage failed, don't commit
                    error_count += 1
                    logger.error(f"❌ Failed to store event {event_id}")
                    
            except Exception as e:
                logger.error(f"❌ Error processing message: {e}")
                error_count += 1
                
                # Store to DLQ
                try:
                    store_to_dlq(
                        {"raw": str(msg.value())},
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        str(e)
                    )
                except Exception as dlq_error:
                    logger.error(f"❌ Failed to store DLQ: {dlq_error}")
    
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down...")
    finally:
        # Report statistics
        logger.info("=" * 80)
        logger.info("📊 FINAL STATISTICS")
        logger.info("=" * 80)
        logger.info(f"✅ Total processed: {processed_count}")
        logger.info(f"   ✅ Valid: {valid_count}")
        logger.info(f"   ❌ Invalid: {invalid_count}")
        logger.info(f"   ⏭️ Duplicates skipped: {duplicate_count}")
        logger.info(f"   ❌ Errors: {error_count}")
        logger.info(f"🔒 Idempotency cache: {idempotency.get_processed_count('bronze')} events")
        logger.info("=" * 80)
        consumer.close()
        logger.info("✅ Consumer closed")

if __name__ == "__main__":
    main()
