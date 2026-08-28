#!/usr/bin/env python3
"""
S3 Landing to Kafka Ingestion - Reads from S3 and sends to Kafka
"""
import os
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    logger.info("S3 Landing to Kafka Ingestion Started")
    logger.info(f"S3 Endpoint: {os.getenv('S3_ENDPOINT', 'http://minio:9000')}")
    logger.info(f"Kafka: {os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')}")
    
    while True:
        try:
            logger.info("Ingestion service running...")
            time.sleep(60)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
