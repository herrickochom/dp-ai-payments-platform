#!/usr/bin/env python3
"""
Modeling Agent - ML model training and inference for payment data
"""
import os
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    logger.info("Modeling Agent Started")
    logger.info(f"MinIO Endpoint: {os.getenv('MINIO_ENDPOINT', 'http://minio:9000')}")
    logger.info(f"Nessie Endpoint: {os.getenv('NESSIE_ENDPOINT', 'http://nessie:19120')}")
    logger.info(f"Trino Host: {os.getenv('TRINO_HOST', 'trino')}")
    
    while True:
        try:
            logger.info("Modeling agent running...")
            time.sleep(60)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
