#!/usr/bin/env python3
"""
Data Quality Agent - Monitors payment data quality
"""
import os
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    logger.info("Data Quality Agent Started")
    logger.info(f"MinIO: {os.getenv('MINIO_ENDPOINT', 'http://minio:9000')}")
    logger.info(f"Nessie: {os.getenv('NESSIE_ENDPOINT', 'http://nessie:19120')}")
    logger.info(f"Trino: {os.getenv('TRINO_HOST', 'trino')}")
    logger.info(f"Log Directory: {os.getenv('LOG_DIR', '/logs')}")
    
    while True:
        try:
            logger.info("Data Quality Agent running...")
            time.sleep(60)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
