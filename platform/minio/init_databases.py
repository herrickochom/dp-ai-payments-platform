#!/usr/bin/env python3
"""
Initialize MinIO databases and configurations.
"""
import boto3
import os
import time
from botocore.client import Config

def main():
    endpoint = os.environ.get('MINIO_ENDPOINT', 'http://minio:9000')
    access_key = os.environ.get('MINIO_ROOT_USER', 'minioadmin')
    secret_key = os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')
    
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    # Create buckets if they don't exist
    buckets = ['dp-ai-payment']
    for bucket in buckets:
        try:
            s3.head_bucket(Bucket=bucket)
            print(f"Bucket {bucket} already exists")
        except:
            s3.create_bucket(Bucket=bucket)
            print(f"Created bucket {bucket}")
    
    print("MinIO initialization complete!")

if __name__ == "__main__":
    main()
