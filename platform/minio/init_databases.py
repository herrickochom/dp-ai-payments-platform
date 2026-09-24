#!/usr/bin/env python3
"""
Initialize MinIO databases and configurations.
"""
import boto3
import os
from services.shared.security.runtime_security import validate_object_store_security
from services.shared.security.secret_provider import require_secret
from botocore.client import Config

def main():
    endpoint, _, ca_bundle = validate_object_store_security(
        os.environ['S3_ENDPOINT'],
        use_ssl=os.environ['S3_USE_SSL'],
        ca_bundle=os.getenv('S3_CA_BUNDLE'),
    )
    bucket = os.environ['OBJECT_STORE_BUCKET'].strip()
    region = os.environ['OBJECT_STORE_REGION'].strip()
    if not bucket or not region:
        raise RuntimeError('OBJECT_STORE_BUCKET and OBJECT_STORE_REGION are required')
    access_key = os.environ['MINIO_ROOT_USER']
    secret_key = require_secret('MINIO_ROOT_PASSWORD')
    
    client_options = {'verify': ca_bundle} if ca_bundle else {}
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name=region,
        **client_options
    )
    
    # Create buckets if they don't exist
    buckets = [bucket]
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
