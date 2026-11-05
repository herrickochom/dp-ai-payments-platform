"""Explicit Raw v2 cloud adapters. No cloud calls at import time."""
import importlib.util
from pathlib import Path

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('raw_v2_contract',HERE/'raw_v2.py')
raw=importlib.util.module_from_spec(spec)
spec.loader.exec_module(raw)

class GCSConditionalStore:
    """Uses GCS generation precondition, not check-then-put."""
    def __init__(self,bucket):
        self.bucket=bucket
    def create_if_absent(self,name,data):
        from google.api_core.exceptions import PreconditionFailed
        blob=self.bucket.blob(name)
        try:
            blob.upload_from_string(data,content_type='application/avro',if_generation_match=0)
            return True
        except PreconditionFailed:
            return False
    def read(self,name):
        return self.bucket.blob(name).download_as_bytes()

class PubSubAcknowledger:
    def __init__(self,subscriber,subscription):
        self.subscriber=subscriber
        self.subscription=subscription
    def ack(self,ack_id):
        self.subscriber.acknowledge(request={'subscription':self.subscription,'ack_ids':[ack_id]})

class RegistryDecoder:
    def __init__(self,registry):
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        self.deserializer=AvroDeserializer(registry)
    def __call__(self,wire,topic):
        from confluent_kafka.serialization import SerializationContext,MessageField
        return self.deserializer(wire,SerializationContext(topic,MessageField.VALUE))

def process_message(data,ack_id,store,acker,decoder):
    return raw.process_raw_v2(data,ack_id,store,acker,decoder)
