"""Explicit Raw v1 cloud adapters. No cloud calls at import time."""
import importlib.util
from pathlib import Path

HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('raw_v1_contract',HERE/'raw_v1.py')
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
    """Schema Registry-backed decoder requiring one complete Avro datum."""

    def __init__(self, registry):
        from confluent_kafka.schema_registry.avro import AvroDeserializer
        self.registry = registry
        self.deserializer = AvroDeserializer(registry)
        self._parsed_schemas = {}

    def __call__(self, wire, topic):
        import io
        import json
        from fastavro import parse_schema, schemaless_reader
        from confluent_kafka.serialization import (
            SerializationContext,
            MessageField,
        )

        if type(wire) is not bytes or len(wire) <= 5 or wire[0] != 0:
            raise ValueError("invalid Confluent Avro wire framing")

        schema_id = int.from_bytes(wire[1:5], "big")
        if schema_id < 1:
            raise ValueError("invalid Schema Registry ID")

        schema = self._parsed_schemas.get(schema_id)
        if schema is None:
            registered = self.registry.get_schema(schema_id)
            schema_text = registered.schema_str
            schema = parse_schema(json.loads(schema_text))
            self._parsed_schemas[schema_id] = schema

        # Independently verify that the writer schema consumes the
        # complete Avro payload, including its final byte.
        stream = io.BytesIO(wire[5:])
        schemaless_reader(stream, schema)

        if stream.read(1):
            raise ValueError("unexpected trailing Avro bytes")

        # Preserve the existing Confluent deserialization behaviour.
        return self.deserializer(
            wire,
            SerializationContext(topic, MessageField.VALUE),
        )

def process_message(data,ack_id,store,acker,decoder):
    return raw.process_raw_v1(data,ack_id,store,acker,decoder)
