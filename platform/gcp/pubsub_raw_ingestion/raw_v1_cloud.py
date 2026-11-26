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
    """Strict decoder for registered Confluent Avro wire records."""

    def __init__(self, registry):
        self.registry = registry
        self._schemas = {}

    def __call__(self, wire, topic):
        import io
        import json
        from fastavro import parse_schema, schemaless_reader

        if not isinstance(wire, (bytes, bytearray)):
            raise ValueError("Avro wire payload must be bytes")

        if len(wire) < 5 or wire[0] != 0:
            raise ValueError("Invalid Confluent Avro wire header")

        schema_id = int.from_bytes(wire[1:5], "big")

        if schema_id <= 0:
            raise ValueError("Invalid schema registry ID")

        if schema_id not in self._schemas:
            registered = self.registry.get_schema(schema_id)

            if registered.schema_type != "AVRO":
                raise ValueError("Registered schema is not AVRO")

            if getattr(registered, "references", None):
                raise ValueError(
                    "Referenced Avro schemas require explicit resolution"
                )

            schema = json.loads(registered.schema_str)

            self._schemas[schema_id] = parse_schema(schema)

        stream = io.BytesIO(wire[5:])

        event = schemaless_reader(
            stream,
            self._schemas[schema_id]
        )

        if stream.read(1):
            raise ValueError("Unexpected trailing Avro bytes")

        return event

def process_message(data,ack_id,store,acker,decoder):
    return raw.process_raw_v1(data,ack_id,store,acker,decoder)
