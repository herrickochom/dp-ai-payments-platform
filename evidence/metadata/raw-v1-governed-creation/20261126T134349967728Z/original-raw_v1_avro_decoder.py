
"""Offline decoder for governed lossless Raw v1 Avro objects."""

import ast
import base64
import hashlib
import io
import json
from pathlib import Path

from fastavro import reader
from fastavro.schema import to_parsing_canonical_form

ROOT = Path(__file__).resolve().parents[2]


def load_schema(root, filename, variable):
    tree = ast.parse((Path(root) / filename).read_bytes())

    values = [
        node.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == variable
            for t in node.targets
        )
    ]

    if len(values) != 1:
        raise ValueError("Authoritative schema not found")

    schema = ast.literal_eval(values[0])

    return json.loads(schema) if isinstance(schema, str) else schema


class RawAvroDecoder:

    def __init__(self, contract, root=ROOT):
        self.contract = contract

        self.schemas = {
            "RawSourceEvent": load_schema(
                root,
                "platform/gcp/pubsub_raw_ingestion/raw_v1.py",
                "SCHEMA_JSON"
            ),
            "CanonicalSourceEnvelopeV1": load_schema(
                root,
                "platform/gcp/kafka_pubsub_bridge/source_runtime_adapter_v1.py",
                "AVRO_SCHEMA"
            )
        }

        self.fingerprints = {
            name: to_parsing_canonical_form(schema)
            for name, schema in self.schemas.items()
        }

    def decode(self, raw_bytes):
        if not isinstance(raw_bytes, bytes):
            raise ValueError("Raw object must be bytes")

        if not raw_bytes.startswith(b"Obj\x01"):
            raise ValueError("Invalid Avro object container")

        try:
            stream = io.BytesIO(raw_bytes)
            container = reader(stream)

            fingerprint = to_parsing_canonical_form(
                container.writer_schema
            )

            matches = [
                name for name, expected in self.fingerprints.items()
                if fingerprint == expected
            ]

            if len(matches) != 1:
                raise ValueError("Unknown Raw envelope schema")

            records = iter(container)
            record = next(records, None)

            if record is None:
                raise ValueError("Empty Raw object")

            if next(records, None) is not None:
                raise ValueError("Multiple records in Raw object")

            if stream.tell() != len(raw_bytes):
                raise ValueError("Unconsumed Avro bytes")

        except Exception as exc:
            raise ValueError("Raw Avro decoding rejected") from exc

        name = matches[0]
        legacy = name == "RawSourceEvent"

        payload = record[
            "payload_wire" if legacy else "payload_bytes"
        ]

        if not isinstance(payload, bytes):
            raise ValueError("Invalid source payload")

        digest = hashlib.sha256(payload).hexdigest()

        if digest != record["payload_sha256"]:
            raise ValueError("Source payload digest mismatch")

        topic = record["kafka_topic"]
        route = self.contract.routes.get(topic)

        if route is None:
            raise ValueError("Unknown source topic")

        if route["status"] != "legacy_payment_event":
            raise ValueError("Source admission not authorised")

        for key in (
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp_ms"
        ):
            if type(record[key]) is not int or record[key] < 0:
                raise ValueError("Invalid Kafka coordinates")

        payload_format = (
            "confluent_avro"
            if legacy else record["payload_format"]
        )

        processing_mode = (
            route["processing_mode"]
            if legacy else record["processing_mode"]
        )

        value_is_null = (
            False if legacy else record["value_is_null"]
        )

        is_tombstone = (
            False if legacy else record["is_tombstone"]
        )

        if processing_mode != route["processing_mode"]:
            raise ValueError("Processing mode mismatch")

        if value_is_null or is_tombstone:
            raise ValueError("Tombstone not admitted by this decoder")

        schema_id = record["schema_registry_id"]

        if payload_format == "confluent_avro":
            if len(payload) < 5 or payload[0] != 0:
                raise ValueError("Invalid Confluent wire framing")

            wire_id = int.from_bytes(payload[1:5], "big")

            if type(schema_id) is not int or wire_id != schema_id:
                raise ValueError("Schema registry ID mismatch")

        envelope = {
            "message_type": "payments.source-event.v1",
            "kafka_topic": topic,
            "kafka_partition": record["kafka_partition"],
            "kafka_offset": record["kafka_offset"],
            "kafka_timestamp_ms": record["kafka_timestamp_ms"],
            "payload_base64": base64.b64encode(payload).decode("ascii"),
            "payload_sha256": digest,
            "schema_registry_id": schema_id,
            "payload_format": payload_format,
            "processing_mode": processing_mode,
            "value_is_null": value_is_null
        }

        self.contract.envelope.validate_envelope(envelope)

        return {
            "envelope_name": name,
            "source_envelope": envelope,
            "payload_bytes": payload,
            "payload_sha256": digest,
            "raw_object_sha256": hashlib.sha256(raw_bytes).hexdigest()
        }

    def derive_metadata(self, object_name, raw_bytes):
        decoded = self.decode(raw_bytes)

        metadata = self.contract.derive(
            object_name,
            decoded["source_envelope"]
        )

        return metadata, decoded
