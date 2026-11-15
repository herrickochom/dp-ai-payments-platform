"""Lossless, fail-closed governed Raw v1 Avro OCF encoder."""

import base64
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path
import importlib.util

from fastavro import parse_schema, reader, writer


class RawEncodingError(ValueError):
    pass


def load_module(filename, name):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


governance = load_module(
    "raw_topic_governance_v1.py", "raw_governance_contract"
)
envelopes = load_module(
    "source_envelope_v1.py", "raw_source_envelope_contract"
)



def prepare(envelope):
    """Encode governed non-legacy source events using canonical Raw v1."""
    if not isinstance(envelope, dict):
        raise RawEncodingError("envelope must be a dictionary")

    validated = envelopes.validate_envelope(envelope)
    topic = validated["topic"]
    route = governance.route_for(topic)

    if route["status"] != "contract_only":
        raise RawEncodingError(
            "legacy writer owns this topic or route is not eligible"
        )

    adapter = load_module(
        "source_runtime_adapter_v1.py",
        "canonical_adapter_for_governed_raw",
    )

    name, body = adapter.prepare_object(envelope)

    decoded = list(reader(io.BytesIO(body)))

    if len(decoded) != 1:
        raise RawEncodingError("canonical Avro record count mismatch")

    expected = decoded[0]

    if expected.get("kafka_topic") != topic:
        raise RawEncodingError("canonical topic mismatch")

    if expected.get("payload_sha256") != validated["payload_sha256"]:
        raise RawEncodingError("canonical payload hash mismatch")

    if expected.get("processing_mode") != route["processing_mode"]:
        raise RawEncodingError("canonical processing mode mismatch")

    return name, body, expected
