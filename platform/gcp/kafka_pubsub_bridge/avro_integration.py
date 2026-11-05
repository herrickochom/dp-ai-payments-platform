"""Offline Confluent-wire-format Avro decoding and fail-closed contract routing.

No Kafka consumer, Pub/Sub client, cloud network calls or offset commits.
An in-memory registry of explicit schema IDs is used ONLY for local tests.
"""
import io
import json
import struct
from pathlib import Path
from typing import Mapping
from fastavro import parse_schema, schemaless_reader, schemaless_writer

MAGIC = b'\x00'
BUSINESS = 'com.payments.platform.PaymentEvent'
TECHNICAL = 'com.payments.platform.TechnicalPaymentEvent'

class WireContractError(ValueError):
    pass

def full_name(schema):
    return (schema.get('namespace', '') + '.' + schema['name']).lstrip('.')

def encode_wire(schema: dict, record: dict, schema_id: int) -> bytes:
    if not 1 <= schema_id <= 0xffffffff:
        raise WireContractError('invalid schema id')
    output = io.BytesIO()
    schemaless_writer(output, parse_schema(schema), record)
    return MAGIC + struct.pack('>I', schema_id) + output.getvalue()

def decode_wire(payload: bytes, schemas_by_id: Mapping[int, dict]):
    if not isinstance(payload, bytes) or len(payload) < 6 or payload[:1] != MAGIC:
        raise WireContractError('invalid Confluent Avro wire header')
    schema_id = struct.unpack('>I', payload[1:5])[0]
    schema = schemas_by_id.get(schema_id)
    if schema is None:
        raise WireContractError('unapproved or unknown schema id')
    buf = io.BytesIO(payload[5:])
    try:
        record = schemaless_reader(buf, parse_schema(schema))
    except Exception as exc:
        raise WireContractError('invalid Avro payload') from exc
    if buf.read(1):
        raise WireContractError('unexpected trailing Avro bytes')
    if not isinstance(record, dict):
        raise WireContractError('decoded record is not an object')
    return schema_id, full_name(schema), record

def assess_wire(payload: bytes, schemas_by_id: Mapping[int, dict]):
    """Decode actual Avro; retain mapping block for unapproved source contracts."""
    from importlib.util import module_from_spec, spec_from_file_location
    module_path = Path(__file__).with_name('source_mapping.py')
    spec = spec_from_file_location('kp001f_mapping_for_g', module_path)
    mod = module_from_spec(spec)
    # dataclass uses sys.modules lookup during class creation
    import sys
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    schema_id, schema_name, record = decode_wire(payload, schemas_by_id)
    assessment = mod.assess(schema_name, record)
    return schema_id, record, assessment
