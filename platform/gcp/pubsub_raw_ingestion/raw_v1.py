"""Raw v1 offline core; cloud adapter and Schema Registry decoder are injected."""
import base64
import hashlib
import io
import json
from datetime import datetime, timezone
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

SCHEMA_JSON = '{"type":"record","name":"RawSourceEvent","namespace":"com.dp.ai.raw","fields":[{"name":"kafka_topic","type":"string"},{"name":"kafka_partition","type":"int"},{"name":"kafka_offset","type":"long"},{"name":"kafka_timestamp_ms","type":"long"},{"name":"schema_registry_id","type":"int"},{"name":"payload_sha256","type":"string"},{"name":"payload_wire","type":"bytes"}]}'
TOPIC_MAP = {'icmn.vpm.pain001': {'category': 'icmn', 'source_group': 'icmn', 'system': 'vpm', 'msg_type': 'pain001', 'processing_mode': 'real-time'}, 'icmn.pmn.pain001': {'category': 'icmn', 'source_group': 'icmn', 'system': 'pmn', 'msg_type': 'technical_payment_event', 'processing_mode': 'real-time'}, 'cpo.psn.pain002': {'category': 'cpo', 'source_group': 'cpo', 'system': 'psn', 'msg_type': 'pain002', 'processing_mode': 'real-time'}, 'cpo.plm.pain002': {'category': 'cpo', 'source_group': 'cpo', 'system': 'plm', 'msg_type': 'technical_payment_event', 'processing_mode': 'real-time'}, 'wendi.camt052': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'camt052', 'processing_mode': 'streaming'}, 'wendi.camt053': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'camt053', 'processing_mode': 'streaming'}, 'wendi.camt054': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'camt054', 'processing_mode': 'streaming'}, 'wendi.transactions': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'transactions', 'processing_mode': 'streaming'}, 'wendi.pain001': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'pain001', 'processing_mode': 'real-time'}, 'wendi.pain002': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'pain002', 'processing_mode': 'real-time'}, 'mobile.mtn.pacs008': {'category': 'mobile_networks', 'source_group': 'mobile', 'system': 'mtn', 'msg_type': 'pacs008', 'processing_mode': 'streaming'}, 'mobile.mtn.pacs002': {'category': 'mobile_networks', 'source_group': 'mobile', 'system': 'mtn', 'msg_type': 'pacs002', 'processing_mode': 'real-time'}, 'mobile.airtel.pacs008': {'category': 'mobile_networks', 'source_group': 'mobile', 'system': 'airtel', 'msg_type': 'pacs008', 'processing_mode': 'real-time'}, 'mobile.airtel.pacs002': {'category': 'mobile_networks', 'source_group': 'mobile', 'system': 'airtel', 'msg_type': 'pacs002', 'processing_mode': 'real-time'}, 'agent.transactions': {'category': 'agent_network', 'source_group': 'agent', 'system': 'agent', 'msg_type': 'transactions', 'processing_mode': 'streaming'}, 'agent.profiles': {'category': 'agent_network', 'source_group': 'agent', 'system': 'agent', 'msg_type': 'profiles', 'processing_mode': 'batch'}, 'agent.locations': {'category': 'agent_network', 'source_group': 'agent', 'system': 'agent', 'msg_type': 'locations', 'processing_mode': 'streaming'}, 'pdmis.beneficiaries': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'beneficiaries', 'processing_mode': 'batch'}, 'pdmis.business_plans': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'business_plans', 'processing_mode': 'batch'}, 'pdmis.households': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'households', 'processing_mode': 'batch'}, 'pdmis.loans': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'loans', 'processing_mode': 'batch'}, 'pdmis.repayments': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'repayments', 'processing_mode': 'streaming'}, 'pdmis.saccos': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'saccos', 'processing_mode': 'batch'}, 'pdmis.special_groups': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'special_groups', 'processing_mode': 'batch'}}
SCHEMA = avro.schema.parse(SCHEMA_JSON)

class ContractError(ValueError): pass
class CollisionError(RuntimeError): pass

def transport(data):
    try:
        obj=json.loads(data)
        if type(obj) is not dict or set(obj)!={'kafka_topic','kafka_partition','kafka_offset','kafka_timestamp_ms','schema_registry_id','payload_base64','payload_sha256'}:
            raise ValueError('fields')
        if type(obj['kafka_topic']) is not str or obj['kafka_topic'] not in TOPIC_MAP: raise ValueError('topic')
        if any(type(obj[k]) is not int or obj[k]<0 for k in ('kafka_partition','kafka_offset','kafka_timestamp_ms')): raise ValueError('coordinates')
        if type(obj['schema_registry_id']) is not int or obj['schema_registry_id']<1: raise ValueError('schema id')
        wire=base64.b64decode(obj['payload_base64'],validate=True)
        if len(wire)<=5 or wire[0]!=0 or int.from_bytes(wire[1:5],'big')!=obj['schema_registry_id']: raise ValueError('wire framing')
        if hashlib.sha256(wire).hexdigest()!=obj['payload_sha256']: raise ValueError('digest')
        return obj,wire
    except (ValueError,TypeError,KeyError,UnicodeError,OverflowError) as exc:
        raise ContractError('invalid transport') from exc

def name_for(obj):
    info=TOPIC_MAP[obj['kafka_topic']]
    dt=datetime.fromtimestamp(obj['kafka_timestamp_ms']/1000,timezone.utc)
    return (f"raw/v1/{info['processing_mode']}/category={info['category']}/source_group={info['source_group']}/"
            f"source_system={info['system']}/year={dt:%Y}/month={dt:%m}/day={dt:%d}/"
            f"topic={obj['kafka_topic']}/partition={obj['kafka_partition']}/offset={obj['kafka_offset']}/record.avro")

def record_for(event, obj):
    raise ContractError(
        "lossy business-event projection is prohibited in Raw v1"
    )

def ocf_encode(record):
    from fastavro import parse_schema, writer

    schema = parse_schema(json.loads(SCHEMA_JSON))
    buffer = io.BytesIO()
    writer(buffer, schema, [record])
    data = buffer.getvalue()

    if not data.startswith(bytes((79, 98, 106, 1))):
        raise ContractError("Avro OCF magic mismatch")

    return data

def ocf_read(data):
    from fastavro import reader

    records = list(reader(io.BytesIO(data)))

    if len(records) != 1:
        raise CollisionError("not exactly one Avro datum")

    return records[0]

def process_raw_v1(data, ack_id, store, acker, decoder):
    obj, wire = transport(data)

    event = decoder(wire, obj["kafka_topic"])

    if (
        type(event) is not dict
        or type(event.get("event_id")) is not str
        or not event["event_id"]
    ):
        raise ContractError("invalid decoded source event")

    record = {
        "kafka_topic": obj["kafka_topic"],
        "kafka_partition": obj["kafka_partition"],
        "kafka_offset": obj["kafka_offset"],
        "kafka_timestamp_ms": obj["kafka_timestamp_ms"],
        "schema_registry_id": obj["schema_registry_id"],
        "payload_sha256": obj["payload_sha256"],
        "payload_wire": wire,
    }

    candidate = ocf_encode(record)
    name = name_for(obj)

    created = store.create_if_absent(name, candidate)

    try:
        actual = ocf_read(store.read(name))
    except Exception as exc:
        raise CollisionError("stored OCF invalid") from exc

    if actual != record:
        raise CollisionError(
            "immutable Kafka coordinate collision"
        )

    acker.ack(ack_id)

    return (
        name,
        "CREATED" if created
        else "ALREADY_PRESENT_IDENTICAL"
    )
