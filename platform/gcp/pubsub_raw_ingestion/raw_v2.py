"""Raw v2 offline core; cloud adapter and Schema Registry decoder are injected."""
import base64
import hashlib
import io
import json
from datetime import datetime, timezone
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

SCHEMA_JSON = '{"type": "record", "name": "PaymentEvent", "namespace": "com.dp.ai.payment", "fields": [{"name": "event_id", "type": "string"}, {"name": "message_id", "type": ["null", "string"], "default": null}, {"name": "timestamp", "type": "string"}, {"name": "source_system", "type": "string"}, {"name": "message_type", "type": "string"}, {"name": "event_family", "type": "string", "default": "PAYMENT_BUSINESS_EVENT"}, {"name": "payload", "type": {"type": "record", "name": "Payload", "fields": [{"name": "amount", "type": ["double", "null"]}, {"name": "currency", "type": ["string", "null"]}, {"name": "debtor", "type": ["string", "null"]}, {"name": "creditor", "type": ["string", "null"]}, {"name": "status", "type": ["string", "null"]}, {"name": "x_attributes", "type": {"type": "map", "values": "string"}, "default": {}}]}}, {"name": "event_data", "type": ["null", "string"], "default": null}, {"name": "parsed_event_data", "type": ["null", "string"], "default": null}, {"name": "_kafka_metadata", "type": {"type": "record", "name": "KafkaMetadata", "fields": [{"name": "topic", "type": "string"}, {"name": "partition", "type": "int"}, {"name": "offset", "type": "long"}, {"name": "timestamp", "type": "string"}, {"name": "category", "type": ["string", "null"]}, {"name": "source_group", "type": ["string", "null"]}, {"name": "source_system", "type": ["string", "null"]}]}}]}'
TOPIC_MAP = {'icmn.vpm.pain001': {'category': 'icmn', 'source_group': 'icmn', 'system': 'vpm', 'msg_type': 'pain001'}, 'icmn.pmn.pain001': {'category': 'icmn', 'source_group': 'icmn', 'system': 'pmn', 'msg_type': 'technical_payment_event'}, 'cpo.psn.pain002': {'category': 'cpo', 'source_group': 'cpo', 'system': 'psn', 'msg_type': 'pain002'}, 'cpo.plm.pain002': {'category': 'cpo', 'source_group': 'cpo', 'system': 'plm', 'msg_type': 'technical_payment_event'}, 'wendi.camt052': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'camt052'}, 'wendi.camt053': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'camt053'}, 'wendi.camt054': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'camt054'}, 'wendi.transactions': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'transactions'}, 'wendi.pain001': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'pain001'}, 'wendi.pain002': {'category': 'wendi', 'source_group': 'wendi', 'system': 'wendi', 'msg_type': 'pain002'}, 'mobile.mtn.pacs008': {'category': 'mobile_networks', 'source_group': 'mobile', 'system': 'mtn', 'msg_type': 'pacs008'}, 'mobile.mtn.pacs002': {'category': 'mobile_networks', 'source_group': 'mobile', 'system': 'mtn', 'msg_type': 'pacs002'}, 'mobile.airtel.pacs008': {'category': 'mobile_networks', 'source_group': 'mobile', 'system': 'airtel', 'msg_type': 'pacs008'}, 'mobile.airtel.pacs002': {'category': 'mobile_networks', 'source_group': 'mobile', 'system': 'airtel', 'msg_type': 'pacs002'}, 'agent.transactions': {'category': 'agent_network', 'source_group': 'agent', 'system': 'agent', 'msg_type': 'transactions'}, 'agent.profiles': {'category': 'agent_network', 'source_group': 'agent', 'system': 'agent', 'msg_type': 'profiles'}, 'agent.locations': {'category': 'agent_network', 'source_group': 'agent', 'system': 'agent', 'msg_type': 'locations'}, 'pdmis.beneficiaries': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'beneficiaries'}, 'pdmis.business_plans': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'business_plans'}, 'pdmis.households': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'households'}, 'pdmis.loans': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'loans'}, 'pdmis.repayments': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'repayments'}, 'pdmis.saccos': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'saccos'}, 'pdmis.special_groups': {'category': 'pdmis', 'source_group': 'pdmis', 'system': 'pdmis', 'msg_type': 'special_groups'}}
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
    return (f"raw/v2/category={info['category']}/source_group={info['source_group']}/"
            f"source_system={info['system']}/year={dt:%Y}/month={dt:%m}/day={dt:%d}/"
            f"topic={obj['kafka_topic']}/partition={obj['kafka_partition']}/offset={obj['kafka_offset']}/record.avro")

def record_for(event,obj):
    if type(event) is not dict or type(event.get('event_id')) is not str or not event['event_id']:
        raise ContractError('decoded source event requires event_id')
    info=TOPIC_MAP[obj['kafka_topic']]
    dt=datetime.fromtimestamp(obj['kafka_timestamp_ms']/1000,timezone.utc)
    parsed_data=event.get('parsed_event_data')
    try: parsed=json.loads(parsed_data) if parsed_data else {}
    except (ValueError,TypeError): parsed={}
    if type(parsed) is not dict: parsed={}
    payload=parsed.get('payload',{})
    if type(payload) is not dict: payload={}
    attributes=event.get('x_attributes') or parsed.get('x_attributes',{})
    if type(attributes) is not dict or any(type(k) is not str or type(v) is not str for k,v in attributes.items()):
        raise ContractError('invalid x_attributes')
    amount=event.get('instructed_amount')
    if amount is not None and type(amount) not in (float,int): raise ContractError('invalid instructed_amount')
    return {'event_id':event['event_id'],'message_id':event.get('message_id'),
      'timestamp':event.get('timestamp',dt.isoformat()),
      'source_system':event.get('source_system') or info['system'],
      'message_type':event.get('message_type',info['msg_type']),
      'event_family':event.get('event_family','PAYMENT_BUSINESS_EVENT'),
      'payload':{'amount':amount,'currency':event.get('currency'),
       'debtor':payload.get('debtor'),'creditor':payload.get('creditor'),
       'status':payload.get('transaction_status') or payload.get('status'),
       'x_attributes':attributes},
      'event_data':event.get('event_data'),'parsed_event_data':parsed_data,
      '_kafka_metadata':{'topic':obj['kafka_topic'],'partition':obj['kafka_partition'],
       'offset':obj['kafka_offset'],'timestamp':dt.isoformat(),
       'category':info['category'],'source_group':info['source_group'],'source_system':info['system']}}

def ocf_encode(record):
    buf=io.BytesIO(); writer=DataFileWriter(buf,DatumWriter(),SCHEMA)
    try:
        writer.append(record); writer.flush(); data=buf.getvalue()
    finally: writer.close()
    if data[:4]!=bytes((79,98,106,1)): raise ContractError('Avro OCF magic mismatch')
    return data

def ocf_read(data):
    reader=DataFileReader(io.BytesIO(data),DatumReader())
    try: records=list(reader)
    finally: reader.close()
    if len(records)!=1: raise CollisionError('not exactly one Avro datum')
    return records[0]

def process_raw_v2(data,ack_id,store,acker,decoder):
    """decoder(wire,topic) must be Schema Registry-backed; store.create_if_absent must be atomic."""
    obj,wire=transport(data)
    record=record_for(decoder(wire,obj['kafka_topic']),obj)
    candidate=ocf_encode(record)
    name=name_for(obj)
    created=store.create_if_absent(name,candidate)
    try: actual=ocf_read(store.read(name))
    except Exception as exc: raise CollisionError('stored OCF invalid') from exc
    if actual!=record: raise CollisionError('immutable Kafka coordinate collision')
    acker.ack(ack_id)
    return name,'CREATED' if created else 'ALREADY_PRESENT_IDENTICAL'
