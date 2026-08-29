#!/usr/bin/env python3
"""
Payment Event Producer - Reads source XML/JSON files and produces to PDM platform Kafka topics.

Source domains supported:
- ICMN, CPO, Wendi, Mobile Networks, Agent Network and PDMIS
- Reconciliation topics are intentionally excluded because they are downstream-derived events.
"""

import os
import sys
import glob
import logging
import time
import uuid
import json
from pathlib import Path
from typing import Dict, Any, Optional
from confluent_kafka import SerializingProducer, KafkaError
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import avro.schema
import xml.etree.ElementTree as ET

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    DATA_ROOT = os.getenv("DATA_ROOT", "/app/data")
    SCHEMA_PATH = os.getenv("SCHEMA_PATH", "/app/schemas/payment_event.avsc")
    
    # Producer settings
    PRODUCER_IDEMPOTENCE = os.getenv("PRODUCER_IDEMPOTENCE", "true").lower() == "true"
    PRODUCER_ACKS = os.getenv("PRODUCER_ACKS", "all")
    PRODUCER_RETRIES = int(os.getenv("PRODUCER_RETRIES", "5"))

# ------------------------------------------------------------------------------
# Topic to System Mapping
# ------------------------------------------------------------------------------
TOPIC_MAPPINGS = {
    # ICMN
    "icmn.vpm.pain001": {"category": "icmn", "system": "vpm", "msg_type": "pain001", "description": "VPM Payment Initiation", "has_technical_attrs": False},
    "icmn.pmn.pain001": {"category": "icmn", "system": "pmn", "msg_type": "pain001", "description": "PMN Payment Initiation", "has_technical_attrs": True},

    # CPO
    "cpo.psn.pain002": {"category": "cpo", "system": "psn", "msg_type": "pain002", "description": "PSN Payment Status", "has_technical_attrs": False},
    "cpo.plm.pain002": {"category": "cpo", "system": "plm", "msg_type": "pain002", "description": "PLM Payment Lifecycle", "has_technical_attrs": True},

    # Wendi
    "wendi.pain001": {"category": "wendi", "system": "wendi", "msg_type": "pain001", "description": "Wendi Payment Initiation", "has_technical_attrs": False},
    "wendi.pain002": {"category": "wendi", "system": "wendi", "msg_type": "pain002", "description": "Wendi Payment Status", "has_technical_attrs": True},
    "wendi.camt052": {"category": "wendi", "system": "wendi", "msg_type": "camt052", "description": "Wendi Account Report", "has_technical_attrs": False},
    "wendi.camt053": {"category": "wendi", "system": "wendi", "msg_type": "camt053", "description": "Wendi Account Statement", "has_technical_attrs": False},
    "wendi.camt054": {"category": "wendi", "system": "wendi", "msg_type": "camt054", "description": "Wendi Debit/Credit Notification", "has_technical_attrs": False},
    "wendi.transactions": {"category": "wendi", "system": "wendi", "msg_type": "transactions", "description": "Wendi Transactions", "has_technical_attrs": False},

    # Mobile Networks
    "mobile.mtn.pacs008": {"category": "mobile_networks", "system": "mtn", "msg_type": "pacs008", "description": "MTN FI-to-FI Customer Credit Transfer", "has_technical_attrs": False},
    "mobile.mtn.pacs002": {"category": "mobile_networks", "system": "mtn", "msg_type": "pacs002", "description": "MTN Payment Status", "has_technical_attrs": False},
    "mobile.airtel.pacs008": {"category": "mobile_networks", "system": "airtel", "msg_type": "pacs008", "description": "Airtel FI-to-FI Customer Credit Transfer", "has_technical_attrs": False},
    "mobile.airtel.pacs002": {"category": "mobile_networks", "system": "airtel", "msg_type": "pacs002", "description": "Airtel Payment Status", "has_technical_attrs": False},

    # Agent Network
    "agent.profiles": {"category": "agent_network", "system": "agent", "msg_type": "profiles", "description": "Agent Profiles", "has_technical_attrs": False},
    "agent.locations": {"category": "agent_network", "system": "agent", "msg_type": "locations", "description": "Agent Locations", "has_technical_attrs": False},
    "agent.transactions": {"category": "agent_network", "system": "agent", "msg_type": "transactions", "description": "Agent Transactions", "has_technical_attrs": False},

    # PDMIS
    "pdmis.beneficiaries": {"category": "pdmis", "system": "pdmis", "msg_type": "beneficiaries", "description": "PDMIS Beneficiaries", "has_technical_attrs": False},
    "pdmis.business_plans": {"category": "pdmis", "system": "pdmis", "msg_type": "business_plans", "description": "PDMIS Business Plans", "has_technical_attrs": False},
    "pdmis.households": {"category": "pdmis", "system": "pdmis", "msg_type": "households", "description": "PDMIS Households", "has_technical_attrs": False},
    "pdmis.loans": {"category": "pdmis", "system": "pdmis", "msg_type": "loans", "description": "PDMIS Loans", "has_technical_attrs": False},
    "pdmis.saccos": {"category": "pdmis", "system": "pdmis", "msg_type": "saccos", "description": "PDMIS SACCOs", "has_technical_attrs": False},
    "pdmis.special_groups": {"category": "pdmis", "system": "pdmis", "msg_type": "special_groups", "description": "PDMIS Special Groups", "has_technical_attrs": False},
}


# ------------------------------------------------------------------------------
# Avro Schema (for validation)
# ------------------------------------------------------------------------------
def load_avro_schema():
    """Load Avro schema from file"""
    try:
        with open(Config.SCHEMA_PATH, "r") as f:
            schema_str = f.read()
        return avro.schema.parse(schema_str)
    except FileNotFoundError:
        # Fallback schema if file doesn't exist
        logger.warning(f"Schema file not found: {Config.SCHEMA_PATH}, using fallback")
        return avro.schema.parse("""
        {
            "type": "record",
            "name": "PaymentEvent",
            "namespace": "com.dp.ai.payment",
            "fields": [
                {"name": "event_id", "type": "string"},
                {"name": "message_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "source_system", "type": "string"},
                {"name": "source_key", "type": "string"},
                {"name": "instructed_amount", "type": "double"},
                {"name": "currency", "type": "string"},
                {"name": "creation_date", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "event_data", "type": ["null", "string"], "default": null},
                {"name": "payload", "type": ["null", "string"], "default": null},
                {"name": "x_attributes", "type": ["null", {"type": "map", "values": "string"}], "default": null},
                {"name": "parsed_event_data", "type": ["null", "string"], "default": null}
            ]
        }
        """)

# ------------------------------------------------------------------------------
# XML Parsing
# ------------------------------------------------------------------------------
def xml_to_dict(element: ET.Element) -> Any:
    """Project every XML element and attribute to JSON-friendly values."""
    children = list(element)
    value: Dict[str, Any] = {}

    if element.attrib:
        value["_attributes"] = dict(element.attrib)

    for child in children:
        name = child.tag.rsplit('}', 1)[-1]
        child_value = xml_to_dict(child)
        if name in value:
            value[name] = value[name] if isinstance(value[name], list) else [value[name]]
            value[name].append(child_value)
        else:
            value[name] = child_value

    text = (element.text or "").strip()
    if text:
        if value:
            value["_text"] = text
        else:
            return text

    return value


def parse_pain001_xml(file_path: str) -> Dict[str, Any]:
    """Parse PAIN.001 XML file and extract payment data"""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Extract namespace
        ns = {'ns': 'urn:iso:std:iso:20022:tech:xsd:pain.001.001.09'}
        
        # Extract data
        event = {
            "event_id": str(uuid.uuid4()),
            "message_id": root.find('.//ns:MsgId', ns),
            "creation_date": root.find('.//ns:CreDtTm', ns),
            "instructed_amount": root.find('.//ns:InstdAmt', ns),
            "currency": root.find('.//ns:InstdAmt', ns),
            "debtor": root.find('.//ns:Dbtr/ns:Nm', ns),
            "creditor": root.find('.//ns:Cdtr/ns:Nm', ns),
        }
        
        # Convert to dict
        result = {}
        for key, value in event.items():
            if key == "event_id":
                result[key] = value
                continue
            if value is not None:
                if key == "instructed_amount":
                    result[key] = float(value.text) if value.text else 0.0
                elif key == "currency":
                    result[key] = value.get('Ccy') if hasattr(value, 'get') else "GBP"
                else:
                    result[key] = value.text if value.text else ""
            else:
                result[key] = "" if key not in ["instructed_amount"] else 0.0
        
        result["x_attributes"] = {
            element.tag.rsplit('}', 1)[-1]: element.text or ""
            for element in root.iter()
            if element.tag.rsplit('}', 1)[-1].startswith('x-')
        }
        result["xml"] = {root.tag.rsplit('}', 1)[-1]: xml_to_dict(root)}
        return result
    except Exception as e:
        logger.error(f"Error parsing XML {file_path}: {e}")
        return {}

def parse_pain002_xml(file_path: str) -> Dict[str, Any]:
    """Parse PAIN.002 XML into a header, business payload and x-* attributes."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        ns = {'ns': 'urn:iso:std:iso:20022:tech:xsd:pain.002.001.12'}
        
        def text(path: str) -> str:
            element = root.find(path, ns)
            return element.text if element is not None and element.text else ""

        x_attributes = {
            element.tag.rsplit('}', 1)[-1]: element.text or ""
            for element in root.iter()
            if element.tag.rsplit('}', 1)[-1].startswith('x-')
        }
        return {
            "header": {
                "message_id": text('.//ns:GrpHdr/ns:MsgId'),
                "creation_date": text('.//ns:GrpHdr/ns:CreDtTm'),
                "initiating_party": text('.//ns:GrpHdr/ns:InitgPty'),
                "original_message_id": text('.//ns:OrgnlGrpInfAndSts/ns:OrgnlMsgId'),
                "original_message_type": text('.//ns:OrgnlGrpInfAndSts/ns:OrgnlMsgNmId'),
                "group_status": text('.//ns:OrgnlGrpInfAndSts/ns:GrpSts'),
            },
            "payload": {
                "transaction_status": text('.//ns:OrgnlPmtInfAndSts/ns:TxSts'),
                "reason_code": text('.//ns:OrgnlPmtInfAndSts/ns:StsRsnInf/ns:Rsn'),
                "additional_info": text('.//ns:OrgnlPmtInfAndSts/ns:StsRsnInf/ns:AddtlInf'),
                "settlement_status": text('.//ns:OrgnlPmtInfAndSts/ns:SettlementStatus'),
                "business_date": text('.//ns:OrgnlPmtInfAndSts/ns:BusinessDate'),
            },
            "x_attributes": x_attributes,
            "xml": {root.tag.rsplit('}', 1)[-1]: xml_to_dict(root)},
        }
    except Exception as e:
        logger.error(f"Error parsing XML {file_path}: {e}")
        return {}

def parse_generic_xml(file_path: str) -> Dict[str, Any]:
    """Preserve an arbitrary XML document as the canonical nested JSON shape."""
    try:
        root = ET.parse(file_path).getroot()
        return {"xml": {root.tag.rsplit('}', 1)[-1]: xml_to_dict(root)}}
    except Exception as exc:
        logger.error(f"Error parsing XML {file_path}: {exc}")
        return {}

def detect_system_from_path(file_path: str) -> tuple:
    """Detect (system, msg_type, topic) from the source path or filename."""
    path = Path(file_path)
    normalized = path.as_posix().lower()
    name = path.name.lower()

    # Exact entity feeds where filename is the most reliable routing key.
    filename_routes = {
        "agent_profiles.json": ("agent", "profiles", "agent.profiles"),
        "agent_locations.json": ("agent", "locations", "agent.locations"),
        "agent_transactions.json": ("agent", "transactions", "agent.transactions"),
        "beneficiaries.json": ("pdmis", "beneficiaries", "pdmis.beneficiaries"),
        "business_plans.json": ("pdmis", "business_plans", "pdmis.business_plans"),
        "households.json": ("pdmis", "households", "pdmis.households"),
        "loans.json": ("pdmis", "loans", "pdmis.loans"),
        "saccos.json": ("pdmis", "saccos", "pdmis.saccos"),
        "special_groups.json": ("pdmis", "special_groups", "pdmis.special_groups"),
        "transactions.json": ("wendi", "transactions", "wendi.transactions"),
        "wendi_transactions.json": ("wendi", "transactions", "wendi.transactions"),
    }
    if name in filename_routes:
        # Avoid routing an unrelated transactions.json as Wendi.
        if name == "transactions.json" and "/wendi/" not in normalized:
            pass
        else:
            return filename_routes[name]

    # Path markers cover the full source-domain layout.
    route_markers = [
        ("/icmn/vpm/pain001/", ("vpm", "pain001", "icmn.vpm.pain001")),
        ("/icmn/pmn/pain001/", ("pmn", "pain001", "icmn.pmn.pain001")),
        ("/cpo/psn/pain002/", ("psn", "pain002", "cpo.psn.pain002")),
        ("/cpo/plm/pain002/", ("plm", "pain002", "cpo.plm.pain002")),
        ("/wendi/pain001/", ("wendi", "pain001", "wendi.pain001")),
        ("/wendi/pain002/", ("wendi", "pain002", "wendi.pain002")),
        ("/wendi/camt052/", ("wendi", "camt052", "wendi.camt052")),
        ("/wendi/camt053/", ("wendi", "camt053", "wendi.camt053")),
        ("/wendi/camt054/", ("wendi", "camt054", "wendi.camt054")),
        ("/mobile_networks/mtn/pacs008/", ("mtn", "pacs008", "mobile.mtn.pacs008")),
        ("/mobile_networks/mtn/pacs002/", ("mtn", "pacs002", "mobile.mtn.pacs002")),
        ("/mobile_networks/airtel/pacs008/", ("airtel", "pacs008", "mobile.airtel.pacs008")),
        ("/mobile_networks/airtel/pacs002/", ("airtel", "pacs002", "mobile.airtel.pacs002")),
        ("/agent_network/agent_transactions/", ("agent", "transactions", "agent.transactions")),
    ]
    for marker, route in route_markers:
        if marker in normalized:
            return route

    # PDMIS files can be nested below an entity directory with arbitrary filenames.
    pdmis_entities = ("beneficiaries", "business_plans", "households", "loans", "saccos", "special_groups")
    if "/pdmis/" in normalized:
        for entity in pdmis_entities:
            if f"/pdmis/{entity}/" in normalized or f"/{entity}/" in normalized:
                return "pdmis", entity, f"pdmis.{entity}"

    return "unknown", "unknown", "unknown"


def parse_event(file_path: str) -> Optional[Dict[str, Any]]:
    """Parse XML file and create event with proper topic mapping"""
    system, msg_type, topic = detect_system_from_path(file_path)
    
    if topic == "unknown" or topic not in TOPIC_MAPPINGS:
        logger.warning(f"Unknown system/topic for {file_path}, skipping")
        return None
    
    # Parse based on message type
    if "pain001" in msg_type.lower():
        payload = parse_pain001_xml(file_path)
    elif "pain002" in msg_type.lower():
        payload = parse_pain002_xml(file_path)
    else:
        # PACS/CAMT are preserved losslessly here. Message-family-specific
        # projections can be added without changing the raw contract.
        payload = parse_generic_xml(file_path)

    # event_data is the immutable, original source message.  The separate
    # parsed_event_data JSON supports field extraction without altering XML.
    try:
        event_data = Path(file_path).read_text(encoding="utf-8")
    except OSError as exc:
        logger.error(f"Unable to read source XML {file_path}: {exc}")
        return None
    
    # Build event
    event = {
        "event_id": str(uuid.uuid4()),
        "message_id": payload.get(
            "message_id",
            payload.get("header", {}).get(
                "message_id", f"{system.upper()}-{msg_type}-{uuid.uuid4().hex[:8]}"
            ),
        ),
        "event_type": msg_type,
        "source_system": system,
        "source_key": os.path.basename(file_path),
        "source_location": file_path,
        "instructed_amount": payload.get("instructed_amount", 1000.00),
        "currency": payload.get("currency", "GBP"),
        "creation_date": payload.get("creation_date", payload.get("header", {}).get("creation_date", time.strftime("%Y-%m-%dT%H:%M:%S.000Z"))),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "version": "1.0.0",
        "event_data": event_data,
        "parsed_event_data": json.dumps(payload) if payload else None,
        "payload": json.dumps(payload.get("payload", payload)) if payload else None,
    }
    
    # Add x-* attributes for technical systems
    if TOPIC_MAPPINGS[topic]["has_technical_attrs"]:
        event["x_attributes"] = payload.get("x_attributes", {}) | {
            "x-correlationId": str(uuid.uuid4()),
            "x-traceId": str(uuid.uuid4()),
            "x-spanId": f"span-{uuid.uuid4().hex[:4]}",
            "x-environment": os.getenv("ENVIRONMENT", "dev"),
            "x-tenantId": f"tenant-{uuid.uuid4().hex[:4]}",
            "x-messageType": topic,
        }
    else:
        event["x_attributes"] = None
    
    return event

def parse_json_events(file_path: str) -> list[Dict[str, Any]]:
    """Create one Kafka event per JSON object for Agent, Wendi or PDMIS feeds."""
    system, msg_type, topic = detect_system_from_path(file_path)
    if topic == "unknown" or topic not in TOPIC_MAPPINGS:
        logger.warning(f"Unknown JSON system/topic for {file_path}, skipping")
        return []
    try:
        document = json.loads(Path(file_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.error(f"Unable to parse JSON source {file_path}: {exc}")
        return []

    records = document if isinstance(document, list) else [document]
    events = []
    id_candidates = (
        "transaction_id", "payment_id", "loan_id", "beneficiary_id", "household_id",
        "sacco_id", "business_plan_id", "special_group_id", "agent_id", "event_id", "id"
    )
    date_candidates = (
        "created_at", "transaction_timestamp", "timestamp", "payment_date",
        "disbursement_date", "updated_at"
    )

    for record in records:
        if not isinstance(record, dict):
            continue
        message_id = next(
            (str(record[key]) for key in id_candidates if record.get(key) is not None),
            f"{system.upper()}-{msg_type}-{uuid.uuid4().hex[:8]}",
        )
        creation_date = next(
            (str(record[key]) for key in date_candidates if record.get(key)),
            time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        )
        amount = next(
            (record[key] for key in ("amount", "loan_amount", "disbursed_amount", "instructed_amount") if record.get(key) is not None),
            0.0,
        )
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            amount = 0.0

        raw = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        events.append({
            "event_id": str(uuid.uuid4()),
            "message_id": message_id,
            "event_type": msg_type,
            "source_system": system,
            "source_key": os.path.basename(file_path),
            "source_location": file_path,
            "instructed_amount": amount,
            "currency": str(record.get("currency") or "UGX"),
            "creation_date": creation_date,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "version": "1.0.0",
            "event_data": raw,
            "payload": raw,
            "x_attributes": None,
            "parsed_event_data": raw,
            "_topic": topic,
        })
    return events


# ------------------------------------------------------------------------------
# Main Producer
# ------------------------------------------------------------------------------
def main():
    logger.info("=" * 80)
    logger.info("🚀 Payment Event Producer")
    logger.info("=" * 80)
    logger.info(f"📋 Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"📡 Schema Registry: {Config.SCHEMA_REGISTRY_URL}")
    logger.info(f"📁 Data Root: {Config.DATA_ROOT}")
    logger.info(f"🔒 Idempotent: {Config.PRODUCER_IDEMPOTENCE}")
    logger.info("=" * 80)
    
    # Load Avro schema
    avro_schema = load_avro_schema()
    logger.info("✅ Avro schema loaded")
    
    # Initialize Schema Registry
    sr_client = SchemaRegistryClient({"url": Config.SCHEMA_REGISTRY_URL})
    
    # Initialize Avro serializer
    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=str(avro_schema),
        to_dict=lambda data, ctx: data
    )
    
    # Initialize Kafka producer with idempotency
    producer_config = {
        "bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "acks": Config.PRODUCER_ACKS,
        "retries": Config.PRODUCER_RETRIES,
    }
    
    if Config.PRODUCER_IDEMPOTENCE:
        producer_config.update({
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
        })
        logger.info("🔒 Idempotent producer enabled")
    
    producer = SerializingProducer(producer_config)
    
    # Discover all supported source files across the six source domains.
    xml_files = glob.glob(f"{Config.DATA_ROOT}/**/*.xml", recursive=True)
    json_files = glob.glob(f"{Config.DATA_ROOT}/**/*.json", recursive=True)
    logger.info(f"📄 Found {len(xml_files)} XML files and {len(json_files)} JSON files")

    if not xml_files and not json_files:
        logger.warning("No supported XML/JSON source files found under DATA_ROOT.")
        return
    
    # Group files by topic
    files_by_topic = {}
    for file_path in xml_files:
        system, msg_type, topic = detect_system_from_path(file_path)
        if topic != "unknown":
            if topic not in files_by_topic:
                files_by_topic[topic] = []
            files_by_topic[topic].append(file_path)

    json_events_by_topic = {}
    for file_path in json_files:
        for event in parse_json_events(file_path):
            topic = event.pop("_topic")
            json_events_by_topic.setdefault(topic, []).append(event)
    
    logger.info("📋 Source records grouped by topic:")
    for topic, files in files_by_topic.items():
        logger.info(f"  • {topic}: {len(files)} XML files")
    for topic, events in json_events_by_topic.items():
        logger.info(f"  • {topic}: {len(events)} JSON records")
    
    # Produce events
    produced = 0
    failed = 0
    
    for topic, files in files_by_topic.items():
        logger.info(f"📤 Producing to topic: {topic}")

        for file_path in files:
            event = parse_event(file_path)
            if event is None:
                failed += 1
                continue
            try:
                key = event.get("event_id", str(uuid.uuid4()))
                producer.produce(
                    topic=topic,
                    key=key,
                    value=event,
                    on_delivery=lambda err, msg: (
                        logger.error(f"❌ Delivery failed: {err}") if err else None
                    )
                )
                produced += 1
                if produced % 10 == 0:
                    logger.info(f"  📊 Produced {produced} events...")
            except Exception as e:
                logger.error(f"❌ Error producing {file_path}: {e}")
                failed += 1

    for topic, events in json_events_by_topic.items():
        logger.info(f"📤 Producing {len(events)} JSON records to topic: {topic}")
        for event in events:
            try:
                producer.produce(
                    topic=topic,
                    key=event["event_id"],
                    value=event,
                    on_delivery=lambda err, msg: (
                        logger.error(f"❌ Delivery failed: {err}") if err else None
                    ),
                )
                produced += 1
            except Exception as exc:
                logger.error(f"❌ Error producing JSON event to {topic}: {exc}")
                failed += 1
    
    # Flush all messages
    logger.info("⏳ Flushing producer...")
    producer.flush()
    
    # Summary
    logger.info("=" * 80)
    logger.info("📊 PRODUCTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"✅ Successfully produced: {produced} events")
    logger.info(f"❌ Failed: {failed} events")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()
