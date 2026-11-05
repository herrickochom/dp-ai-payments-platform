#!/usr/bin/env python3
"""Schema-driven source-to-Kafka publisher for all 24 approved PDM datasets.

Reads contracts/kafka/payment_event.avsc and technical_payment_event.avsc at
runtime, constructs schema-conformant records, and emits Confluent Avro wire
format so the unchanged downstream path applies:

    Kafka -> bridge.encode_wire_envelope -> Pub/Sub -> raw_v2.name_for
         -> GCSConditionalStore (raw/v2/.../record.avro) -> Dataflow

Invariants: explicit --live; bounded --max-messages; atomic evidence; optional
--no-idempotence for stuck transaction coordinators.
"""
from __future__ import annotations
import argparse, hashlib, json, os, sys, tempfile, time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

ROOT = Path.cwd().resolve()
SOURCES_DIR = Path(os.getenv("PDM_SOURCES_ROOT", str(ROOT / "data")))
CONTRACTS_DIR = ROOT / "contracts" / "kafka"

TOPIC_MANIFEST: dict[str, dict[str, str]] = {
    "icmn.vpm.pain001":      {"system":"icmn","msg_type":"pain001","family":"PAYMENT_BUSINESS_EVENT"},
    "icmn.pmn.pain001":      {"system":"pmn","msg_type":"technical_payment_event","family":"TECHNICAL_PAYMENT_EVENT"},
    "cpo.psn.pain002":       {"system":"cpo","msg_type":"pain002","family":"PAYMENT_BUSINESS_EVENT"},
    "cpo.plm.pain002":       {"system":"plm","msg_type":"technical_payment_event","family":"TECHNICAL_PAYMENT_EVENT"},
    "wendi.camt052":         {"system":"wendi","msg_type":"camt052","family":"PAYMENT_BUSINESS_EVENT"},
    "wendi.camt053":         {"system":"wendi","msg_type":"camt053","family":"PAYMENT_BUSINESS_EVENT"},
    "wendi.camt054":         {"system":"wendi","msg_type":"camt054","family":"PAYMENT_BUSINESS_EVENT"},
    "wendi.transactions":    {"system":"wendi","msg_type":"transactions","family":"PAYMENT_BUSINESS_EVENT"},
    "wendi.pain001":         {"system":"wendi","msg_type":"pain001","family":"PAYMENT_BUSINESS_EVENT"},
    "wendi.pain002":         {"system":"wendi","msg_type":"pain002","family":"PAYMENT_BUSINESS_EVENT"},
    "mobile.mtn.pacs008":    {"system":"mtn","msg_type":"pacs008","family":"PAYMENT_BUSINESS_EVENT"},
    "mobile.mtn.pacs002":    {"system":"mtn","msg_type":"pacs002","family":"PAYMENT_BUSINESS_EVENT"},
    "mobile.airtel.pacs008": {"system":"airtel","msg_type":"pacs008","family":"PAYMENT_BUSINESS_EVENT"},
    "mobile.airtel.pacs002": {"system":"airtel","msg_type":"pacs002","family":"PAYMENT_BUSINESS_EVENT"},
    "agent.transactions":    {"system":"agent","msg_type":"transactions","family":"PAYMENT_BUSINESS_EVENT"},
    "agent.profiles":        {"system":"agent","msg_type":"profiles","family":"PAYMENT_BUSINESS_EVENT"},
    "agent.locations":       {"system":"agent","msg_type":"locations","family":"PAYMENT_BUSINESS_EVENT"},
    "pdmis.beneficiaries":   {"system":"pdmis","msg_type":"beneficiaries","family":"PAYMENT_BUSINESS_EVENT"},
    "pdmis.business_plans":  {"system":"pdmis","msg_type":"business_plans","family":"PAYMENT_BUSINESS_EVENT"},
    "pdmis.households":      {"system":"pdmis","msg_type":"households","family":"PAYMENT_BUSINESS_EVENT"},
    "pdmis.loans":           {"system":"pdmis","msg_type":"loans","family":"PAYMENT_BUSINESS_EVENT"},
    "pdmis.repayments":      {"system":"pdmis","msg_type":"repayments","family":"PAYMENT_BUSINESS_EVENT"},
    "pdmis.saccos":          {"system":"pdmis","msg_type":"saccos","family":"PAYMENT_BUSINESS_EVENT"},
    "pdmis.special_groups":  {"system":"pdmis","msg_type":"special_groups","family":"PAYMENT_BUSINESS_EVENT"},
}

FILE_ROUTES = {
    "agent_profiles.json":"agent.profiles","agent_locations.json":"agent.locations",
    "agent_transactions.json":"agent.transactions","beneficiaries.json":"pdmis.beneficiaries",
    "business_plans.json":"pdmis.business_plans","households.json":"pdmis.households",
    "loans.json":"pdmis.loans","repayments.json":"pdmis.repayments",
    "saccos.json":"pdmis.saccos","special_groups.json":"pdmis.special_groups",
    "wendi_transactions.json":"wendi.transactions","transactions.json":"wendi.transactions",
}
MARKERS = (
    ("/icmn/vpm/pain001/","icmn.vpm.pain001"),("/icmn/pmn/pain001/","icmn.pmn.pain001"),
    ("/cpo/psn/pain002/","cpo.psn.pain002"),("/cpo/plm/pain002/","cpo.plm.pain002"),
    ("/wendi/pain001/","wendi.pain001"),("/wendi/pain002/","wendi.pain002"),
    ("/wendi/camt052/","wendi.camt052"),("/wendi/camt053/","wendi.camt053"),("/wendi/camt054/","wendi.camt054"),
    ("/mobile_networks/mtn/pacs008/","mobile.mtn.pacs008"),
    ("/mobile_networks/mtn/pacs002/","mobile.mtn.pacs002"),
    ("/mobile_networks/airtel/pacs008/","mobile.airtel.pacs008"),
    ("/mobile_networks/airtel/pacs002/","mobile.airtel.pacs002"),
    ("/agent_network/agent_transactions/","agent.transactions"),
)
PDMIS_ENTITIES = ("beneficiaries","business_plans","households","loans","repayments","saccos","special_groups")

def route(rel: Path) -> str | None:
    norm = "/" + rel.as_posix().lower().lstrip("/")
    name = rel.name.lower()
    if name in FILE_ROUTES and (name != "transactions.json" or "/wendi/" in norm):
        return FILE_ROUTES[name]
    for marker, topic in MARKERS:
        if marker in norm: return topic
    if "/pdmis/" in norm:
        for entity in PDMIS_ENTITIES:
            if f"/pdmis/{entity}/" in norm or f"/{entity}/" in norm:
                return f"pdmis.{entity}"
    return None

def _sha(s): return hashlib.sha256(s if isinstance(s, bytes) else s.encode()).hexdigest()
def _canon(o): return json.dumps(o, sort_keys=True, separators=(",",":"), ensure_ascii=False, allow_nan=False).encode()
def _event_id(topic, path, idx): return "evt-" + _sha(f"{topic}\x00{path}\x00{idx}")[:32]

def _business_key(topic, payload):
    p = payload or {}
    for f in ("loan_id","payment_id","transaction_id","beneficiary_id","agent_id",
              "sacco_id","household_id","business_plan_id","special_group","group_code",
              "MsgId","EndToEndId","TxId"):
        v = p.get(f)
        if isinstance(v, str) and v.strip(): return v
    return ""

def _extract_iso(xml):
    try: root = ET.fromstring(xml)
    except ET.ParseError: return {}
    def t(tag):
        el = root.find(f".//{tag}")
        return el.text.strip() if el is not None and el.text else None
    grp = root.find(".//GrpHdr")
    instd = root.find(".//InstdAmt"); intr = root.find(".//IntrBkSttlmAmt")
    ccy = (instd or intr).attrib.get("Ccy") if (instd is not None or intr is not None) else None
    return {
        "MsgId": grp.findtext("MsgId") if grp is not None else None,
        "EndToEndId": t("EndToEndId"), "TxId": t("TxId"), "UETR": t("UETR"),
        "InstdAmt": t("InstdAmt") or t("IntrBkSttlmAmt"), "Ccy": ccy,
    }

TECH_XML_MAP = {
    "EventId":"event_id","MessageId":"message_id","EventFamily":"event_family",
    "EventType":"event_type","CorrelationId":"correlation_id",
    "PaymentInstructionId":"instruction_id","InstructionId":"instruction_id",
    "EndToEndId":"end_to_end_id","TransactionId":"transaction_id","UETR":"uetr",
    "BusinessReference":"business_reference","XTrace":"x_trace","XChannel":"x_channel",
    "XBeneficiarySa":"x_beneficiary_sa","XSourceSystem":"x_source_system",
    "XTargetSystem":"x_target_system","XService":"x_service","XOperation":"x_operation",
    "XComponent":"x_component","XNode":"x_node","XHost":"x_host",
    "XPaymentRoute":"x_payment_route","XOriginatingInstitution":"x_originating_institution",
    "XIntermediaryInstitution":"x_intermediary_institution","XRouteDecision":"x_route_decision",
    "XValidationStatus":"x_validation_status","XSubmissionStatus":"x_submission_status",
    "XProvider":"x_provider","XNetwork":"x_network","XWalletReference":"x_wallet_reference",
    "XProviderTransactionId":"x_provider_transaction_id","XCreditStatus":"x_credit_status",
    "XAgentReference":"x_agent_reference","XCashoutStatus":"x_cashout_status",
    "TechnicalStage":"technical_stage","TechnicalStatus":"technical_status",
    "EventTimestamp":"event_timestamp","ProcessingTimestamp":"processing_timestamp",
    "XLatencyMs":"x_latency_ms","XRetryCount":"x_retry_count",
    "XTimeoutIndicator":"x_timeout_indicator","XErrorCode":"x_error_code",
    "XErrorCategory":"x_error_category","SchemaVersion":"schema_version",
}

def _extract_technical(xml_bytes):
    try: root = ET.fromstring(xml_bytes)
    except ET.ParseError: return {}
    out = {}
    for child in root:
        if child.tag in TECH_XML_MAP and child.text is not None and child.text.strip():
            out[TECH_XML_MAP[child.tag]] = child.text.strip()
    return out

_MISSING = object()
_CACHE: dict = {}

def _load_schemas(schemas_dir: Path):
    key = str(schemas_dir.resolve())
    if key in _CACHE: return _CACHE[key]
    pay = schemas_dir / "payment_event.avsc"
    tech = schemas_dir / "technical_payment_event.avsc"
    for p in (pay, tech):
        if not p.is_file(): raise SystemExit(f"STOP: missing {p}")
    out = (json.loads(pay.read_text()), json.loads(tech.read_text()),
           pay.read_text(), tech.read_text())
    _CACHE[key] = out
    return out

def _default_for(ftype, default):
    if default is not _MISSING: return default
    if isinstance(ftype, list):
        if "null" in ftype: return None
        return _default_for(ftype[0], _MISSING)
    if isinstance(ftype, dict):
        t = ftype.get("type")
        if t == "record": return {}
        if t == "map": return {}
        if t == "array": return []
        if t == "enum":
            s = ftype.get("symbols", []); return s[0] if s else ""
        return None
    return {"string":"","long":0,"int":0,"double":0.0,"float":0.0,
            "boolean":False,"bytes":b""}.get(ftype)

def _normalize(ftype, value, default=_MISSING):
    if value is None:
        if isinstance(ftype, list) and "null" in ftype: return None
        return _default_for(ftype, default)
    if isinstance(ftype, list):
        for t in ftype:
            if t == "null": continue
            return _normalize(t, value, default)
        return None
    if isinstance(ftype, dict):
        t = ftype.get("type")
        if t == "string": return str(value)
        if t in ("long","int"):
            try: return int(value)
            except (TypeError, ValueError): return 0
        if t in ("double","float"):
            try: return float(value)
            except (TypeError, ValueError): return 0.0
        if t == "boolean":
            if isinstance(value, str): return value.lower() in ("true","1","yes","y","t")
            return bool(value)
        if t == "map":
            if isinstance(value, dict):
                vt = ftype.get("values", "string")
                return {str(k): _normalize(vt, v) for k, v in value.items()}
            return {}
        if t == "record": return value if isinstance(value, dict) else {}
        if t == "array": return value if isinstance(value, list) else []
        if t == "enum":
            s = ftype.get("symbols", [])
            return value if value in s else (s[0] if s else "")
        return _default_for(ftype, default)
    if ftype == "string": return str(value)
    if ftype in ("long","int"):
        try: return int(value)
        except (TypeError, ValueError): return 0
    if ftype in ("double","float"):
        try: return float(value)
        except (TypeError, ValueError): return 0.0
    if ftype == "boolean":
        if isinstance(value, str): return value.lower() in ("true","1","yes","y","t")
        return bool(value)
    if ftype == "bytes": return value if isinstance(value, bytes) else str(value).encode()
    return value

def _build_for_schema(schema: dict, candidates: dict) -> dict:
    return {f["name"]: _normalize(f["type"], candidates.get(f["name"]),
                                  f.get("default", _MISSING))
            for f in schema["fields"]}

def build_record(*, topic, source_path, record_index, payload, raw_bytes,
                 is_technical, business_schema=None, technical_schema=None):
    if business_schema is None or technical_schema is None:
        b, t, _, _ = _load_schemas(CONTRACTS_DIR)
        business_schema = business_schema or b
        technical_schema = technical_schema or t

    info = TOPIC_MANIFEST[topic]
    ev_id = _event_id(topic, source_path, record_index)
    ts = datetime.fromtimestamp(
        (Path(source_path).stat().st_mtime if Path(source_path).exists() else time.time()),
        timezone.utc).isoformat(timespec="seconds")

    if is_technical:
        tech = _extract_technical(raw_bytes) if raw_bytes.lstrip().startswith(b"<") else {}
        bk = (tech.get("business_reference")
              or tech.get("end_to_end_id")
              or tech.get("message_id")
              or _business_key(topic, payload))
        msg_id = tech.get("message_id") or f"{topic}/{record_index}"
        candidates = {
            **tech,
            "event_id": tech.get("event_id") or ev_id,
            "message_id": msg_id,
            "event_family": "TECHNICAL_PAYMENT_EVENT",
            "event_type": info["msg_type"],
            "schema_version": tech.get("schema_version") or "1",
            "business_reference": tech.get("business_reference") or bk or ev_id,
            "end_to_end_id": tech.get("end_to_end_id") or bk or ev_id,
            "x_source_system": tech.get("x_source_system") or info["system"],
            "x_attributes": {
                "source_path": source_path, "record_index": str(record_index),
                "record_sha256": _sha(raw_bytes), "publisher": "source_publisher.v2",
            },
        }
        return _build_for_schema(technical_schema, candidates)

    extracted = dict(payload or {})
    if raw_bytes.lstrip().startswith(b"<"):
        extracted.update({k: v for k, v in _extract_iso(raw_bytes).items() if v is not None})
    amount = None
    for c in ("amount","InstdAmt","amount_disbursed","amount_approved","total"):
        v = extracted.get(c)
        if isinstance(v, (int, float)): amount = float(v); break
    currency = extracted.get("currency") or extracted.get("Ccy") or ("UGX" if amount is not None else None)
    bk = _business_key(topic, extracted)
    msg_id = extracted.get("MsgId") or bk or ev_id
    if not isinstance(msg_id, str) or not msg_id.strip():
        msg_id = ev_id
    candidates = {
        "event_id": ev_id,
        "message_id": msg_id,
        "creation_date": ts[:10],
        "event_type": info["msg_type"],
        "event_family": info["family"],
        "source_system": info["system"],
        "source_key": bk or None,
        "source_location": source_path,
        "instructed_amount": amount,
        "currency": currency,
        "timestamp": ts,
        "version": "1",
        "event_data": raw_bytes.decode("utf-8","replace") if raw_bytes.lstrip().startswith(b"<") else None,
        "parsed_event_data": json.dumps(extracted, sort_keys=True, ensure_ascii=False) if extracted else None,
        "payload": extracted if extracted else None,
        "x_attributes": {
            "source_path": source_path, "record_index": str(record_index),
            "record_sha256": _sha(raw_bytes), "publisher": "source_publisher.v2",
        },
    }
    return _build_for_schema(business_schema, candidates)

def iter_records(root: Path):
    if not root.is_dir(): raise SystemExit(f"STOP: source root missing: {root}")
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.is_symlink(): continue
        if path.suffix.lower() not in (".xml",".json"): continue
        rel = path.relative_to(root); topic = route(rel)
        if topic is None: continue
        raw = path.read_bytes(); sp = rel.as_posix()
        if path.suffix.lower() == ".xml":
            yield (topic, sp, 0, {}, raw)
        else:
            try: parsed = json.loads(raw.decode())
            except (UnicodeError, ValueError): continue
            for i, r in enumerate(parsed if isinstance(parsed, list) else [parsed]):
                if isinstance(r, dict): yield (topic, sp, i, r, _canon(r))

@dataclass
class Evidence:
    started_utc: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))
    finished_utc: str | None = None
    status: str = "RUNNING"
    source_root: str = ""; schemas_dir: str = ""; bootstrap: str = ""; registry_url: str = ""
    max_messages: int = 0; published: int = 0; skipped_already_published: int = 0
    errors: list[str] = field(default_factory=list)
    topics: dict = field(default_factory=dict)
    message_ids: list = field(default_factory=list)
    def as_json(self):
        return {k: getattr(self, k) for k in
                ("started_utc","finished_utc","status","source_root","schemas_dir",
                 "bootstrap","registry_url","max_messages","published",
                 "skipped_already_published","errors","topics","message_ids")}

def write_evidence(path: Path, ev):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".pub-", suffix=".json", dir=path.parent)
    try:
        with os.fdopen(fd,"w") as f:
            json.dump(ev.as_json(), f, indent=2, sort_keys=True); f.write("\n")
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

def run_live(args) -> int:
    try:
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import SerializationContext, MessageField
    except ImportError as exc:
        raise SystemExit(f"STOP: confluent-kafka missing: {exc}")

    business_schema, technical_schema, pay_str, tech_str = _load_schemas(Path(args.schemas_dir).resolve())
    topic_filter = {t.strip() for t in args.topics.split(",") if t.strip()}

    ev = Evidence(source_root=str(args.sources_root), schemas_dir=str(args.schemas_dir),
                  bootstrap=args.bootstrap, registry_url=args.registry_url,
                  max_messages=args.max_messages)
    ev_path = Path(args.evidence)
    prior_ids = set()
    prior_records = []
    if ev_path.exists():
        try:
            prior = json.loads(ev_path.read_text())
            for m in prior.get("message_ids", []):
                eid = m.get("event_id")
                if eid: prior_ids.add(eid)
            if prior_ids:
                print(f"Resume: {len(prior_ids)} previously published event_ids will be skipped")
        except (ValueError, KeyError):
            pass
    write_evidence(ev_path, ev)

    reg = SchemaRegistryClient({"url": args.registry_url})
    ps = AvroSerializer(reg, pay_str)
    ts = AvroSerializer(reg, tech_str)
    prod_cfg = {"bootstrap.servers": args.bootstrap,
                "compression.type": "zstd", "linger.ms": 20}
    if getattr(args, "no_idempotence", False):
        prod_cfg.update({"acks": "1", "retries": 5, "message.timeout.ms": 30000})
    else:
        prod_cfg.update({"enable.idempotence": True, "acks": "all"})
    prod = Producer(prod_cfg)

    delivered = []
    def make_cb(rec):
        def cb(err, msg):
            if err: raise RuntimeError(f"delivery: {err}")
            delivered.append({
                "topic": msg.topic(), "partition": str(msg.partition()),
                "offset": str(msg.offset()),
                "event_id": rec.get("event_id", ""),
                "business_key": (msg.key() or b"").decode(),
            })
        return cb

    published = 0; start = time.monotonic()
    for topic, sp, idx, payload, raw in iter_records(args.sources_root):
        if topic_filter and topic not in topic_filter: continue
        if published >= args.max_messages: break
        if time.monotonic() - start > args.max_seconds:
            ev.status = "STOPPED_AT_WALL_CLOCK"; break
        info = TOPIC_MANIFEST[topic]; is_tech = info["family"] == "TECHNICAL_PAYMENT_EVENT"
        try:
            rec = build_record(topic=topic, source_path=sp, record_index=idx,
                               payload=payload, raw_bytes=raw, is_technical=is_tech,
                               business_schema=business_schema, technical_schema=technical_schema)
            if rec.get("event_id") in prior_ids:
                ev.skipped_already_published += 1
                continue
            key = (rec.get("source_key")
                   or rec.get("business_reference")
                   or rec.get("message_id")
                   or _business_key(topic, payload)
                   or rec.get("event_id", ""))
            ser = ts if is_tech else ps
            wire = ser(rec, SerializationContext(topic, MessageField.VALUE))
            prod.produce(topic=topic, key=key.encode(), value=wire, on_delivery=make_cb(rec))
            prod.poll(0)
        except Exception as exc:
            ev.errors.append(f"{sp}#{idx}: {type(exc).__name__}: {exc}")
            print(f"STOP: {sp}#{idx}: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
            ev.status = "STOPPED_WITH_ERROR"
            if prior_records:
                new_ids = {m.get("event_id") for m in ev.message_ids if m.get("event_id")}
                for m in prior_records:
                    eid = m.get("event_id")
                    if eid and eid not in new_ids:
                        ev.message_ids.append(m)
            write_evidence(ev_path, ev); return 1
        published += 1
        ev.topics.setdefault(topic, {"attempted": 0})["attempted"] += 1
        if published % 50 == 0: write_evidence(ev_path, ev)

    prod.flush(timeout=60)
    ev.published = len(delivered); ev.message_ids = delivered  # KP-011: was [:1000]; truncated dedup ledger
    # CANONICAL-MERGE-V3: append prior message_ids not seen this run
    if prior_records:
        new_ids = {m.get("event_id") for m in ev.message_ids if m.get("event_id")}
        for m in prior_records:
            eid = m.get("event_id")
            if eid and eid not in new_ids:
                ev.message_ids.append(m)
    for d in delivered:
        b = ev.topics.setdefault(d["topic"], {"attempted": 0, "delivered": 0})
        b["delivered"] = b.get("delivered", 0) + 1
    if ev.published:
        ev.status = "PUBLISHED"
    elif ev.skipped_already_published:
        ev.status = "ALL_SKIPPED_AS_PREVIOUSLY_PUBLISHED"
    else:
        ev.status = "PUBLISHED_NOTHING"
    ev.finished_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write_evidence(ev_path, ev)
    print(f"PUBLISHED {ev.published} events across "
          f"{sum(1 for v in ev.topics.values() if v.get('delivered'))} topics; "
          f"skipped {ev.skipped_already_published} previously published")
    print(f"Evidence: {ev_path}")
    return 0 if ev.status in ("PUBLISHED", "ALL_SKIPPED_AS_PREVIOUSLY_PUBLISHED") else 2

def self_test():
    import io as _io
    import fastavro
    assert route(Path("icmn/vpm/pain001/a.xml")) == "icmn.vpm.pain001"
    assert route(Path("mobile_networks/airtel/pacs002/a.xml")) == "mobile.airtel.pacs002"
    assert route(Path("agent_network/agent_profiles.json")) == "agent.profiles"
    assert route(Path("pdmis/loans.json")) == "pdmis.loans"
    assert route(Path("other/transactions.json")) is None

    business_schema, technical_schema, _, _ = _load_schemas(CONTRACTS_DIR)
    parsed_b = fastavro.parse_schema(business_schema)
    parsed_t = fastavro.parse_schema(technical_schema)

    # Business event round-trip
    rec_b = build_record(topic="agent.locations",
                         source_path="fake/agent.locations", record_index=0,
                         payload={"agent_id": "AGENT-0001"}, raw_bytes=b'{}',
                         is_technical=False,
                         business_schema=business_schema, technical_schema=technical_schema)
    buf = _io.BytesIO(); fastavro.schemaless_writer(buf, parsed_b, rec_b)
    buf.seek(0); fastavro.schemaless_reader(buf, parsed_b)
    print(f"PASS: payment_event round-trips  bytes={len(buf.getvalue())}")

    # Technical event round-trip
    xml = (b'<TechnicalPaymentEvent><EventId>abc</EventId><MessageId>m1</MessageId>'
           b'<EventType>PMN</EventType><EventFamily>TECHNICAL_PAYMENT_EVENT</EventFamily>'
           b'<CorrelationId>c1</CorrelationId><EndToEndId>e1</EndToEndId>'
           b'<BusinessReference>b1</BusinessReference><XTrace>t1</XTrace>'
           b'<XChannel>BANK</XChannel><XSourceSystem>pmn</XSourceSystem>'
           b'<XTargetSystem>BOU</XTargetSystem><XService>route</XService>'
           b'<XOperation>route</XOperation><XComponent>pmn</XComponent>'
           b'<XNode>n1</XNode><XHost>h1</XHost>'
           b'<TechnicalStage>routing</TechnicalStage><TechnicalStatus>COMPLETED</TechnicalStatus>'
           b'<EventTimestamp>2026-10-31T11:00:00.000+03:00</EventTimestamp>'
           b'<ProcessingTimestamp>2026-10-31T11:00:00.100+03:00</ProcessingTimestamp>'
           b'<XLatencyMs>100</XLatencyMs><XRetryCount>0</XRetryCount>'
           b'<XTimeoutIndicator>false</XTimeoutIndicator></TechnicalPaymentEvent>')
    rec_t = build_record(topic="icmn.pmn.pain001",
                         source_path="fake/icmn.pmn.pain001", record_index=0,
                         payload={}, raw_bytes=xml, is_technical=True,
                         business_schema=business_schema, technical_schema=technical_schema)
    buf = _io.BytesIO(); fastavro.schemaless_writer(buf, parsed_t, rec_t)
    buf.seek(0); fastavro.schemaless_reader(buf, parsed_t)
    print(f"PASS: technical_payment_event round-trips  bytes={len(buf.getvalue())}")

    print(f"PASS: source_publisher self-tests; {len(TOPIC_MANIFEST)} topics mapped")

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--live", action="store_true")
    p.add_argument("--bootstrap", default=os.getenv("KP_KAFKA_BOOTSTRAP","localhost:9094"))
    p.add_argument("--registry-url", default=os.getenv("KP_SCHEMA_REGISTRY_URL","http://localhost:8081"))
    p.add_argument("--sources-root", type=Path, default=SOURCES_DIR)
    p.add_argument("--schemas-dir", type=Path, default=CONTRACTS_DIR)
    p.add_argument("--max-messages", type=int, default=200)
    p.add_argument("--max-seconds", type=int, default=900)
    p.add_argument("--topics", default="", help="comma-separated topic allowlist")
    p.add_argument("--no-idempotence", action="store_true")
    p.add_argument("--evidence", default="evidence/source-publisher/last_run.json")
    a = p.parse_args()
    if a.self_test: self_test(); return
    if not a.live:
        print("SAFE: source_publisher installed; pass --live with --max-messages N to publish.")
        return
    if not 1 <= a.max_messages <= 50000: p.error("--max-messages 1..50000")
    if not 60 <= a.max_seconds <= 14400: p.error("--max-seconds 60..14400")
    raise SystemExit(run_live(a))

if __name__ == "__main__":
    sys.exit(main())
