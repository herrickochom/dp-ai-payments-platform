#!/usr/bin/env python3
"""Guarded end-to-end execution: inventory -> publish -> bridge -> worker -> verify.

Only processes messages published in THIS run. Never replays history. Existing
907 GCS objects are untouched: only new objects for events this run publishes
are added after snapshotting high watermarks.

Run from repo root:
  python3 scripts/execute_guarded_pipeline.py --self-test
  python3 scripts/execute_guarded_pipeline.py --live \
    --project $KP_GCP_PROJECT --subscription $KP_RAW_V2_SUBSCRIPTION \
    --max-events 200 --max-seconds 1200
"""
from __future__ import annotations
import argparse, importlib.util, json, os, sys, time, uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path.cwd().resolve()
PUBLISHER = ROOT / "services/payment-producer/source_publisher.py"
BRIDGE = ROOT / "platform/gcp/kafka_pubsub_bridge/bridge.py"
RAW_V2 = ROOT / "platform/gcp/pubsub_raw_ingestion/raw_v2.py"
RAW_V2_CLOUD = ROOT / "platform/gcp/pubsub_raw_ingestion/raw_v2_cloud.py"
EXPECTED_TOPICS = 24
EXPECTED_PARTITIONS = 136
PUBSUB_TOPIC = "pdm-native-events-poc-v1"

def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

def _sec(t): print("\n=== " + t + " ===")

def self_test():
    for p in (PUBLISHER, BRIDGE, RAW_V2, RAW_V2_CLOUD):
        if not p.is_file(): raise SystemExit(f"STOP: missing {p}")
    sp = _load("gs_sp", PUBLISHER)
    assert len(sp.TOPIC_MANIFEST) == EXPECTED_TOPICS
    print(f"PASS: guarded pipeline self-tests; {EXPECTED_TOPICS} topics mapped")

def run(args):
    for p in (PUBLISHER, BRIDGE, RAW_V2, RAW_V2_CLOUD):
        if not p.is_file(): raise SystemExit(f"STOP: missing {p}")

    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from google.cloud import pubsub_v1, storage

    sp = _load("gs_sp", PUBLISHER)
    bridge_mod = _load("gs_bridge", BRIDGE)
    rv2 = _load("gs_rv2", RAW_V2)
    rv2_cloud = _load("gs_rv2_cloud", RAW_V2_CLOUD)

    evidence = {
        "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "status": "RUNNING", "max_events": args.max_events, "max_seconds": args.max_seconds,
        "kafka_inventory": {}, "publish_evidence": {}, "bridge": {}, "worker": {},
        "gcs_created": {}, "errors": [],
    }
    ev_path = Path(args.evidence); ev_path.parent.mkdir(parents=True, exist_ok=True)
    def write():
        tmp = ev_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n"); tmp.replace(ev_path)
    write()

    _sec("STEP 1 — inventory")
    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    meta = admin.list_topics(timeout=15)
    names = sorted(sp.TOPIC_MANIFEST)
    total_parts = 0
    for name in names:
        t = meta.topics.get(name)
        if t is None or t.error is not None:
            evidence["errors"].append(f"missing topic {name}"); evidence["status"] = "INVENTORY_FAILED"; write(); return 1
        total_parts += len(t.partitions)
        evidence["kafka_inventory"][name] = len(t.partitions)
    if len(names) != EXPECTED_TOPICS or total_parts != EXPECTED_PARTITIONS:
        evidence["errors"].append(f"inventory mismatch {len(names)}/{total_parts}"); evidence["status"] = "INVENTORY_FAILED"; write(); return 1
    print(f"PASS: {len(names)} topics, {total_parts} partitions")

    _sec("STEP 2 — snapshot high watermarks (before any publish)")
    snap_consumer = Consumer({"bootstrap.servers": args.bootstrap,
                              "group.id": f"guarded-snap-{uuid.uuid4().hex[:8]}",
                              "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    meta2 = snap_consumer.list_topics(timeout=15)
    snapshot: dict[tuple[str, int], int] = {}
    for name in names:
        t = meta2.topics.get(name)
        for pid in sorted(t.partitions):
            tp = TopicPartition(name, pid)
            _, hi = snap_consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            snapshot[(name, pid)] = hi
    print(f"Snapshot captured across {len(snapshot)} partitions")

    _sec("STEP 3 — source-to-Kafka publisher")
    import subprocess
    publish_ev = Path(args.publish_evidence); publish_ev.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(PUBLISHER), "--live",
           "--bootstrap", args.bootstrap, "--registry-url", args.registry_url,
           "--sources-root", str(args.sources_root), "--schemas-dir", str(args.schemas_dir),
           "--max-messages", str(args.max_events),
           *(["--no-idempotence"] if args.no_idempotence else []), "--max-seconds", str(args.max_seconds),
           "--evidence", str(publish_ev),
           *(["--topics", args.topics] if args.topics else [])]
    if subprocess.run(cmd).returncode != 0:
        evidence["errors"].append("publisher non-zero"); evidence["status"] = "PUBLISHER_FAILED"; write(); return 1
    pub = json.loads(publish_ev.read_text())
    evidence["publish_evidence"] = {"published": pub.get("published"), "topics": pub.get("topics"), "status": pub.get("status")}
    print(f"PUBLISHED: {pub.get('published')} events")

    _sec("STEP 4 — Kafka -> Pub/Sub bridge (only newly published offsets)")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, PUBSUB_TOPIC)
    publisher.get_topic(request={"topic": topic_path})  # fail-closed if missing

    bridged = 0; deadline = time.monotonic() + args.max_seconds
    total_to_bridge = pub.get("published", 0)
    to_assign = [TopicPartition(t, p, snapshot[(t, p)]) for (t, p) in snapshot]
    snap_consumer.assign(to_assign)
    idle = 0
    while bridged < total_to_bridge and time.monotonic() < deadline:
        msg = snap_consumer.poll(1.0)
        if msg is None:
            idle += 1
            if idle > 30: break
            continue
        idle = 0
        if msg.error(): raise RuntimeError(str(msg.error()))
        envelope = bridge_mod.encode_wire_envelope(msg)
        publisher.publish(topic_path, envelope).result(timeout=30)
        snap_consumer.commit(message=msg, asynchronous=False)
        bridged += 1
    snap_consumer.close()
    evidence["bridge"] = {"bridged": bridged, "expected": total_to_bridge}
    print(f"BRIDGED: {bridged}/{total_to_bridge}")

    _sec("STEP 5 — Pub/Sub -> GCS Raw v2 worker")
    reg = SchemaRegistryClient({"url": args.registry_url})
    sub = pubsub_v1.SubscriberClient()
    sub_path = sub.subscription_path(args.project, args.subscription)
    sc = storage.Client(project=args.project)
    bucket = sc.lookup_bucket(args.bucket)
    if bucket is None:
        evidence["errors"].append(f"bucket not accessible {args.bucket}"); evidence["status"] = "BUCKET_FAILED"; write(); return 1
    store = rv2_cloud.GCSConditionalStore(bucket)
    acker = rv2_cloud.PubSubAcknowledger(sub, sub_path)
    decoder = rv2_cloud.RegistryDecoder(reg)

    processed = 0
    while processed < bridged and time.monotonic() < deadline:
        # PULL-RETRY-ON-NOT-FOUND: retry pull on NotFound up to 60s (subscription propagation race)
        _pull_deadline = time.monotonic() + 60
        resp = None
        while time.monotonic() < _pull_deadline:
            try:
                resp = sub.pull(request={"subscription": sub_path, "max_messages": min(10, bridged - processed)}, timeout=15)
                break
            except Exception as _pe:
                if "NotFound" in type(_pe).__name__ or "404" in str(_pe):
                    time.sleep(2)
                    continue
                raise
        if resp is None:
            raise RuntimeError("pull never succeeded after 60s retries")
        if not resp.received_messages: continue
        for recv in resp.received_messages:
            try:
                name, state = rv2_cloud.process_message(recv.message.data, recv.ack_id, store, acker, decoder)
                evidence["gcs_created"][name] = state
                processed += 1
            except Exception as exc:
                evidence["errors"].append(f"worker {type(exc).__name__}: {exc}")
    sub.close()
    evidence["worker"] = {"processed": processed}
    print(f"WORKER: {processed} objects present in GCS")

    _sec("STEP 6 — verify")
    created = sum(1 for v in evidence["gcs_created"].values() if v == "CREATED")
    already = sum(1 for v in evidence["gcs_created"].values() if v == "ALREADY_PRESENT_IDENTICAL")
    print(f"GCS: created={created} already_present_identical={already} errors={len(evidence['errors'])}")
    if created + already == bridged == total_to_bridge and total_to_bridge > 0:
        evidence["status"] = "COMPLETE"
    else:
        evidence["status"] = "INCOMPLETE"
    evidence["finished_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    write()
    print(f"\nEVIDENCE: {ev_path}\nSTATUS:   {evidence['status']}")
    return 0 if evidence["status"] == "COMPLETE" else 2

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--live", action="store_true")
    p.add_argument("--bootstrap", default=os.getenv("KP_KAFKA_BOOTSTRAP","localhost:9094"))
    p.add_argument("--registry-url", default=os.getenv("KP_SCHEMA_REGISTRY_URL","http://localhost:8081"))
    p.add_argument("--project", default=os.getenv("KP_GCP_PROJECT",""))
    p.add_argument("--subscription", default=os.getenv("KP_RAW_V2_SUBSCRIPTION",""))
    p.add_argument("--bucket", default="dp-pdm-ai-platform-dev")
    p.add_argument("--sources-root", type=Path, default=Path("data"))
    p.add_argument("--schemas-dir", type=Path, default=Path("contracts/kafka"))
    p.add_argument("--max-events", type=int, default=200)
    p.add_argument("--no-idempotence", action="store_true")
    p.add_argument("--topics", default="", help="comma-separated topic allowlist")
    p.add_argument("--max-seconds", type=int, default=1200)
    p.add_argument("--publish-evidence", default="evidence/source-publisher/last_run.json")
    p.add_argument("--evidence", default="evidence/e2e-pipeline/last_run.json")
    a = p.parse_args()
    if a.self_test: self_test(); return
    if not a.live: print("SAFE: guarded pipeline installed; --live required."); return
    if not (1 <= a.max_events <= 50000 and 60 <= a.max_seconds <= 14400):
        p.error("--max-events 1..50000; --max-seconds 60..14400")
    if a.bucket != "dp-pdm-ai-platform-dev": raise SystemExit("STOP: unapproved destination bucket")
    if not (a.project and a.subscription): raise SystemExit("STOP: --project and --subscription required")
    raise SystemExit(run(a))

if __name__ == "__main__":
    sys.exit(main())
