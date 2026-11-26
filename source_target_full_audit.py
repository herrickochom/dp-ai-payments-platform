#!/usr/bin/env python3
"""
One-shot source-to-target integrity and lineage audit.

Phases:
  1. Contract discovery  — how is the Kafka value represented in GCS Avro?
  2. Payload integrity   — SHA-256 per (topic, partition, offset)
  3. Source lineage      — file/generator → topic, including mobile_networks
  4. Restricted MDM      — contracts only, never values
  5. Acceptance report   — JSON + per-topic matrix

Read-only. No commits, no replay, no PII.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import yaml
from confluent_kafka import Consumer, TopicPartition
from google.cloud import storage

try:
    import fastavro
    HAVE_FASTAVRO = True
except Exception:
    HAVE_FASTAVRO = False

if not HAVE_FASTAVRO:
    print("STOP: install fastavro in the venv: pip install fastavro", file=sys.stderr)
    sys.exit(2)


# ──────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────

DEFAULTS = {
    "repo_root": Path.cwd(),
    "bucket": "dp-pdm-ai-platform-dev",
    "project": "dp-pdm-ai-dev-0b4da804",
    "bootstrap": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
    "group": "source-target-full-audit-read-only",
    "topics_yaml": "platform/kafka/topics.yaml",
    "raw_prefix": "raw/v1/",
    "report_json": "/tmp/source-target-full-audit.json",
    "sample_per_topic": 3,
}

EXPECTED_FILE_SOURCES = {
    "mdm.agent.golden":                     "data/mdm/golden_agents.json",
    "mdm.geography.golden":                 "data/mdm/golden_geography.json",
    "mdm.sacco.golden":                     "data/mdm/golden_saccos.json",
    "mdm.beneficiary.golden.restricted":    "data/mdm/golden_beneficiaries_restricted.json",
    "mdm.crosswalk.restricted":             "data/mdm/source_crosswalk.json",
    "mdm.beneficiary.identity-alert.restricted":
        "data/mdm/beneficiary_identity_alerts_restricted.json",
}

EXPECTED_MOBILE_TOPICS = [
    "mobile.mtn.pacs008", "mobile.mtn.pacs002",
    "mobile.airtel.pacs008", "mobile.airtel.pacs002",
]

RESTRICTED_TOPICS = [
    "mdm.beneficiary.golden.restricted",
    "mdm.crosswalk.restricted",
    "mdm.beneficiary.identity-alert.restricted",
]

RAW_NAME_RE = re.compile(
    r"^raw/v1/([^/]+)/.*/topic=([^/]+)/partition=(\d+)/offset=(\d+)/record\.avro$"
)


# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_gcs_avro(client, bucket_name: str, blob_name: str):
    """Return (writer_schema_dict, [record_dicts])."""
    blob = client.bucket(bucket_name).blob(blob_name)
    raw = blob.download_as_bytes()
    import io as _io
    reader = fastavro.reader(_io.BytesIO(raw))
    schema = reader.writer_schema
    records = [dict(r) for r in reader]
    return schema, records


def find_paths_with_bytes(record, target: bytes, _path=None):
    """Return dotted paths in `record` whose bytes value equals `target`."""
    if _path is None:
        _path = []
    matches = []
    if isinstance(record, (bytes, bytearray)):
        if bytes(record) == target:
            matches.append(".".join(_path) or "<root>")
    elif isinstance(record, dict):
        for k, v in record.items():
            matches.extend(find_paths_with_bytes(v, target, _path + [str(k)]))
    elif isinstance(record, list):
        for i, v in enumerate(record):
            matches.extend(find_paths_with_bytes(v, target, _path + [f"[{i}]"]))
    return matches


def top_level_shape(record) -> dict:
    """Structure-only summary; never includes field values."""
    if not isinstance(record, dict):
        return {"__root__": type(record).__name__}
    out = {}
    for k, v in record.items():
        if isinstance(v, dict):
            out[k] = {"type": "record", "keys": sorted(v.keys())}
        elif isinstance(v, list):
            out[k] = {"type": "array", "length": len(v),
                      "element_type": type(v[0]).__name__ if v else None}
        elif isinstance(v, (bytes, bytearray)):
            out[k] = {"type": "bytes", "length": len(v)}
        else:
            out[k] = {"type": type(v).__name__}
    return out


def parse_blob_name(name: str):
    m = RAW_NAME_RE.match(name)
    if not m:
        return None
    mode, topic, partition, offset = m.groups()
    return mode, topic, int(partition), int(offset)


def list_raw_index(client, bucket_name: str):
    index = defaultdict(dict)
    unmatched = []
    for blob in client.list_blobs(bucket_name, prefix=DEFAULTS["raw_prefix"]):
        if blob.name.endswith("/"):
            continue
        parsed = parse_blob_name(blob.name)
        if not parsed:
            unmatched.append(blob.name)
            continue
        _, topic, partition, offset = parsed
        index[topic][(partition, offset)] = blob.name
    return index, unmatched


def load_topics(repo_root: Path, topics_yaml_rel: str):
    path = repo_root / topics_yaml_rel
    if not path.exists():
        raise SystemExit(f"STOP: missing {path}")
    config = yaml.safe_load(path.read_text())
    if isinstance(config, dict) and isinstance(config.get("topics"), list):
        items = config["topics"]
    elif isinstance(config, list):
        items = config
    else:
        raise SystemExit("STOP: topics.yaml shape not recognized")
    topics = {}
    for item in items:
        name = item.get("name") or item.get("topic")
        mode = item.get("processing_mode")
        if not isinstance(name, str) or mode not in {"batch", "streaming", "real-time"}:
            raise SystemExit(f"STOP: incomplete topic contract: {item}")
        if name in topics:
            raise SystemExit(f"STOP: duplicate topic: {name}")
        topics[name] = mode
    if len(topics) != 38:
        raise SystemExit(f"STOP: expected 38 topics, found {len(topics)}")
    return topics


# ──────────────────────────────────────────────────────────────────────────
# Phase 1 — contract discovery
# ──────────────────────────────────────────────────────────────────────────

def discover_contract(consumer, gcs_client, bucket, topic, raw_index, sample_n):
    topic_blobs = raw_index.get(topic, {})
    if not topic_blobs:
        return {"status": "SKIP", "reason": "no_raw_objects"}

    metadata = consumer.list_topics(topic, timeout=15)
    info = metadata.topics.get(topic)
    if info is None or info.error:
        return {"status": "STOP", "reason": "topic_unavailable"}

    samples = []
    for p in sorted(info.partitions):
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, p), timeout=10, cached=False
        )
        if high <= low:
            continue
        consumer.assign([TopicPartition(topic, p, low)])
        msg = consumer.poll(timeout=10)
        if not msg or msg.error() or msg.value() is None:
            continue
        blob_name = topic_blobs.get((p, low))
        if not blob_name:
            continue
        try:
            schema, records = read_gcs_avro(gcs_client, bucket, blob_name)
        except Exception as exc:
            return {"status": "STOP", "reason": f"gcs_read_error:{exc}"}
        if not records:
            continue
        samples.append({
            "kafka_value": msg.value(),
            "gcs_schema": schema,
            "gcs_record": records[0],
        })
        if len(samples) >= sample_n:
            break

    if not samples:
        return {"status": "SKIP", "reason": "no_samples_read"}

    # Case A: embedded bytes
    for s in samples:
        paths = find_paths_with_bytes(s["gcs_record"], s["kafka_value"])
        if paths:
            return {
                "status": "PASS",
                "case": "A_embedded",
                "embedded_field": paths[0],
                "all_candidate_fields": paths,
                "shape": top_level_shape(s["gcs_record"]),
            }

    # Case B: re-serialize GCS record and compare to Kafka value
    match = 0
    mismatch = 0
    for s in samples:
        try:
            import io as _io
            buf = _io.BytesIO()
            fastavro.schemaless_writer(buf, s["gcs_schema"], s["gcs_record"])
            rebuilt = buf.getvalue()
        except Exception:
            continue
        if rebuilt == s["kafka_value"]:
            match += 1
        else:
            mismatch += 1
    if match > 0 and mismatch == 0:
        return {
            "status": "PASS",
            "case": "B_decoded",
            "shape": top_level_shape(samples[0]["gcs_record"]),
        }
    if match > 0 and mismatch > 0:
        return {
            "status": "UNVERIFIED",
            "case": "B_decoded_partial",
            "reason": "some_samples_reserialize_others_not",
            "sample_match": match, "sample_mismatch": mismatch,
        }

    # Case C: Pub/Sub wrapper detection by keys only
    for s in samples:
        keys = set(s["gcs_record"].keys()) if isinstance(s["gcs_record"], dict) else set()
        if {"data"} <= keys or {"message_id", "publish_time"} <= keys:
            return {
                "status": "UNVERIFIED",
                "case": "C_wrapped",
                "reason": "pubsub_envelope_detected_no_byte_equality_contract",
                "shape": top_level_shape(s["gcs_record"]),
            }

    return {
        "status": "UNVERIFIED",
        "case": "unknown",
        "reason": "no_encoding_case_matched",
        "shape": top_level_shape(samples[0]["gcs_record"]),
        "sample_kafka_value_len": len(samples[0]["kafka_value"]),
    }


# ──────────────────────────────────────────────────────────────────────────
# Phase 2 — payload integrity
# ──────────────────────────────────────────────────────────────────────────

def verify_payload(consumer, gcs_client, bucket, topic, contract, raw_index,
                   max_records=None):
    if contract.get("status") == "SKIP":
        return {"status": "N/A_EMPTY", "matches": 0, "mismatches": 0,
                "unsupported": 0, "errors": 0, "records_read": 0}
    if contract.get("status") == "STOP":
        return {"status": "STOP", "reason": contract.get("reason"),
                "matches": 0, "mismatches": 0, "unsupported": 0,
                "errors": 0, "records_read": 0}

    case = contract.get("case")
    embedded_field = contract.get("embedded_field")

    metadata = consumer.list_topics(topic, timeout=15)
    info = metadata.topics.get(topic)
    if info is None or info.error:
        return {"status": "STOP", "reason": "topic_unavailable",
                "matches": 0, "mismatches": 0, "unsupported": 0,
                "errors": 0, "records_read": 0}

    matches = mismatches = unsupported = errors = 0
    mismatch_examples, unsupported_examples, error_examples = [], [], []
    n_read = 0

    for p in sorted(info.partitions):
        low, high = consumer.get_watermark_offsets(
            TopicPartition(topic, p), timeout=10, cached=False
        )
        if high <= low:
            continue
        cur = low
        while cur < high:
            end = min(cur + 500, high)
            consumer.assign([TopicPartition(topic, p, cur)])
            for m in _drain(consumer, end - cur, timeout=15):
                if m.error() or m.value() is None:
                    continue
                offset = m.offset()
                blob_name = raw_index.get(topic, {}).get((p, offset))
                if not blob_name:
                    errors += 1
                    if len(error_examples) < 3:
                        error_examples.append(
                            {"partition": p, "offset": offset,
                             "reason": "no_raw_object"})
                    continue
                try:
                    _, records = read_gcs_avro(gcs_client, bucket, blob_name)
                except Exception as exc:
                    errors += 1
                    if len(error_examples) < 3:
                        error_examples.append(
                            {"partition": p, "offset": offset,
                             "reason": f"gcs_read:{exc}"})
                    continue
                if not records:
                    errors += 1
                    continue
                n_read += 1

                if case == "A_embedded" and embedded_field:
                    parts = embedded_field.split(".")
                    cur_val = records[0]
                    ok = True
                    for part in parts:
                        if isinstance(cur_val, dict) and part in cur_val:
                            cur_val = cur_val[part]
                        else:
                            ok = False
                            break
                    if not ok or not isinstance(cur_val, (bytes, bytearray)):
                        unsupported += 1
                        if len(unsupported_examples) < 3:
                            unsupported_examples.append(
                                {"partition": p, "offset": offset,
                                 "reason": "field_path_not_bytes"})
                    elif bytes(cur_val) == m.value():
                        matches += 1
                    else:
                        mismatches += 1
                        if len(mismatch_examples) < 3:
                            mismatch_examples.append(
                                {"partition": p, "offset": offset})
                else:
                    unsupported += 1
                    if len(unsupported_examples) < 3:
                        unsupported_examples.append(
                            {"partition": p, "offset": offset,
                             "reason": f"case_{case}_not_byte_comparable"})

                if max_records and n_read >= max_records:
                    break
            cur = end
            if max_records and n_read >= max_records:
                break
        if max_records and n_read >= max_records:
            break

    if n_read == 0:
        status = "UNVERIFIED"
    elif errors > 0 or mismatches > 0:
        status = "FAIL"
    elif unsupported > 0:
        status = "UNVERIFIED"
    elif matches == n_read:
        status = "PASS"
    else:
        status = "UNVERIFIED"

    return {
        "status": status, "case": case, "records_read": n_read,
        "matches": matches, "mismatches": mismatches,
        "unsupported": unsupported, "errors": errors,
        "mismatch_examples": mismatch_examples,
        "unsupported_examples": unsupported_examples,
        "error_examples": error_examples,
    }


def _drain(consumer, n, timeout):
    out = []
    got = 0
    while got < n:
        msg = consumer.poll(timeout=timeout)
        if msg is None:
            break
        out.append(msg)
        got += 1
    return out


# ──────────────────────────────────────────────────────────────────────────
# Phase 3 — source lineage
# ──────────────────────────────────────────────────────────────────────────

def discover_lineage(repo_root: Path, populated_topics):
    results = {}
    mobile_dir = repo_root / "data" / "mobile_networks"
    mobile_file_count = 0
    mobile_exts = set()
    if mobile_dir.is_dir():
        for p in mobile_dir.rglob("*"):
            if p.is_file():
                mobile_file_count += 1
                if p.suffix:
                    mobile_exts.add(p.suffix.lower())

    for topic in populated_topics:
        entry = {"topic": topic}
        if topic in EXPECTED_FILE_SOURCES:
            src = repo_root / EXPECTED_FILE_SOURCES[topic]
            if src.is_file():
                try:
                    data = json.loads(src.read_text())
                    entry["source"] = EXPECTED_FILE_SOURCES[topic]
                    entry["source_records"] = (
                        len(data) if isinstance(data, list) else None)
                    entry["status"] = "PASS"
                except Exception as exc:
                    entry["status"] = "UNVERIFIED"
                    entry["reason"] = f"source_unreadable:{exc}"
            else:
                entry["status"] = "UNVERIFIED"
                entry["reason"] = "source_file_missing"
            results[topic] = entry
            continue

        if topic in EXPECTED_MOBILE_TOPICS:
            entry["source"] = "data/mobile_networks/"
            entry["source_file_count"] = mobile_file_count
            entry["source_extensions"] = sorted(mobile_exts)
            entry["status"] = "UNVERIFIED"
            entry["reason"] = "directory_present_but_no_manifest_mapping"
            results[topic] = entry
            continue

        candidates = _grep_producers(repo_root, topic)
        if candidates:
            entry["producer_candidates"] = candidates
            entry["status"] = "UNVERIFIED"
            entry["reason"] = "producer_found_but_no_source_mapping"
        else:
            entry["status"] = "NO_SOURCE_EXPECTED"
            entry["reason"] = "no_file_source_and_no_producer_found"
        results[topic] = entry
    return results


def _grep_producers(repo_root: Path, topic: str):
    hits = []
    pattern = re.compile(rf"['\"]{re.escape(topic)}['\"]")
    for p in repo_root.rglob("*.py"):
        parts = set(p.parts)
        if ".venv" in parts or "node_modules" in parts or "__pycache__" in parts:
            continue
        try:
            text = p.read_text(errors="ignore")
        except Exception:
            continue
        if pattern.search(text):
            try:
                hits.append(str(p.relative_to(repo_root)))
            except ValueError:
                hits.append(str(p))
    return sorted(hits)[:5]


# ──────────────────────────────────────────────────────────────────────────
# Phase 4 — restricted MDM
# ──────────────────────────────────────────────────────────────────────────

def check_restricted(repo_root: Path, contracts):
    out = {}
    for topic in RESTRICTED_TOPICS:
        entry = {"topic": topic}
        c = contracts.get(topic, {})
        entry["records"] = (0 if c.get("status") == "SKIP"
                            else ("nonempty" if c.get("case") else "unknown"))
        src_rel = EXPECTED_FILE_SOURCES.get(topic)
        entry["source_file"] = src_rel
        entry["source_exists"] = bool(
            src_rel and (repo_root / src_rel).is_file())
        entry["producer_candidates"] = _grep_producers(repo_root, topic)

        if entry["source_exists"] and not entry["producer_candidates"]:
            entry["diagnosis"] = "PRODUCER_MISSING"
        elif entry["source_exists"] and entry["producer_candidates"] \
                and c.get("status") == "SKIP":
            entry["diagnosis"] = "PRODUCER_EXISTS_ROUTE_OR_AUTH_MISSING"
        elif not entry["source_exists"]:
            entry["diagnosis"] = "SOURCE_MISSING"
        else:
            entry["diagnosis"] = "UNVERIFIED"

        entry["working_golden_siblings"] = [
            "mdm.agent.golden", "mdm.geography.golden", "mdm.sacco.golden",
        ]
        out[topic] = entry
    return out


# ──────────────────────────────────────────────────────────────────────────
# Phase 5 — report
# ──────────────────────────────────────────────────────────────────────────

def emit_matrix(report):
    print("\n" + "=" * 120)
    print("SOURCE -> KAFKA -> GCS RAW INTEGRITY MATRIX")
    print("=" * 120)
    print(f"{'topic':<42} {'k_recs':>7} {'gcs_objs':>8} {'avro':>5} "
          f"{'STRUCT':<10} {'PAYLOAD':<14} {'LINEAGE':<18}")
    print("-" * 120)

    payloads, lineages = [], []
    for topic in sorted(report["topics"]):
        t = report["topics"][topic]
        payloads.append(t["payload_status"])
        lineages.append(t["lineage_status"])
        print(f"{topic:<42} "
              f"{t['kafka_records']:>7} {t['raw_objects']:>8} {t['avro_rows']:>5} "
              f"{t['structure_status']:<10} {t['payload_status']:<14} "
              f"{t['lineage_status']:<18}")
    print("-" * 120)

    def agg(values):
        if not values:
            return "UNVERIFIED"
        if any(v == "FAIL" for v in values):
            return "FAIL"
        if all(v in ("PASS", "N/A_EMPTY") for v in values):
            return "PASS"
        return "UNVERIFIED"

    print(f"{'AGGREGATE':<42} {'':>7} {'':>8} {'':>5} "
          f"{'PASS_STRUCTURE':<10} "
          f"{('PAYLOAD=' + agg(payloads)):<14} "
          f"{('LINEAGE=' + agg(lineages)):<18}")
    print("\nPAYLOAD legend: PASS | FAIL | UNVERIFIED | N/A_EMPTY")


def build_report(topics, contracts, payload, lineage, restricted,
                 raw_index, unmatched, kafka_counts, kafka_bytes):
    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "bucket": DEFAULTS["bucket"],
        "project": DEFAULTS["project"],
        "topics": {},
        "unmatched_raw_objects": unmatched,
        "restricted": restricted,
    }
    for topic in topics:
        kc = kafka_counts.get(topic, 0)
        raw = raw_index.get(topic, {})
        p = payload.get(topic, {})
        l = lineage.get(topic, {})
        avro_rows = p.get("records_read", len(raw))
        report["topics"][topic] = {
            "kafka_records": kc,
            "kafka_bytes": kafka_bytes.get(topic, 0),
            "raw_objects": len(raw),
            "avro_rows": avro_rows,
            "structure_status": (
                "PASS" if kc == len(raw) == avro_rows else "FAIL"),
            "payload_status": p.get("status", "UNVERIFIED"),
            "payload_detail": p,
            "contract": contracts.get(topic, {}),
            "lineage_status": l.get("status", "UNVERIFIED"),
            "lineage": l,
        }
    return report


# ──────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=DEFAULTS["repo_root"])
    parser.add_argument("--report", type=Path,
                        default=Path(DEFAULTS["report_json"]))
    parser.add_argument("--max-records-per-topic", type=int, default=None)
    args = parser.parse_args()

    repo = args.repo_root.resolve()
    topics = load_topics(repo, DEFAULTS["topics_yaml"])
    print(f"Loaded {len(topics)} governed topics")

    gcs_client = storage.Client(project=DEFAULTS["project"])
    if gcs_client.lookup_bucket(DEFAULTS["bucket"]) is None:
        raise SystemExit(f"STOP: bucket {DEFAULTS['bucket']} not found")
    raw_index, unmatched = list_raw_index(gcs_client, DEFAULTS["bucket"])
    print(f"Raw inventory: {sum(len(v) for v in raw_index.values())} objects, "
          f"{len(unmatched)} unmatched")

    consumer = Consumer({
        "bootstrap.servers": DEFAULTS["bootstrap"],
        "group.id": DEFAULTS["group"],
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
    })

    contracts, payload, kafka_counts, kafka_bytes = {}, {}, {}, {}
    try:
        print("\n--- Phase 1: contract discovery ---")
        for topic in topics:
            try:
                c = discover_contract(consumer, gcs_client, DEFAULTS["bucket"],
                                      topic, raw_index,
                                      DEFAULTS["sample_per_topic"])
            except Exception as exc:
                c = {"status": "STOP", "reason": f"discovery_error:{exc}"}
            contracts[topic] = c
            print(f"  {c.get('status','?'):<10} {topic}  "
                  f"case={c.get('case','-')}  reason={c.get('reason','-')}")

        print("\n--- Phase 2: payload integrity ---")
        for topic in topics:
            meta = consumer.list_topics(topic, timeout=15)
            info = meta.topics.get(topic)
            total = 0
            if info and not info.error:
                for p in sorted(info.partitions):
                    low, high = consumer.get_watermark_offsets(
                        TopicPartition(topic, p), timeout=10, cached=False)
                    total += max(0, high - low)
            kafka_counts[topic] = total

            if total == 0:
                payload[topic] = {"status": "N/A_EMPTY", "matches": 0,
                                  "mismatches": 0, "unsupported": 0,
                                  "errors": 0, "records_read": 0}
                print(f"  N/A_EMPTY  {topic}")
                continue

            try:
                pr = verify_payload(consumer, gcs_client, DEFAULTS["bucket"],
                                    topic, contracts[topic], raw_index,
                                    max_records=args.max_records_per_topic)
            except Exception as exc:
                pr = {"status": "FAIL", "reason": str(exc),
                      "matches": 0, "mismatches": 0, "unsupported": 0,
                      "errors": 1, "records_read": 0}
            payload[topic] = pr
            print(f"  {pr['status']:<10} {topic}  read={pr['records_read']} "
                  f"match={pr['matches']} mismatch={pr['mismatches']} "
                  f"unsupported={pr['unsupported']} err={pr['errors']}")
    finally:
        consumer.close()

    print("\n--- Phase 3: source lineage ---")
    populated = [t for t, n in kafka_counts.items() if n > 0]
    lineage = discover_lineage(repo, populated)
    for t, l in lineage.items():
        print(f"  {l['status']:<20} {t}  source={l.get('source','-')}")

    print("\n--- Phase 4: restricted MDM ---")
    restricted = check_restricted(repo, contracts)
    for t, r in restricted.items():
        print(f"  {r['diagnosis']:<40} {t}  "
              f"source_exists={r['source_exists']}  "
              f"producers={len(r['producer_candidates'])}")

    print("\n--- Phase 5: report ---")
    report = build_report(topics, contracts, payload, lineage, restricted,
                          raw_index, unmatched, kafka_counts, kafka_bytes)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True))
    print(f"JSON report: {args.report}")
    emit_matrix(report)

    any_fail = any(t["payload_status"] == "FAIL"
                   for t in report["topics"].values())
    print("\n" + ("FAIL: at least one topic failed payload verification"
                  if any_fail else
                  "PASS_STRUCTURE (see payload/lineage columns)"))
    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
