#!/usr/bin/env python3
"""Governed single-object Raw v1 metadata utility."""

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import storage

from raw_v1_metadata_contract import (
    MetadataContract,
    plan_metadata_update
)
from raw_v1_avro_decoder import RawAvroDecoder

ROOT = Path(__file__).resolve().parents[2]

SNAPSHOT_KEYS = (
    "generation", "metageneration", "crc32c",
    "md5_hash", "size", "content_type",
    "cache_control", "content_encoding",
    "content_disposition", "content_language",
    "custom_time", "metadata"
)

INTEGRITY_KEYS = (
    "generation", "crc32c", "md5_hash", "size",
    "content_type", "content_encoding",
    "content_disposition", "content_language",
    "custom_time"
)


def snapshot(blob):
    result = {k: getattr(blob, k) for k in SNAPSHOT_KEYS}
    result["metadata"] = dict(result["metadata"] or {})
    return result


def read_payload(blob, state):
    payload = blob.download_as_bytes(
        raw_download=True,
        if_generation_match=state["generation"],
        if_metageneration_match=state["metageneration"],
        checksum="crc32c"
    )

    if len(payload) != state["size"]:
        raise ValueError("Object size mismatch")

    return payload


def execute(bucket, name, decoder, *, apply=False,
            report=None, persist=lambda: None):

    if report is None:
        report = {}

    blob = bucket.blob(name)
    blob.reload()
    before = snapshot(blob)

    report["before"] = before

    original = read_payload(blob, before)

    proposed, decoded = decoder.derive_metadata(name, original)

    report["envelope_name"] = decoded["envelope_name"]
    report["source_payload_sha256"] = decoded["payload_sha256"]
    report["raw_object_sha256"] = hashlib.sha256(original).hexdigest()
    report["proposed_custom_metadata"] = proposed

    conflicts = {
        key: {
            "existing": before["metadata"][key],
            "expected": value
        }
        for key, value in proposed.items()
        if key in before["metadata"]
        and before["metadata"][key] != value
    }

    if conflicts:
        report["conflicts"] = conflicts
        raise ValueError("Metadata conflict; controlled remediation required")

    merged, changed = plan_metadata_update(before, proposed)

    report["proposed_metadata"] = merged
    report["proposed_cache_control"] = "no-store"
    report["update_required"] = changed

    patched = apply and changed

    if patched:
        report["status"] = "UPDATE_PENDING"
        persist()

        blob.metadata = merged
        blob.cache_control = "no-store"

        blob.patch(
            if_generation_match=before["generation"],
            if_metageneration_match=before["metageneration"]
        )

        report["status"] = "UPDATE_SENT_VERIFICATION_PENDING"
        persist()

    fresh = bucket.blob(name)
    fresh.reload()
    after = snapshot(fresh)

    report["after"] = after

    verified = read_payload(fresh, after)

    checks = {
        key: before[key] == after[key]
        for key in INTEGRITY_KEYS
    }

    checks["payload_bytes_unchanged"] = verified == original

    checks["sha256_unchanged"] = (
        hashlib.sha256(verified).digest()
        == hashlib.sha256(original).digest()
    )

    checks["metageneration"] = (
        int(after["metageneration"])
        == int(before["metageneration"]) + int(patched)
    )

    checks["metadata"] = (
        after["metadata"]
        == (merged if patched else before["metadata"])
    )

    checks["cache_control"] = (
        after["cache_control"]
        == ("no-store" if patched else before["cache_control"])
    )

    report["checks"] = checks

    if not all(checks.values()):
        raise RuntimeError("Independent object verification failed")

    report["status"] = (
        "PASS" if patched else
        "NO_CHANGE_REQUIRED" if apply else
        "DRY_RUN_PASS"
    )

    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--project", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--object", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--evidence-dir", default="metadata-evidence"
    )

    args = parser.parse_args()

    directory = Path(args.evidence_dir)
    directory.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )

    evidence = directory / f"raw-metadata-{stamp}.json"

    report = {
        "uri": f"gs://{args.bucket}/{args.object}",
        "apply": args.apply,
        "status": "STARTED",
        "metadata_contract_version": "1.0.0"
    }

    def save():
        temporary = evidence.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(report, indent=2, default=str) + "\n"
        )
        temporary.replace(evidence)

    try:
        contract = MetadataContract(ROOT)
        decoder = RawAvroDecoder(contract, ROOT)

        environment = (
            "dp-pdm-ai-dev-0b4da804",
            "dp-pdm-ai-dev-0b4da804-data"
        )

        if (args.project, args.bucket) != environment:
            raise ValueError("Unapproved project or bucket")

        client = storage.Client(project=args.project)
        bucket = client.bucket(args.bucket)
        bucket.reload()

        if str(bucket.project_number) != "324778038883":
            raise ValueError("Bucket ownership mismatch")

        if bucket.location.upper() != "EUROPE-WEST2":
            raise ValueError("Bucket location mismatch")

        execute(
            bucket, args.object, decoder,
            apply=args.apply,
            report=report,
            persist=save
        )

    except Exception as exc:
        report["previous_status"] = report["status"]
        report["status"] = "FAIL"
        report["error_type"] = type(exc).__name__
        raise

    finally:
        save()
        print(json.dumps(report, indent=2, default=str))
        print(f"Evidence: {evidence}")


if __name__ == "__main__":
    main()
