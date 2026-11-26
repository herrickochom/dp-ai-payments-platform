"""Controlled Raw v1 metadata remediation.

Dry-run by default. Bulk execution is deliberately unsupported.
"""

import argparse
import hashlib
import json
import os
import sys

from datetime import datetime, timezone
from pathlib import Path

from raw_v1_metadata import snapshot, read_payload
from raw_v1_metadata_contract import MetadataContract
from raw_v1_avro_decoder import RawAvroDecoder

ROOT = Path(__file__).resolve().parents[2]

PROJECT = "dp-pdm-ai-dev-0b4da804"
BUCKET = "dp-pdm-ai-dev-0b4da804-data"
PROJECT_NUMBER = "324778038883"
REGION = "EUROPE-WEST2"

PILOT_SHA = (
    "d68fc6ee31c86f051a3c6a42f3b6ab6d35"
    "ea8fa896cd17ee48f0bafc7b01fa90"
)

CONFLICT_OBJECT = (
    "raw/v1/streaming/category=agent_network/"
    "source_group=agent/source_system=agent/"
    "year=2026/month=11/day=25/"
    "topic=agent.locations/partition=0/offset=0/record.avro"
)

INTEGRITY_FIELDS = (
    "generation", "crc32c", "md5_hash", "size",
    "content_type", "content_encoding",
    "content_disposition", "content_language", "custom_time"
)

NATIVE_EXPECTED = {
    "content_type": "application/avro",
    "content_encoding": None,
    "content_disposition": None,
    "content_language": None,
    "custom_time": None
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(body):
    return hashlib.sha256(body).hexdigest()


def load_manifest(path):
    body = Path(path).read_bytes()

    require(digest(body) == PILOT_SHA, "Manifest checksum mismatch")

    manifest = json.loads(body)

    require(manifest["project"] == PROJECT, "Project mismatch")
    require(manifest["bucket"] == BUCKET, "Bucket mismatch")

    require(
        manifest["metadata_contract_version"] == "1.0.0",
        "Contract version mismatch"
    )

    require(manifest["pilot_objects"] == 10, "Pilot count mismatch")
    require(len(manifest["objects"]) == 10, "Invalid object count")

    require(
        manifest["execution_authorised"] is False,
        "Unexpected embedded authorisation"
    )

    require(
        manifest["bulk_execution_authorised"] is False,
        "Bulk execution prohibited"
    )

    objects = manifest["objects"]

    require(
        len({item["object"] for item in objects}) == 10,
        "Duplicate pilot objects"
    )

    require(
        all(item["execution_authorised"] is False for item in objects),
        "Unexpected object authorisation"
    )

    return manifest


def validate_conflict(item, actual, expected, allow_conflict):
    conflicts = sorted(
        key for key, value in expected.items()
        if key in actual and actual[key] != value
    )

    if not conflicts:
        return

    require(allow_conflict, "Conflict migration not authorised")

    require(
        item["remediation_type"] == "EXPLICIT_CONFLICT_MIGRATION",
        "Unexpected conflict migration"
    )

    require(
        item["object"] == CONFLICT_OBJECT,
        "Conflict object not authorised"
    )

    require(conflicts == ["data-domain"], "Unexpected conflicting keys")

    require(
        actual["data-domain"] == "payments",
        "Unexpected original domain"
    )

    require(
        expected["data-domain"] == "agent_management",
        "Unexpected target domain"
    )


def assess(bucket, item, decoder, allow_conflict=False):
    blob = bucket.blob(item["object"])
    blob.reload()

    state = snapshot(blob)

    require(
        str(state["generation"]) == str(item["expected_generation"]),
        "Object generation changed"
    )

    require(
        state["size"] == item["expected_size_bytes"],
        "Object size changed"
    )

    body = read_payload(blob, state)

    require(
        digest(body) == item["expected_raw_sha256"],
        "Raw payload checksum mismatch"
    )

    expected, decoded = decoder.derive_metadata(item["object"], body)

    require(expected == item["expected_metadata"], "Metadata mismatch")

    require(
        decoded["payload_sha256"] ==
        item["expected_source_payload_sha256"],
        "Source payload checksum mismatch"
    )

    require(len(expected) == 11, "Mandatory metadata count mismatch")

    for key, value in NATIVE_EXPECTED.items():
        require(state.get(key) == value, f"Invalid native metadata: {key}")

    for key in INTEGRITY_FIELDS:
        require(
            state.get(key) == item["before"].get(key),
            f"Original integrity baseline changed: {key}"
        )

    actual = state["metadata"] or {}

    proposed = dict(item["before"]["metadata"] or {})
    proposed.update(expected)

    require(
        proposed == item["proposed_metadata"],
        "Proposed metadata differs from manifest"
    )

    original_metageneration = int(item["expected_metageneration"])
    current_metageneration = int(state["metageneration"])

    compliant = (
        actual == proposed
        and state["cache_control"] == "no-store"
    )

    if compliant:
        require(
            current_metageneration in {
                original_metageneration,
                original_metageneration + 1
            },
            "Unexpected compliant metageneration"
        )

        return blob, state, body, proposed, True

    require(
        current_metageneration == original_metageneration,
        "Object metageneration changed"
    )

    require(state == item["before"], "Original metadata baseline changed")

    validate_conflict(item, actual, expected, allow_conflict)

    return blob, state, body, proposed, False


def remediate(
    bucket, item, decoder, *,
    apply=False,
    allow_conflict=False,
    record=lambda event: None
):
    blob, before, body, proposed, compliant = assess(
        bucket, item, decoder, allow_conflict
    )

    if compliant:
        return {
            "object": item["object"],
            "status": "ALREADY_COMPLIANT",
            "payload_verified": True,
            "generation": before["generation"],
            "metageneration": before["metageneration"]
        }

    if not apply:
        return {
            "object": item["object"],
            "status": "DRY_RUN_PASS",
            "payload_verified": True,
            "proposed_metadata": proposed
        }

    record({
        "object": item["object"],
        "status": "PATCH_INTENT",
        "generation": before["generation"],
        "metageneration": before["metageneration"],
        "raw_sha256": digest(body)
    })

    blob.metadata = proposed
    blob.cache_control = "no-store"

    blob.patch(
        if_generation_match=int(before["generation"]),
        if_metageneration_match=int(before["metageneration"])
    )

    fresh = bucket.blob(item["object"])
    fresh.reload()

    after = snapshot(fresh)
    after_body = read_payload(fresh, after)

    for key in INTEGRITY_FIELDS:
        require(before[key] == after[key], f"Integrity changed: {key}")

    require(after_body == body, "Payload bytes changed")

    require(
        after["metadata"] == proposed,
        "Post-update metadata mismatch"
    )

    require(after["cache_control"] == "no-store", "Cache control mismatch")

    require(
        int(after["metageneration"]) == int(before["metageneration"]) + 1,
        "Unexpected metageneration increment"
    )

    return {
        "object": item["object"],
        "status": "UPDATED_VERIFIED",
        "payload_verified": True,
        "raw_sha256": digest(after_body),
        "generation": after["generation"],
        "metageneration": after["metageneration"]
    }


def append_event(path, event):
    event = {
        **event,
        "timestamp_utc": datetime.now(timezone.utc).isoformat()
    }

    with Path(path).open("a") as f:
        f.write(json.dumps(event, default=str) + "\n")
        f.flush()
        os.fsync(f.fileno())


def run_pilot(
    bucket, manifest, decoder, journal,
    *, apply=False, allow_conflict=False
):
    require(
        not apply,
        "Live writes require the controlled safety entry point"
    )

    objects = manifest["objects"]

    require(len(objects) == 10, "Only the approved ten-object pilot is supported")

    # Preflight every object before the first write.
    for item in objects:
        assess(bucket, item, decoder, allow_conflict)

    results = []

    for item in objects:
        try:
            result = remediate(
                bucket, item, decoder,
                apply=apply,
                allow_conflict=allow_conflict,
                record=lambda event: append_event(journal, event)
            )

            append_event(journal, result)
            results.append(result)

        except Exception as exc:
            append_event(journal, {
                "object": item["object"],
                "status": "QUARANTINED_REVIEW_REQUIRED",
                "error_type": type(exc).__name__
            })
            raise

    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--manifest", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--authorise-pilot")
    parser.add_argument("--authorise-conflict-migration", action="store_true")

    args = parser.parse_args()

    manifest = load_manifest(args.manifest)

    if args.apply:
        require(
            args.authorise_pilot == PILOT_SHA,
            "Explicit pilot authorisation required"
        )

        require(
            args.authorise_conflict_migration,
            "Explicit historical conflict authorisation required"
        )

    from google.cloud import storage

    contract = MetadataContract(ROOT)
    decoder = RawAvroDecoder(contract, ROOT)

    client = storage.Client(project=PROJECT)
    bucket = client.get_bucket(BUCKET)

    require(str(bucket.project_number) == PROJECT_NUMBER, "Bucket ownership mismatch")
    require(bucket.location.upper() == REGION, "Bucket region mismatch")

    directory = Path(args.evidence_dir)
    directory.mkdir(parents=True, exist_ok=True)

    journal = directory / "remediation-execution.jsonl"

    import raw_v1_remediation_safety as safety

    if args.apply:
        results = safety.run_controlled_apply(
            bucket, args.manifest, decoder
        )
    else:
        results = safety.run_read_only(
            bucket,
            args.manifest,
            decoder,
            journal,
            safety.LOCK_PATH
        )

    print(json.dumps({
        "mode": "APPLY" if args.apply else "DRY_RUN",
        "objects": len(results),
        "results": results,
        "bulk_execution": "DISABLED"
    }, indent=2, default=str))


if __name__ == "__main__":
    main()
