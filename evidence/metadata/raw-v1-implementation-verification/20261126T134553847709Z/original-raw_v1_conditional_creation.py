
"""Conditional immutable Raw object creation.

No acknowledgement or subscriber operation is performed here.
"""

import hashlib

from google.api_core.exceptions import PreconditionFailed

from raw_v1_metadata import snapshot, read_payload
from raw_v1_metadata_contract import plan_metadata_update


def create_and_verify(bucket, name, body, decoder):
    proposed, decoded = decoder.derive_metadata(name, body)

    blob = bucket.blob(name)
    blob.metadata = dict(proposed)
    blob.cache_control = "no-store"

    created = False

    try:
        blob.upload_from_string(
            body,
            content_type="application/avro",
            if_generation_match=0,
            checksum="crc32c"
        )
        created = True

    except PreconditionFailed:
        # Existing object: verify, never overwrite or patch.
        pass

    fresh = bucket.blob(name)
    fresh.reload()

    state = snapshot(fresh)

    stored = read_payload(fresh, state)

    if stored != body:
        raise RuntimeError("Conflicting immutable Raw object")

    if hashlib.sha256(stored).digest() != hashlib.sha256(body).digest():
        raise RuntimeError("Raw object digest mismatch")

    merged, changed = plan_metadata_update(state, proposed)

    if changed:
        raise RuntimeError(
            "Existing object requires controlled metadata remediation"
        )

    if created and state["metadata"] != proposed:
        raise RuntimeError("Unexpected creation metadata")

    return {
        "key": name,
        "created": created,
        "sha256": hashlib.sha256(body).hexdigest(),
        "source_payload_sha256": decoded["payload_sha256"],
        "generation": state["generation"],
        "metageneration": state["metageneration"],
        "metadata_verified": True,
        "payload_verified": True,
        "status": (
            "CREATED_VERIFIED" if created
            else "ALREADY_PRESENT_IDENTICAL"
        )
    }
