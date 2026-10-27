"""Deterministic identity rules for payment events."""

import hashlib
import json

BUSINESS_FIELDS = (
    "event_id",
    "payment_id",
    "event_type",
    "amount_minor",
    "currency",
    "event_timestamp",
)


def canonical_payload(event):
    """Represent a validated event deterministically."""
    return json.dumps(
        {field: event[field] for field in BUSINESS_FIELDS},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def event_fingerprint(event):
    """Return a stable SHA-256 fingerprint."""
    return hashlib.sha256(
        canonical_payload(event).encode("utf-8")
    ).hexdigest()


def classify_events(events):
    """Classify a bounded observation set by event identity.

    A conflicted identity has no trusted representative.
    Classification does not depend on observation order.
    """
    identities = {}

    for event in events:
        event_id = event["event_id"]
        fingerprint = event_fingerprint(event)

        variants = identities.setdefault(event_id, {})
        variants.setdefault(fingerprint, []).append(event)

    unique = []
    replays = []
    conflicts = []

    for event_id in sorted(identities):
        variants = identities[event_id]

        if len(variants) > 1:
            for fingerprint in sorted(variants):
                observations = variants[fingerprint]
                conflicts.append(observations[0])
                replays.extend(observations[1:])
        else:
            observations = next(iter(variants.values()))
            unique.append(observations[0])
            replays.extend(observations[1:])

    return {
        "unique": unique,
        "replays": replays,
        "conflicts": conflicts,
    }
