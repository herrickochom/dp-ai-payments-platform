"""Local payment event idempotency contract tests."""

import copy
import unittest

from idempotency import (
    canonical_payload,
    classify_events,
    event_fingerprint,
)


class PaymentIdempotencyTests(unittest.TestCase):

    def setUp(self):
        self.event = {
            "event_id": "evt-001",
            "payment_id": "pay-001",
            "event_type": "PAYMENT_INITIATED",
            "amount_minor": 1500,
            "currency": "GBP",
            "event_timestamp": "2026-10-27T10:00:00+00:00",
        }

    def test_fingerprint_deterministic(self):
        reordered = dict(reversed(list(self.event.items())))
        self.assertEqual(
            event_fingerprint(self.event),
            event_fingerprint(reordered),
        )

    def test_identical_replay(self):
        result = classify_events([
            self.event,
            copy.deepcopy(self.event),
        ])
        self.assertEqual(len(result["unique"]), 1)
        self.assertEqual(len(result["replays"]), 1)
        self.assertEqual(len(result["conflicts"]), 0)

    def test_conflicting_event_identity(self):
        changed = dict(self.event, amount_minor=2000)
        result = classify_events([self.event, changed])

        self.assertEqual(len(result["unique"]), 0)
        self.assertEqual(len(result["conflicts"]), 2)
        self.assertEqual(len(result["replays"]), 0)

        self.assertEqual(
            {event_fingerprint(e) for e in result["conflicts"]},
            {
                event_fingerprint(self.event),
                event_fingerprint(changed),
            },
        )

    def test_conflict_classification_independent_of_order(self):
        first = dict(self.event)
        second = dict(self.event, amount_minor=2000)

        forward = classify_events([first, second])
        reverse = classify_events([second, first])

        self.assertEqual(
            {event_fingerprint(e) for e in forward["unique"]},
            {event_fingerprint(e) for e in reverse["unique"]},
        )

    def test_conflicted_identity_not_trusted(self):
        first = dict(self.event)
        second = dict(self.event, amount_minor=2000)

        result = classify_events([first, second])

        self.assertFalse(
            any(
                event["event_id"] == "evt-001"
                for event in result["unique"]
            )
        )

    def test_multiple_lifecycle_events(self):
        second = dict(
            self.event,
            event_id="evt-002",
            event_type="PAYMENT_SETTLED",
        )
        result = classify_events([self.event, second])
        self.assertEqual(len(result["unique"]), 2)

    def test_fingerprint_excludes_processing_metadata(self):
        enriched = dict(
            self.event,
            processing_timestamp="2026-10-27T11:00:00Z",
        )
        self.assertEqual(
            event_fingerprint(self.event),
            event_fingerprint(enriched),
        )

    def test_canonical_payload(self):
        result = canonical_payload(self.event)
        self.assertIn('"event_id":"evt-001"', result)


if __name__ == "__main__":
    unittest.main()
