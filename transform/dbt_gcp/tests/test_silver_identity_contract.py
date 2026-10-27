"""Local contract tests for deterministic Silver classification."""

import unittest
from collections import defaultdict


BUSINESS_FIELDS = (
    "event_id",
    "payment_id",
    "event_type",
    "amount_minor",
    "currency",
    "event_timestamp",
)


def classify(rows):
    """Mirror the current six-field SQL grouping semantics."""
    identities = defaultdict(list)

    for row in rows:
        identities[row["event_id"]].append(
            tuple(row[field] for field in BUSINESS_FIELDS)
        )

    result = {}

    for event_id, observations in identities.items():
        variants = set(observations)

        result[event_id] = {
            "observation_count": len(observations),
            "distinct_payload_count": len(variants),
            "identity_status": (
                "IDENTITY_VERIFIED"
                if len(variants) == 1
                else "IDENTITY_CONFLICT"
            ),
        }

    return result


class SilverIdentityContractTests(unittest.TestCase):

    def setUp(self):
        self.event = {
            "event_id": "evt-001",
            "payment_id": "pay-001",
            "event_type": "INITIATED",
            "amount_minor": 10000,
            "currency": "GBP",
            "event_timestamp": "2026-10-27T10:00:00+00:00",
        }

    def test_single_event(self):
        result = classify([self.event])
        self.assertEqual(
            result["evt-001"]["identity_status"],
            "IDENTITY_VERIFIED",
        )

    def test_identical_replay(self):
        result = classify([self.event, dict(self.event)])
        self.assertEqual(
            result["evt-001"]["distinct_payload_count"],
            1,
        )
        self.assertEqual(
            result["evt-001"]["observation_count"],
            2,
        )

    def test_conflicting_amount(self):
        changed = dict(self.event, amount_minor=20000)
        result = classify([self.event, changed])
        self.assertEqual(
            result["evt-001"]["identity_status"],
            "IDENTITY_CONFLICT",
        )

    def test_conflict_order_independence(self):
        changed = dict(self.event, currency="EUR")

        forward = classify([self.event, changed])
        reverse = classify([changed, self.event])

        self.assertEqual(forward, reverse)

    def test_distinct_lifecycle_events(self):
        completed = dict(
            self.event,
            event_id="evt-002",
            event_type="COMPLETED",
        )

        result = classify([self.event, completed])

        self.assertEqual(len(result), 2)
        self.assertTrue(
            all(
                value["identity_status"] == "IDENTITY_VERIFIED"
                for value in result.values()
            )
        )

    def test_conflicting_timestamp(self):
        changed = dict(
            self.event,
            event_timestamp="2026-10-27T11:00:00+00:00",
        )
        result = classify([self.event, changed])
        self.assertEqual(
            result["evt-001"]["distinct_payload_count"],
            2,
        )


if __name__ == "__main__":
    unittest.main()
