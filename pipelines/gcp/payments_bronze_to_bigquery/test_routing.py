"""Apache Beam tests for payment validation and quarantine."""

import json
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that

from routing import ValidateAndRoute


class PaymentRoutingTests(unittest.TestCase):

    def test_valid_and_invalid_records(self):
        valid = {
            "event_id": "evt-001",
            "payment_id": "pmt-001",
            "event_type": "PAYMENT_CREATED",
            "amount_minor": 12500,
            "currency": "GBP",
            "event_timestamp": "2026-10-27T12:00:00Z",
        }

        missing = dict(valid)
        del missing["payment_id"]

        negative = dict(valid)
        negative["amount_minor"] = -1

        bad_timestamp = dict(valid)
        bad_timestamp["event_timestamp"] = "invalid"

        source = [
            json.dumps(valid),
            json.dumps(missing),
            json.dumps(negative),
            json.dumps(bad_timestamp),
            "{invalid-json",
        ]

        def check_valid(actual):
            self.assertEqual(len(actual), 1)
            self.assertEqual(actual[0]["event_id"], "evt-001")
            self.assertEqual(actual[0]["amount_minor"], 12500)

        def check_invalid(actual):
            self.assertEqual(len(actual), 4)

            for record in actual:
                self.assertIn("raw_payload", record)
                self.assertTrue(record["error_reason"])
                self.assertTrue(record["processing_timestamp"])

            reasons = " ".join(
                record["error_reason"] for record in actual
            )

            self.assertIn("payment_id", reasons)
            self.assertIn("negative", reasons)
            self.assertIn("invalid isoformat string", reasons.lower())

        with TestPipeline() as pipeline:
            routed = (
                pipeline
                | "Create Test Events" >> beam.Create(source)
                | "Route Test Events" >> beam.ParDo(
                    ValidateAndRoute()
                ).with_outputs("invalid", main="valid")
            )

            assert_that(
                routed.valid,
                check_valid,
                label="Assert Valid Events",
            )

            assert_that(
                routed.invalid,
                check_invalid,
                label="Assert Quarantined Events",
            )


if __name__ == "__main__":
    unittest.main()
