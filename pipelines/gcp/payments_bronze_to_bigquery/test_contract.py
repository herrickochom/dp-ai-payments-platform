import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SCHEMA = (
    ROOT
    / "infrastructure/terraform/gcp/experiments/pubsub_payments"
    / "payment-event-v1.avsc"
)

PIPELINE = Path(__file__).with_name("transform.py")


def load_transform():
    spec = importlib.util.spec_from_file_location(
        "payments_transform", PIPELINE
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.transform_payment_event


class PaymentContractTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads(SCHEMA.read_text())
        cls.transform = staticmethod(load_transform())

    def valid_event(self):
        return {
            "event_id": "evt-001",
            "payment_id": "pmt-001",
            "event_type": "PAYMENT_CREATED",
            "amount_minor": 12500,
            "currency": "GBP",
            "event_timestamp": "2026-10-27T12:00:00Z",
        }

    def test_schema_fields(self):
        actual = {
            field["name"]
            for field in self.schema["fields"]
        }
        self.assertEqual(actual, set(self.valid_event()))

    def test_valid_payment(self):
        result = self.transform(json.dumps(self.valid_event()))
        self.assertEqual(result["amount_minor"], 12500)
        self.assertEqual(result["currency"], "GBP")

    def test_timestamp_normalisation(self):
        event = self.valid_event()
        event["event_timestamp"] = "2026-10-27T13:00:00+01:00"

        result = self.transform(json.dumps(event))
        self.assertEqual(
            result["event_timestamp"],
            "2026-10-27T12:00:00+00:00",
        )

    def test_missing_field(self):
        event = self.valid_event()
        del event["payment_id"]

        with self.assertRaises(ValueError):
            self.transform(json.dumps(event))

    def test_negative_amount(self):
        event = self.valid_event()
        event["amount_minor"] = -100

        with self.assertRaises(ValueError):
            self.transform(json.dumps(event))

    def test_boolean_amount(self):
        event = self.valid_event()
        event["amount_minor"] = True

        with self.assertRaises(ValueError):
            self.transform(json.dumps(event))

    def test_invalid_timestamp(self):
        event = self.valid_event()
        event["event_timestamp"] = "not-a-timestamp"

        with self.assertRaises(ValueError):
            self.transform(json.dumps(event))

    def test_timezone_required(self):
        event = self.valid_event()
        event["event_timestamp"] = "2026-10-27T12:00:00"

        with self.assertRaises(ValueError):
            self.transform(json.dumps(event))


if __name__ == "__main__":
    unittest.main()
