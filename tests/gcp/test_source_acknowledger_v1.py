import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

MODULE = (
    ROOT
    / "platform/gcp/pubsub_raw_ingestion/"
    "source_acknowledger_v1.py"
)

spec = importlib.util.spec_from_file_location(
    "source_acknowledger_v1", MODULE
)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class FakeSubscriber:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def acknowledge(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return None


class AcknowledgementAcceptance(unittest.TestCase):
    def test_exact_acknowledgement_request(self):
        subscriber = FakeSubscriber()
        acker = mod.SourceAcknowledger(
            subscriber,
            "projects/test/subscriptions/source",
        )

        acker.ack("ack-17")

        self.assertEqual(len(subscriber.calls), 1)
        self.assertEqual(
            subscriber.calls[0],
            {
                "request": {
                    "subscription":
                        "projects/test/subscriptions/source",
                    "ack_ids": ["ack-17"],
                },
                "timeout": 5.0,
                "retry": None,
            },
        )

    def test_custom_timeout(self):
        subscriber = FakeSubscriber()
        acker = mod.SourceAcknowledger(
            subscriber,
            "subscription",
            rpc_timeout_seconds=2.0,
        )

        acker.ack("ack-id")

        self.assertEqual(
            subscriber.calls[0]["timeout"], 2.0
        )

    def test_acknowledgement_failure_propagates(self):
        subscriber = FakeSubscriber(
            error=TimeoutError("simulated timeout")
        )

        acker = mod.SourceAcknowledger(
            subscriber, "subscription"
        )

        with self.assertRaises(TimeoutError):
            acker.ack("ack-id")

        self.assertEqual(len(subscriber.calls), 1)

    def test_missing_ack_id_rejected(self):
        subscriber = FakeSubscriber()
        acker = mod.SourceAcknowledger(
            subscriber, "subscription"
        )

        with self.assertRaises(
            mod.AcknowledgementConfigurationError
        ):
            acker.ack("")

        self.assertEqual(subscriber.calls, [])

    def test_invalid_timeout_rejected(self):
        for value in (
            0,
            -1,
            float("inf"),
            float("nan"),
            True,
        ):
            with self.subTest(value=value):
                with self.assertRaises(
                    mod.AcknowledgementConfigurationError
                ):
                    mod.SourceAcknowledger(
                        FakeSubscriber(),
                        "subscription",
                        rpc_timeout_seconds=value,
                    )

    def test_missing_subscriber_method_rejected(self):
        with self.assertRaises(
            mod.AcknowledgementConfigurationError
        ):
            mod.SourceAcknowledger(
                object(), "subscription"
            )

    def test_missing_subscription_rejected(self):
        with self.assertRaises(
            mod.AcknowledgementConfigurationError
        ):
            mod.SourceAcknowledger(
                FakeSubscriber(), ""
            )


if __name__ == "__main__":
    unittest.main()
