import importlib.util
import unittest
from pathlib import Path


MODULE = (
    Path(__file__).resolve().parents[2]
    / "platform/gcp/pubsub_raw_ingestion/source_lease_extension_v1.py"
)

spec = importlib.util.spec_from_file_location(
    "source_lease_extension_v1", MODULE
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class FakeSubscriber:
    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def modify_ack_deadline(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return None


class LeaseExtensionAcceptance(unittest.TestCase):
    def test_exact_request_and_bounded_rpc(self):
        subscriber = FakeSubscriber()
        extender = mod.SourceLeaseExtender(
            subscriber,
            "projects/test/subscriptions/source",
            "ack-123",
        )

        extender.extend(object())

        self.assertEqual(len(subscriber.calls), 1)
        self.assertEqual(
            subscriber.calls[0]["request"],
            {
                "subscription": "projects/test/subscriptions/source",
                "ack_ids": ["ack-123"],
                "ack_deadline_seconds": 30,
            },
        )
        self.assertEqual(subscriber.calls[0]["timeout"], 5.0)
        self.assertIsNone(subscriber.calls[0]["retry"])

    def test_custom_bounds_forwarded(self):
        subscriber = FakeSubscriber()
        extender = mod.SourceLeaseExtender(
            subscriber,
            "subscription",
            "ack-id",
            extension_seconds=20,
            rpc_timeout_seconds=2.0,
        )

        extender.extend(None)

        call = subscriber.calls[0]
        self.assertEqual(
            call["request"]["ack_deadline_seconds"], 20
        )
        self.assertEqual(call["timeout"], 2.0)

    def test_rpc_failure_propagates(self):
        subscriber = FakeSubscriber(
            error=TimeoutError("simulated RPC timeout")
        )
        extender = mod.SourceLeaseExtender(
            subscriber, "subscription", "ack-id"
        )

        with self.assertRaises(TimeoutError):
            extender.extend(None)

        self.assertEqual(len(subscriber.calls), 1)

    def test_invalid_extension_values(self):
        for value in (0, -1, 601, 1.5, True):
            with self.subTest(value=value):
                with self.assertRaises(
                    mod.LeaseExtensionError
                ):
                    mod.SourceLeaseExtender(
                        FakeSubscriber(),
                        "subscription",
                        "ack-id",
                        extension_seconds=value,
                    )

    def test_invalid_rpc_timeouts(self):
        for value in (0, -1, float("inf"), float("nan"), True):
            with self.subTest(value=value):
                with self.assertRaises(
                    mod.LeaseExtensionError
                ):
                    mod.SourceLeaseExtender(
                        FakeSubscriber(),
                        "subscription",
                        "ack-id",
                        rpc_timeout_seconds=value,
                    )

    def test_missing_ack_id_rejected(self):
        with self.assertRaises(mod.LeaseExtensionError):
            mod.SourceLeaseExtender(
                FakeSubscriber(), "subscription", ""
            )

    def test_missing_subscriber_method_rejected(self):
        with self.assertRaises(mod.LeaseExtensionError):
            mod.SourceLeaseExtender(
                object(), "subscription", "ack-id"
            )


if __name__ == "__main__":
    unittest.main()
