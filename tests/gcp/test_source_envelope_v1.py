import base64
import hashlib
import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "platform/gcp/kafka_pubsub_bridge/source_envelope_v1.py"
spec = importlib.util.spec_from_file_location("source_envelope_v1", MODULE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

def envelope(topic, raw, schema_id=None):
    payload = b"" if raw is None else raw
    result = {"kafka_topic": topic, "kafka_partition": 0, "kafka_offset": 17,
              "kafka_timestamp_ms": 1770000000000,
              "payload_base64": base64.b64encode(payload).decode("ascii"),
              "payload_sha256": hashlib.sha256(payload).hexdigest(),
              "schema_registry_id": schema_id}
    if mod.ROUTES[topic]["status"] != "legacy_payment_event":
        result["value_is_null"] = raw is None
    return result

_stage12_original_envelope = envelope

def envelope(*args, **kwargs):
    result = _stage12_original_envelope(*args, **kwargs)
    result["message_type"] = "payments.source-event.v1"
    return result


class TestSourceEnvelope(unittest.TestCase):
    def test_38_routes(self):
        self.assertEqual(len(mod.ROUTES), 38)
    def test_restricted_fail_closed(self):
        for topic in ("mdm.beneficiary.golden.restricted", "mdm.beneficiary.identity-alert.restricted", "mdm.crosswalk.restricted"):
            with self.assertRaises(mod.RouteRejected):
                mod.route_for(topic)
    def test_unverified_mdm_fail_closed(self):
        with self.assertRaises(mod.RouteRejected):
            mod.validate_envelope(envelope("mdm.sacco.golden", b'{}'))
    def test_json_retry(self):
        value = mod.validate_envelope(envelope("payment-events.retry", b'{"reason":"test"}'))
        self.assertEqual(value["payload_format"], "json")
    def test_cdc_tombstone(self):
        value = mod.validate_envelope(envelope("cdc.pdm_mdm.mdm.sacco_master_sources", None))
        self.assertEqual(value["payload_format"], "tombstone")
    def test_missing_null_indicator_rejected(self):
        e = envelope("payment-events.retry", b'{}')
        del e["value_is_null"]
        with self.assertRaises(mod.RouteRejected):
            mod.validate_envelope(e)

    def test_nonboolean_null_indicator_rejected(self):
        e = envelope("payment-events.retry", b'{}')
        e["value_is_null"] = 1
        with self.assertRaises(mod.RouteRejected):
            mod.validate_envelope(e)

    def test_empty_nonnull_cdc_rejected(self):
        with self.assertRaises(mod.RouteRejected):
            mod.validate_envelope(
                envelope("cdc.pdm_mdm.mdm.sacco_master_sources", b'')
            )

    def test_false_null_indicator_cannot_claim_tombstone(self):
        e = envelope("cdc.pdm_mdm.mdm.sacco_master_sources", None)
        e["value_is_null"] = False
        with self.assertRaises(mod.RouteRejected):
            mod.validate_envelope(e)

    def test_true_null_indicator_cannot_carry_payload(self):
        e = envelope("cdc.pdm_mdm.mdm.sacco_master_sources", b'{}')
        e["value_is_null"] = True
        with self.assertRaises(mod.RouteRejected):
            mod.validate_envelope(e)

    def test_non_cdc_null_rejected(self):
        with self.assertRaises(mod.RouteRejected):
            mod.validate_envelope(envelope("payment-events.retry", None))

    def test_avro_frame(self):
        raw = b'\x00' + (42).to_bytes(4, 'big') + b'payload'
        value = mod.validate_envelope(envelope("payments.lifecycle.state.v1", raw, 42))
        self.assertEqual(value["schema_registry_id"], 42)
    def test_reject_bad_hash(self):
        e = envelope("payment-events.dlq", b'{}')
        e["payload_sha256"] = "0" * 64
        with self.assertRaises(mod.RouteRejected):
            mod.validate_envelope(e)
    def test_reject_bad_avro(self):
        with self.assertRaises(mod.RouteRejected):
            mod.validate_envelope(envelope("payments.lifecycle.transitions.v1", b'{}', 42))
    def test_legacy_unchanged(self):
        self.assertEqual(sum(v["status"] == "legacy_payment_event" for v in mod.ROUTES.values()), 24)

if __name__ == "__main__":
    unittest.main()
