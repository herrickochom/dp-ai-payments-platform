import importlib.util
import json
import unittest
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
p = ROOT / "platform/gcp/kafka_pubsub_bridge/source_runtime_adapter_v1.py"
spec = importlib.util.spec_from_file_location("source_runtime_adapter_v1", p)
a = importlib.util.module_from_spec(spec)
spec.loader.exec_module(a)
class AdapterTests(unittest.TestCase):
    def test_json_immutable(self):
        e=a.make_envelope("payment-events.retry",0,5,1770000000000,b'{"reason":"test"}')
        key, raw=a.prepare_object(e)
        self.assertIn("offset=5/record.avro",key)
        from io import BytesIO
        from fastavro import reader
        import base64
        import hashlib

        self.assertTrue(raw.startswith(b"Obj\x01"))
        records = list(reader(BytesIO(raw)))
        self.assertEqual(len(records), 1)

        record = records[0]
        payload = base64.b64decode(e["payload_base64"])

        self.assertEqual(record["payload_bytes"], payload)
        self.assertEqual(
            record["payload_sha256"],
            hashlib.sha256(payload).hexdigest()
        )
        self.assertEqual(record["kafka_topic"], e["kafka_topic"])
        self.assertEqual(record["kafka_partition"], e["kafka_partition"])
        self.assertEqual(record["kafka_offset"], e["kafka_offset"])
        self.assertEqual(
            record["kafka_timestamp_ms"],
            e["kafka_timestamp_ms"]
        )
        self.assertEqual(record["payload_format"], "json")
        self.assertEqual(record["processing_mode"], "streaming")
        self.assertFalse(record["is_tombstone"])

        key2, raw2 = a.prepare_object(e)
        self.assertEqual((key, raw), (key2, raw2))
        self.assertEqual(a.prepare_object(e),(key,raw))
    def test_cdc_tombstone(self):
        e=a.make_envelope("cdc.pdm_mdm.mdm.sacco_master_sources",1,8,1770000000000,None)
        self.assertIn("record.avro",a.prepare_object(e)[0])
    def test_cdc_null_roundtrip(self):
        from io import BytesIO
        from fastavro import reader

        e = a.make_envelope(
            "cdc.pdm_mdm.mdm.sacco_master_sources",
            1, 8, 1770000000000, None
        )
        self.assertIs(e["value_is_null"], True)
        key, body = a.prepare_object(e)
        records = list(reader(BytesIO(body)))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["payload_bytes"], b"")
        self.assertIs(records[0]["value_is_null"], True)
        self.assertIs(records[0]["is_tombstone"], True)
        self.assertEqual(a.prepare_object(e), (key, body))

    def test_external_cdc_empty_nonnull_rejected(self):
        e = a.make_envelope(
            "cdc.pdm_mdm.mdm.sacco_master_sources",
            1, 8, 1770000000000, None
        )
        e["value_is_null"] = False
        with self.assertRaises(a.contract.RouteRejected):
            a.prepare_object(e)

    def test_reject_empty_nonnull_tombstone(self):
        with self.assertRaisesRegex(
            a.contract.RouteRejected,
            "empty non-null Kafka payload"
        ):
            a.make_envelope("cdc.pdm_mdm.mdm.sacco_master_sources",1,8,1770000000000,b'')
    def test_restricted(self):
        with self.assertRaises(a.contract.RouteRejected):
            a.make_envelope("mdm.crosswalk.restricted",0,0,1770000000000,b'{}')
    def test_unverified(self):
        with self.assertRaises(a.contract.RouteRejected):
            a.make_envelope("mdm.sacco.golden",0,0,1770000000000,b'{}')
    def test_legacy_unchanged(self):
        e=a.make_envelope("icmn.vpm.pain001",0,1,1770000000000,b'\x00\x00\x00\x00\x01x',1)
        with self.assertRaises(a.AdapterError):
            a.prepare_object(e)
    def test_avro_lifecycle(self):
        e=a.make_envelope("payments.lifecycle.state.v1",0,1,1770000000000,b'\x00\x00\x00\x00\x01x',1)
        self.assertIn("record.avro",a.prepare_object(e)[0])
    def test_digest(self):
        e=a.make_envelope("payment-events.dlq",0,2,1770000000000,b'{}')
        e["payload_sha256"]="0"*64
        with self.assertRaises(a.contract.RouteRejected):
            a.prepare_object(e)

    def test_governed_paths_contract_only_modes(self):
        cases = [
            (
                "cdc.pdm_mdm.mdm.sacco_master_sources",
                b'{"op":"c"}',
                "streaming",
                "mdm",
                "cdc",
                "pdm_mdm",
            ),
            (
                "payment-events.retry",
                b'{"reason":"retry"}',
                "streaming",
                "operations",
                "payment_events",
                "payment_events",
            ),
            (
                "payments.lifecycle.state.v1",
                b"\x00\x00\x00\x00\x01x",
                "real-time",
                "lifecycle",
                "payments",
                "flink",
            ),
        ]

        for topic, payload, mode, category, group, system in cases:
            with self.subTest(topic=topic):
                sid = 1 if topic.startswith("payments.lifecycle.") else None
                envelope = a.make_envelope(
                    topic, 2, 17, 1770000000000, payload, sid
                )
                key, body = a.prepare_object(envelope)
                expected = (
                    f"raw/v1/{mode}/"
                    f"category={category}/"
                    f"source_group={group}/"
                    f"source_system={system}/"
                )
                self.assertTrue(key.startswith(expected), key)
                self.assertIn(
                    f"topic={topic}/partition=2/offset=17/record.avro",
                    key,
                )
                self.assertTrue(body.startswith(b"Obj\x01"))
                self.assertEqual(a.prepare_object(envelope), (key, body))

    def test_missing_classification_fails_closed(self):
        topic = "payment-events.retry"
        envelope = a.make_envelope(
            topic, 0, 5, 1770000000000, b'{"reason":"test"}'
        )
        original = a.contract.ROUTES[topic]
        try:
            modified = dict(original)
            modified.pop("source_group")
            a.contract.ROUTES[topic] = modified
            with self.assertRaisesRegex(
                a.AdapterError, "missing governed source_group"
            ):
                a.immutable_key(envelope)
        finally:
            a.contract.ROUTES[topic] = original

    def test_invalid_classification_fails_closed(self):
        topic = "payment-events.dlq"
        envelope = a.make_envelope(
            topic, 0, 5, 1770000000000, b'{}'
        )
        original = a.contract.ROUTES[topic]
        try:
            modified = dict(original)
            modified["source_system"] = "../unsafe"
            a.contract.ROUTES[topic] = modified
            with self.assertRaisesRegex(
                a.AdapterError, "invalid governed source_system"
            ):
                a.immutable_key(envelope)
        finally:
            a.contract.ROUTES[topic] = original

    def test_cdc_tombstone_governed_path(self):
        envelope = a.make_envelope(
            "cdc.pdm_mdm.mdm.sacco_master_sources",
            1, 8, 1770000000000, None
        )
        key, body = a.prepare_object(envelope)
        self.assertIn(
            "category=mdm/source_group=cdc/source_system=pdm_mdm/",
            key,
        )
        from io import BytesIO
        from fastavro import reader
        record = list(reader(BytesIO(body)))[0]
        self.assertTrue(record["value_is_null"])
        self.assertTrue(record["is_tombstone"])

if __name__=="__main__": unittest.main()
