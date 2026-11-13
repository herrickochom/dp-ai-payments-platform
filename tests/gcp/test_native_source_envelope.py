import importlib.util
import io
import json
import pathlib
import struct
import unittest
from unittest.mock import Mock
from fastavro import parse_schema, schemaless_writer

ROOT = pathlib.Path(__file__).resolve().parents[2]
P = ROOT / 'platform/gcp/kafka_pubsub_bridge/native_envelope_runtime.py'
spec = importlib.util.spec_from_file_location('native_bridge', P)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


class NativeBridgeTests(unittest.TestCase):
    def setUp(self):
        self.schema = {'type': 'record', 'name': 'T', 'fields': [{'name': 'event_id', 'type': 'string'}]}
        stream = io.BytesIO()
        schemaless_writer(stream, parse_schema(self.schema), {'event_id': 'e1'})
        self.payload = b'\x00' + struct.pack('>I', 9) + stream.getvalue()
        self.msg = Mock()
        self.msg.value.return_value = self.payload
        self.msg.topic.return_value = 'approved.topic'
        self.msg.partition.return_value = 2
        self.msg.offset.return_value = 7
        self.schema_lookup = lambda _: json.dumps(self.schema)

    def test_success_publishes_before_commit(self):
        actions = []
        def pub(body, attrs):
            actions.append('publish_ack')
            env = json.loads(body)
            self.assertEqual(env['schema_registry_id'], 9)
            self.assertEqual(env['kafka_offset'], 7)
            self.assertEqual(attrs['source_partition'], '2')
        def commit(msg):
            actions.append('commit')
        mod.process_one(message=self.msg, get_schema=self.schema_lookup, publish=pub,
                        commit=commit, subject='approved.topic-value')
        self.assertEqual(actions, ['publish_ack', 'commit'])

    def test_failure_does_not_commit(self):
        commit = Mock()
        def fail(*_):
            raise TimeoutError('publish acknowledgement timed out')
        with self.assertRaises(TimeoutError):
            mod.process_one(message=self.msg, get_schema=self.schema_lookup,
                            publish=fail, commit=commit, subject='approved.topic-value')
        commit.assert_not_called()

    def test_schema_id_mismatch_rejected(self):
        with self.assertRaises(mod.BridgeError):
            mod.process_one(message=self.msg, get_schema=self.schema_lookup,
                            publish=Mock(), commit=Mock(), subject='approved.topic-value', expected_schema_id=10)

    def test_invalid_avro_rejected(self):
        self.msg.value.return_value = b'\x00' + struct.pack('>I', 9) + b'\xff'
        with self.assertRaises(mod.BridgeError):
            mod.process_one(message=self.msg, get_schema=self.schema_lookup,
                            publish=Mock(), commit=Mock(), subject='approved.topic-value')

    def test_invalid_header_rejected(self):
        with self.assertRaises(mod.BridgeError):
            mod.decode_header(b'bad')

    def test_no_business_field_inference(self):
        body = mod.envelope_bytes(payload=self.payload, topic='approved.topic', partition=2,
                                  offset=7, schema_id=9, schema_subject='approved.topic-value')
        obj = json.loads(body)
        self.assertNotIn('payment_id', obj)
        self.assertNotIn('amount_minor', obj)
        self.assertIn('payload_base64', obj)


if __name__ == '__main__':
    unittest.main()
