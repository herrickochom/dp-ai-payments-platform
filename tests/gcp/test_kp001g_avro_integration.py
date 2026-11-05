import importlib.util
import json
import pathlib
import unittest
from unittest.mock import Mock

ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE = ROOT / 'platform/gcp/kafka_pubsub_bridge/avro_integration.py'
spec = importlib.util.spec_from_file_location('kp001g_avro', MODULE)
avro = importlib.util.module_from_spec(spec)
spec.loader.exec_module(avro)
bridge_path = ROOT / 'platform/gcp/kafka_pubsub_bridge/bridge.py'
bridge_spec = importlib.util.spec_from_file_location('kp001d_bridge_g', bridge_path)
bridge = importlib.util.module_from_spec(bridge_spec)
bridge_spec.loader.exec_module(bridge)
BUSINESS = json.loads((ROOT/'contracts/kafka/payment_event.avsc').read_text())
TECHNICAL = json.loads((ROOT/'contracts/kafka/technical_payment_event.avsc').read_text())
TARGET = json.loads((ROOT/'infrastructure/terraform/gcp/experiments/pubsub_payments/payment-event-v1.avsc').read_text())

def record(schema):
    values={}
    for f in schema['fields']:
        typ=f['type']
        if isinstance(typ,list) and 'null' in typ:
            values[f['name']]=None
        elif typ=='string':
            values[f['name']]='test-value'
        elif typ in ('int','long'):
            values[f['name']]=1
        elif typ=='boolean':
            values[f['name']]=False
        elif typ=='double':
            values[f['name']]=1.0
        elif typ=='float':
            values[f['name']]=1.0
        else:
            raise AssertionError(f'Unsupported fixture field: {f}')
    return values

class RealAvroIntegration(unittest.TestCase):
    def test_business_binary_decodes_and_is_blocked(self):
        payload=avro.encode_wire(BUSINESS, record(BUSINESS), 101)
        sid,decoded,decision=avro.assess_wire(payload,{101:BUSINESS})
        self.assertEqual(sid,101)
        self.assertEqual(decoded['event_id'],'test-value')
        self.assertFalse(decision.eligible)
        self.assertTrue(any('payment_id' in x for x in decision.blockers))

    def test_technical_binary_decodes_and_is_blocked(self):
        payload=avro.encode_wire(TECHNICAL, record(TECHNICAL), 102)
        _,decoded,decision=avro.assess_wire(payload,{102:TECHNICAL})
        self.assertEqual(decoded['event_timestamp'],'test-value')
        self.assertFalse(decision.eligible)

    def test_unknown_schema_id_fails_closed(self):
        payload=avro.encode_wire(BUSINESS, record(BUSINESS), 103)
        with self.assertRaises(avro.WireContractError): avro.decode_wire(payload,{101:BUSINESS})

    def test_invalid_wire_header_fails_closed(self):
        with self.assertRaises(avro.WireContractError): avro.decode_wire(b'not-avro',{})

    def test_trailing_data_fails_closed(self):
        payload=avro.encode_wire(BUSINESS, record(BUSINESS), 101)
        with self.assertRaises(avro.WireContractError): avro.decode_wire(payload+b'garbage',{101:BUSINESS})

    def test_approved_target_wire_and_mock_publish_commit(self):
        original={'event_id':'e-1','payment_id':'p-1','event_type':'created','amount_minor':150,'currency':'GBP','event_timestamp':'2026-10-30T10:00:00Z'}
        payload=avro.encode_wire(TARGET,original,201)
        sid,name,decoded=avro.decode_wire(payload,{201:TARGET})
        self.assertEqual(sid,201)
        self.assertEqual(decoded,original)
        consumer,publisher,message=Mock(),Mock(),Mock()
        message.topic.return_value='isolated.approved.test'
        message.partition.return_value=0
        message.offset.return_value=5
        actions=[]
        publisher.publish.return_value.result.side_effect=lambda **kwargs: actions.append('ack') or 'mock-id'
        consumer.commit.side_effect=lambda **kwargs: actions.append('commit')
        bridge.publish_then_commit(consumer,publisher,'projects/test/topics/mock',message,decoded,'test-system')
        self.assertEqual(actions,['ack','commit'])
        self.assertEqual(publisher.publish.call_args.kwargs['kafka_offset'],'5')

    def test_failed_ack_never_commits(self):
        event={'event_id':'e','payment_id':'p','event_type':'created','amount_minor':1,'currency':'GBP','event_timestamp':'2026-10-30T10:00:00Z'}
        consumer,publisher,message=Mock(),Mock(),Mock()
        publisher.publish.return_value.result.side_effect=TimeoutError('simulated')
        with self.assertRaises(TimeoutError): bridge.publish_then_commit(consumer,publisher,'test',message,event,'test-system')
        consumer.commit.assert_not_called()

if __name__ == '__main__': unittest.main()
