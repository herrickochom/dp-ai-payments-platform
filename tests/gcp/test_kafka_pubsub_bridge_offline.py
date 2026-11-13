import importlib.util
import json
import pathlib
import unittest
from unittest.mock import Mock

ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE = ROOT / 'platform/gcp/kafka_pubsub_bridge/bridge.py'
spec = importlib.util.spec_from_file_location('kp001d_bridge', MODULE)
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)

EVENT = dict(event_id='evt-1', payment_id='p-1', event_type='created', amount_minor=150,
             currency='GBP', event_timestamp='2026-10-30T10:00:00Z')

class BridgeOfflineTests(unittest.TestCase):
    def test_approved_event(self):
        self.assertEqual(json.loads(bridge.encode_event(EVENT)), EVENT)

    def test_missing_field_fails_closed(self):
        with self.assertRaises(bridge.ContractError):
            bridge.validate_event({k: v for k, v in EVENT.items() if k != 'payment_id'})

    def test_technical_event_extra_field_fails_closed(self):
        with self.assertRaises(bridge.ContractError):
            bridge.validate_event({**EVENT, 'correlation_id': 'c-1'})

    def test_bool_not_accepted_as_integer(self):
        with self.assertRaises(bridge.ContractError):
            bridge.validate_event({**EVENT, 'amount_minor': True})

    def test_out_of_range_integer(self):
        with self.assertRaises(bridge.ContractError):
            bridge.validate_event({**EVENT, 'amount_minor': 2**63})

    def test_empty_source_system_rejected(self):
        msg = Mock()
        with self.assertRaises(bridge.ContractError):
            bridge.provenance(msg, '')

    def test_publish_before_commit(self):
        msg = Mock()
        msg.topic.return_value = 'approved.topic'
        msg.partition.return_value = 2
        msg.offset.return_value = 19
        consumer, publisher, future = Mock(), Mock(), Mock()
        publisher.publish.return_value = future
        future.result.return_value = 'pubsub-id'
        actions = []
        future.result.side_effect = lambda **kw: actions.append('ack') or 'pubsub-id'
        consumer.commit.side_effect = lambda **kw: actions.append('commit')
        result = bridge.publish_then_commit(consumer, publisher, 'topic-path', msg, EVENT, 'approved-source')
        self.assertEqual(result, 'pubsub-id')
        self.assertEqual(actions, ['ack', 'commit'])
        self.assertEqual(publisher.publish.call_args.kwargs['kafka_offset'], '19')
        consumer.commit.assert_called_once_with(message=msg, asynchronous=False)

    def test_no_commit_on_publish_failure(self):
        msg, consumer, publisher = Mock(), Mock(), Mock()
        publisher.publish.return_value.result.side_effect = TimeoutError('ack timeout')
        with self.assertRaises(TimeoutError):
            bridge.publish_then_commit(consumer, publisher, 'topic-path', msg, EVENT, 'approved-source')
        consumer.commit.assert_not_called()

if __name__ == '__main__':
    unittest.main()
