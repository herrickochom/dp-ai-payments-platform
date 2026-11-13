import unittest
from pathlib import Path
import importlib.util

PATH = Path(__file__).resolve().parents[2] / 'platform/gcp/pubsub_raw_ingestion/bounded_subscriber.py'
spec = importlib.util.spec_from_file_location('kp001w_bounded_subscriber', PATH)
module = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = module
spec.loader.exec_module(module)


class Msg:
    def __init__(self, fail_ack=False):
        self.acks = 0
        self.fail_ack = fail_ack
    def ack(self):
        self.acks += 1
        if self.fail_ack:
            raise RuntimeError('ack failed')


class BoundedSubscriberTests(unittest.TestCase):
    def test_success_order(self):
        m = Msg()
        events = []
        result = module.process_one(m, lambda _: events.append('durable') or True,
                                    lambda _: events.append('lease'))
        self.assertEqual(events, ['lease', 'durable'])
        self.assertTrue(result.acknowledged)
        self.assertEqual(m.acks, 1)
    def test_false_durable_result_no_ack(self):
        m = Msg()
        self.assertFalse(module.process_one(m, lambda _: False, lambda _: None).acknowledged)
        self.assertEqual(m.acks, 0)
    def test_none_durable_result_no_ack(self):
        m = Msg()
        self.assertFalse(module.process_one(m, lambda _: None, lambda _: None).acknowledged)
        self.assertEqual(m.acks, 0)
    def test_processing_exception_no_ack(self):
        m = Msg()
        def fail(_): raise RuntimeError('storage unavailable')
        self.assertEqual(module.process_one(m, fail, lambda _: None).reason, 'processing_failed')
        self.assertEqual(m.acks, 0)
    def test_lease_failure_no_processing_or_ack(self):
        m = Msg()
        invoked = []
        def fail(_): raise RuntimeError('lease unavailable')
        result = module.process_one(m, lambda _: invoked.append(True) or True, fail)
        self.assertEqual(result.reason, 'lease_extension_failed')
        self.assertEqual(invoked, [])
        self.assertEqual(m.acks, 0)
    def test_ack_failure_redelivery_possible(self):
        m = Msg(fail_ack=True)
        result = module.process_one(m, lambda _: True, lambda _: None)
        self.assertEqual(result.reason, 'ack_failed_redelivery_possible')
        self.assertFalse(result.acknowledged)
