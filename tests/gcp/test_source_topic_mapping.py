import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
P = ROOT / 'platform/gcp/kafka_pubsub_bridge/source_mapping.py'
spec = importlib.util.spec_from_file_location('kp001f_mapping', P)
mod = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)

class ContractMappingTests(unittest.TestCase):
    def test_source_schema_names_match_repository(self):
        for name, path in ((mod.BUSINESS_SOURCE,'contracts/kafka/payment_event.avsc'),
                           (mod.TECHNICAL_SOURCE,'contracts/kafka/technical_payment_event.avsc')):
            s=json.loads((ROOT/path).read_text())
            self.assertEqual(name, s['namespace']+'.'+s['name'])

    def test_target_fields_match_repository(self):
        s=json.loads((ROOT/'infrastructure/terraform/gcp/experiments/pubsub_payments/payment-event-v1.avsc').read_text())
        self.assertEqual(tuple(f['name'] for f in s['fields']), mod.TARGET_FIELDS)

    def test_business_source_blocked_even_with_nonnull_candidates(self):
        r={'event_id':'e1','event_type':'created','currency':'GBP','timestamp':'2026-10-30T00:00:00Z','instructed_amount':10.5}
        a=mod.assess(mod.BUSINESS_SOURCE,r)
        self.assertFalse(a.eligible)
        self.assertTrue(any('amount_minor' in b for b in a.blockers))
        with self.assertRaises(ValueError): mod.require_approved_mapping(mod.BUSINESS_SOURCE,r)

    def test_business_null_currency_blocked(self):
        a=mod.assess(mod.BUSINESS_SOURCE,{'event_id':'e1','event_type':'created','currency':None})
        self.assertTrue(any('currency' in b for b in a.blockers))

    def test_technical_source_blocked(self):
        a=mod.assess(mod.TECHNICAL_SOURCE,{'event_id':'e1','event_timestamp':'2026-10-30T00:00:00Z'})
        self.assertFalse(a.eligible)
        self.assertTrue(any('amount_minor' in b for b in a.blockers))

    def test_unknown_schema_blocked(self):
        a=mod.assess('unknown.Record',{})
        self.assertFalse(a.eligible)
        self.assertIn('unapproved',a.blockers[0])

    def test_non_record_rejected(self):
        with self.assertRaises(TypeError): mod.assess(mod.BUSINESS_SOURCE, [])

if __name__ == '__main__': unittest.main()
