import datetime as dt
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('kp014b_exporter', Path(__file__).with_name('exporter.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class ExportTests(unittest.TestCase):
    def test_schema(self):
        self.assertEqual(len(m.FIELDS), 24)
        self.assertEqual(len(set(m.FIELDS)), 24)
    def test_query(self):
        q = m.sql(10)
        self.assertIn('TO_JSON_STRING(payload_x_attributes)', q)
        self.assertIn('LIMIT 10', q)
        self.assertIn('ORDER BY `_kafka_topic`, `_kafka_partition`, `_kafka_offset`', q)
    def test_full_query(self):
        self.assertNotIn('LIMIT', m.sql(None))
    def test_normalize(self):
        r = {f: None for f in m.FIELDS}
        r['_kafka_timestamp'] = dt.datetime(2026, 11, 3, tzinfo=dt.timezone.utc)
        r['_bronze_loaded_at'] = r['_kafka_timestamp']
        r['payload_x_attributes'] = '{"x":1}'
        result = m.normalize(r)
        self.assertEqual(result['_kafka_timestamp'], '2026-11-03T00:00:00+00:00')
        self.assertEqual(result['payload_x_attributes'], '{"x":1}')
    def test_offline(self):
        self.assertEqual(m.main(['--limit', '2']), 0)
    def test_invalid_limit(self):
        with self.assertRaises(SystemExit):
            m.main(['--limit', '0'])

if __name__ == '__main__':
    unittest.main()
