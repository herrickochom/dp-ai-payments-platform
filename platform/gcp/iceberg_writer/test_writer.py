import datetime as dt
import unittest
from writer import FIELDS, convert, timestamp

class WriterTests(unittest.TestCase):
    def sample(self):
        data = {name: None for name,_,_ in FIELDS if name != 'parsed_event_timestamp'}
        data.update(event_id='e1', event_timestamp='2026-11-03T12:23:19Z', source_system='test', message_type='payment', _kafka_topic='test.topic', _kafka_partition=0, _kafka_offset=1, _kafka_timestamp='2026-11-03T12:23:19+00:00', _raw_object_path='gs://example/raw/v2/obj', _bronze_pipeline_version='v2', _bronze_loaded_at='2026-11-03T12:24:00Z')
        return data
    def test_25_fields(self):
        self.assertEqual(len(FIELDS),25)
        self.assertEqual(len(convert(self.sample())),25)
    def test_utc(self):
        self.assertEqual(timestamp('2026-11-03T14:23:19+02:00'),dt.datetime(2026,11,3,12,23,19,tzinfo=dt.timezone.utc))
    def test_no_timezone_rejected(self):
        with self.assertRaises(ValueError): convert({**self.sample(), 'event_timestamp':'2026-11-03T12:23:19'})
    def test_invalid_json_rejected(self):
        with self.assertRaises(ValueError): convert({**self.sample(), 'payload_x_attributes':'{bad'})
    def test_json_canonical(self):
        self.assertEqual(convert({**self.sample(), 'payload_x_attributes':{'b':2,'a':1}})['payload_x_attributes'],'{"a":1,"b":2}')
    def test_required_field(self):
        with self.assertRaises(ValueError): convert({**self.sample(), 'event_id':None})
    def test_integer_bool_rejected(self):
        with self.assertRaises(ValueError): convert({**self.sample(), '_kafka_offset':True})
    def test_unexpected_field(self):
        with self.assertRaises(ValueError): convert({**self.sample(), 'unverified':'x'})

if __name__ == '__main__': unittest.main(verbosity=2)
