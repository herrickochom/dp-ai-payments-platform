import sqlite3
import unittest
from planner import init_db, admit

class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        init_db(self.conn)
        self.rows = [{'event_id':'e1','_kafka_topic':'t','_kafka_partition':0,'_kafka_offset':1}]
    def tearDown(self): self.conn.close()
    def test_first_admission(self): self.assertEqual(admit(self.conn,'a',self.rows),'PREPARED_OFFLINE_ONLY')
    def test_retry_idempotent(self):
        admit(self.conn,'a',self.rows)
        self.assertEqual(admit(self.conn,'a',self.rows),'ALREADY_PREPARED')
    def test_identity_conflict_rolls_back(self):
        admit(self.conn,'a',self.rows)
        with self.assertRaises(sqlite3.IntegrityError): admit(self.conn,'b',self.rows)
        self.assertIsNone(self.conn.execute('SELECT * FROM runs WHERE source_sha=?',('b',)).fetchone())
    def test_event_id_conflict_rolls_back(self):
        admit(self.conn,'a',self.rows)
        other = [{'event_id':'e1','_kafka_topic':'t','_kafka_partition':0,'_kafka_offset':2}]
        with self.assertRaises(sqlite3.IntegrityError): admit(self.conn,'b',other)
        self.assertIsNone(self.conn.execute('SELECT * FROM identities WHERE offset=2').fetchone())
    def test_mismatched_retry_rejected(self):
        admit(self.conn,'a',self.rows)
        with self.assertRaises(AssertionError): admit(self.conn,'a',self.rows*2)

if __name__ == '__main__': unittest.main()
