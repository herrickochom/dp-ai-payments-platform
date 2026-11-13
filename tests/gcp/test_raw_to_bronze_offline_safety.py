import ast
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = ROOT / "pipelines/gcp/raw_to_bronze/pipeline.py"

class Stage14EOfflineSafetyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = PIPELINE.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source)

    def test_live_fails_closed(self):
        self.assertIn('live mode BLOCKED: Stage 14E authorises offline tests only', self.source)

    def test_offline_requires_local_input(self):
        self.assertIn('known.raw_pattern.startswith("gs://")', self.source)
        self.assertIn('known.output_jsonl.startswith("gs://")', self.source)

    def test_offline_rejects_bq_targets(self):
        self.assertIn('if known.bronze_table or known.staging_table or known.quarantine_table:', self.source)

    def test_staging_not_canonical_target(self):
        self.assertIn('table=known.staging_table', self.source)
        self.assertNotIn('table=known.bronze_table', self.source)

    def test_kafka_identity_guards(self):
        self.assertIn('partition < 0 or offset < 0', self.source)
        self.assertIn('invalid_kafka_metadata_type', self.source)

    def test_schemas_unchanged_shape(self):
        folder = PIPELINE.parent
        bronze = json.loads((folder / 'bronze_schema.json').read_text())
        quarantine = json.loads((folder / 'quarantine_schema.json').read_text())
        self.assertEqual(len(bronze['fields']), 24)
        self.assertEqual(len(quarantine['fields']), 6)

if __name__ == '__main__':
    unittest.main()
