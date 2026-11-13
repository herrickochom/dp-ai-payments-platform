"""Stage 14E.3: synthetic-only, local Avro -> Bronze validation tests."""
import io
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from fastavro import writer

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = ROOT / "pipelines/gcp/raw_to_bronze/pipeline.py"
spec = importlib.util.spec_from_file_location("stage14e3_pipeline", PIPELINE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

SCHEMA = {
    "type": "record", "name": "SyntheticRawEvent", "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "source_system", "type": "string"},
        {"name": "message_type", "type": "string"},
        {"name": "_kafka_metadata", "type": {"type": "record", "name": "KafkaMeta", "fields": [
            {"name": "topic", "type": "string"},
            {"name": "partition", "type": "long"},
            {"name": "offset", "type": "long"},
            {"name": "timestamp", "type": "string"}
        ]}}
    ]
}

def event(offset=0, event_id="evt-1"):
    return {"event_id": event_id, "timestamp": "2026-11-06T00:00:00Z",
            "source_system": "synthetic", "message_type": "synthetic.event.v1",
            "_kafka_metadata": {"topic": "synthetic.topic", "partition": 0,
                                "offset": offset, "timestamp": "2026-11-06T00:00:00Z"}}

class FakeReadable:
    def __init__(self, data, path):
        self.data = data
        self.metadata = type("Metadata", (), {"path": path})()
    def open(self):
        return io.BytesIO(self.data)

def ocf(records):
    buf = io.BytesIO()
    writer(buf, SCHEMA, records)
    return buf.getvalue()

class Stage14E3AvroTests(unittest.TestCase):
    def test_avro_ocf_decodes_and_tracks_path(self):
        path = "/tmp/synthetic/raw/v1/record.avro"
        records = list(module.ParseOcf().process(FakeReadable(ocf([event()]), path)))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["_raw_object_path"], path)
        self.assertEqual(records[0]["_kafka_metadata"]["offset"], 0)

    def test_corrupt_avro_routes_to_quarantine(self):
        records = list(module.ParseOcf().process(FakeReadable(b"not-avro", "/tmp/bad.avro")))
        self.assertEqual(len(records), 1)
        self.assertIn("_parse_error", records[0])
        outputs = list(module.Validate().process(records[0]))
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0].tag, "quarantine")
        self.assertIn("parse:", outputs[0].value["reason"])

    def test_valid_event_maps_to_bronze_contract(self):
        raw = list(module.ParseOcf().process(FakeReadable(ocf([event()]), "/tmp/record.avro")))[0]
        outputs = list(module.Validate().process(raw))
        self.assertEqual(len(outputs), 1)
        self.assertFalse(isinstance(outputs[0], beam.pvalue.TaggedOutput))
        bronze = outputs[0]
        schema = json.loads((PIPELINE.parent / "bronze_schema.json").read_text())
        required = {f["name"] for f in schema["fields"] if f.get("mode") == "REQUIRED"}
        self.assertTrue(required <= bronze.keys(), required - bronze.keys())
        self.assertEqual(bronze["_kafka_offset"], 0)
        self.assertEqual(bronze["_kafka_topic"], "synthetic.topic")

    def test_missing_metadata_goes_to_quarantine(self):
        bad = event()
        bad["_kafka_metadata"]["topic"] = ""
        bad["_raw_object_path"] = "/tmp/record.avro"
        outputs = list(module.Validate().process(bad))
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0].tag, "quarantine")
        self.assertIn("missing_kafka_topic", outputs[0].value["reason"])

    def test_duplicate_kafka_identity_is_deduplicated_in_beam(self):
        rows = []
        for path in ("/tmp/a.avro", "/tmp/b.avro"):
            raw = list(module.ParseOcf().process(FakeReadable(ocf([event()]), path)))[0]
            rows.extend(module.Validate().process(raw))
        with BeamTestPipeline() as p:
            deduped = (p | beam.Create(rows)
                | beam.Map(lambda r: ((r["_kafka_topic"], r["_kafka_partition"], r["_kafka_offset"]), r))
                | beam.GroupByKey()
                | beam.Map(lambda kv: sorted(kv[1], key=lambda r: r["_raw_object_path"])[0])
                | beam.Map(lambda r: r["_raw_object_path"]))
            assert_that(deduped, equal_to(["/tmp/a.avro"]))

    def test_synthetic_accounting_balances(self):
        good = [event(0), event(0), event(1, "evt-2")]
        for r in good:
            r["_raw_object_path"] = "/tmp/synthetic.avro"
        bad = event(2, "evt-3")
        bad["_raw_object_path"] = "/tmp/synthetic.avro"
        bad["_kafka_metadata"]["topic"] = ""
        outputs = [o for r in good + [bad] for o in module.Validate().process(r)]
        accepted = [o for o in outputs if not isinstance(o, beam.pvalue.TaggedOutput)]
        quarantined = [o for o in outputs if isinstance(o, beam.pvalue.TaggedOutput)]
        identities = {(r["_kafka_topic"], r["_kafka_partition"], r["_kafka_offset"]) for r in accepted}
        duplicate_count = len(accepted) - len(identities)
        self.assertEqual((len(outputs), len(accepted), len(quarantined), duplicate_count, len(identities)), (4, 3, 1, 1, 2))
        self.assertEqual(len(outputs), len(quarantined) + duplicate_count + len(identities))

if __name__ == "__main__":
    unittest.main()
