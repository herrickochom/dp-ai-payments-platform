import io
import tempfile
import unittest
from pathlib import Path
from fastavro import writer, parse_schema
from pipelines.gcp.raw_to_bronze.offline_workflow import (
    prepare_objects, execute_prefetched_offline)
from pipelines.gcp.raw_to_bronze.durable_execution_adapter import DurableLedger, ContractError

ST="sample-project.bronze.payments_raw_v1_staging"
BR="sample-project.bronze.payments_raw_v1"
SCHEMA=parse_schema({"type":"record","name":"Event","fields":[
 {"name":"event_id","type":"string"},
 {"name":"timestamp","type":"string"},
 {"name":"source_system","type":"string"},
 {"name":"message_type","type":"string"},
 {"name":"payload","type":{"type":"map","values":"string"}},
 {"name":"_kafka_metadata","type":{"type":"map","values":"string"}}]})
def ocf(*, offset="1", event="evt-1", valid=True):
    record={"event_id":event,"timestamp":"2026-11-03T00:00:00Z",
       "source_system":"synthetic","message_type":"payment",
       "payload":{"currency":"GBP"},
       "_kafka_metadata":{"topic":"synthetic.topic","partition":"0",
                          "offset":offset,"timestamp":"2026-11-03T00:00:00Z"}}
    if not valid: record["event_id"]=""
    buf=io.BytesIO(); writer(buf,SCHEMA,[record]); return buf.getvalue()

class FakeSink:
    def __init__(self):
        self.bronze={}; self.staged=[]; self.calls=[]
    def count_bronze(self): self.calls.append("before_or_after"); return len(self.bronze)
    def stage(self,run_id,rows): self.calls.append("stage"); self.staged=list(rows)
    def count_staging(self,run_id): self.calls.append("count_staging"); return len(self.staged)
    def merge(self,sql):
        self.calls.append("merge")
        for row in self.staged:
            key=(row["_kafka_topic"],row["_kafka_partition"],row["_kafka_offset"])
            self.bronze.setdefault(key,row)

class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.ledger=DurableLedger(Path(self.temp.name)/"ledger.sqlite")
        self.sink=FakeSink()
    def tearDown(self):
        self.ledger.close(); self.temp.cleanup()
    def execute(self, objects, run="run1"):
        return execute_prefetched_offline(objects=objects,ledger=self.ledger,
          fake_client=self.sink,run_id=run,staging_table=ST,bronze_table=BR)
    def test_avro_to_bronze_complete(self):
        result=self.execute([("raw/v1/a.avro",ocf())])
        self.assertEqual(result["status"],"COMPLETE")
        self.assertEqual(result["reconciliation"]["inserted_records"],1)
        self.assertIn("MERGE",result["merge_sql"])
    def test_retry_persistent_ledger(self):
        objects=[("raw/v1/a.avro",ocf())]
        self.execute(objects)
        calls=len(self.sink.calls)
        self.assertEqual(self.execute(objects)["status"],"ALREADY_COMPLETE")
        self.assertEqual(len(self.sink.calls),calls)
    def test_corrupt_avro_quarantine(self):
        p=prepare_objects([("raw/v1/bad.avro",b"not-avro"),
          ("raw/v1/good.avro",ocf())])
        self.assertEqual((p["input_records"],len(p["quarantine"]),len(p["staging_rows"])),(2,1,1))
    def test_invalid_record_quarantine(self):
        p=prepare_objects([("raw/v1/invalid.avro",ocf(valid=False)),
          ("raw/v1/good.avro",ocf())])
        self.assertEqual(len(p["quarantine"]),1)
    def test_identical_duplicate_conflicts_on_lineage(self):
        with self.assertRaises(ContractError):
            prepare_objects([("raw/v1/a.avro",ocf()),
                             ("raw/v1/b.avro",ocf())])
    def test_distinct_events(self):
        result=self.execute([("raw/v1/a.avro",ocf()),
                             ("raw/v1/b.avro",ocf(offset="2",event="evt-2"))])
        self.assertEqual(result["reconciliation"]["inserted_records"],2)
    def test_bad_object_fails_closed(self):
        with self.assertRaises(ContractError):
            prepare_objects([("raw/v1/a.avro","not bytes")])
    def test_empty_fails_closed(self):
        with self.assertRaises(ContractError): prepare_objects([])
    def test_ambiguous_merge_blocks_retry(self):
        self.sink.merge=lambda sql: (_ for _ in ()).throw(RuntimeError("unknown job result"))
        objects=[("raw/v1/a.avro",ocf())]
        with self.assertRaises(RuntimeError): self.execute(objects)
        with self.assertRaises(ContractError): self.execute(objects)
