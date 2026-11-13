"""GCP-KP-011: Raw v1 -> BigQuery Bronze Dataflow pipeline.

Reads Avro OCF objects from gs://<bucket>/raw/v1/**/record.avro,
validates schema and lineage, deduplicates by (topic, partition, offset),
has a staging sink placeholder; live staging/MERGE is not yet enabled.

Quarantine: invalid records go to a quarantine table.

Idempotency: in-run Kafka identity deduplication only. Durable staging/MERGE
and reconciliation must be implemented and approved before live operation.

Offline mode (default; DirectRunner, --output_jsonl) accepts local files only.
Live mode fails closed until separately approved and implemented.
"""
import argparse, json, sys
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

# ----------------------------------------------------------------------
# DoFns
# ----------------------------------------------------------------------
class ParseOcf(beam.DoFn):
    """Reads one Avro OCF file, yields its single record with source path attached.

    Uses fastavro (shipped with apache-beam[gcp]) instead of avro, because
    Dataflow worker images include fastavro but not the apache-avro package.
    """
    def process(self, readable_file):
        import io
        from fastavro import reader as fastavro_reader
        path = readable_file.metadata.path
        try:
            with readable_file.open() as f:
                raw = f.read()
            for rec in fastavro_reader(io.BytesIO(raw)):
                rec["_raw_object_path"] = path
                yield rec
        except Exception as exc:
            yield {
                "_parse_error": f"{type(exc).__name__}: {exc}",
                "_raw_object_path": path,
            }

class Validate(beam.DoFn):
    """Validates required fields; routes invalid records to quarantine."""
    REQUIRED = ("event_id", "timestamp", "source_system", "message_type", "_kafka_metadata")
    BQ_REQUIRED_IN_KAFKA = ("topic", "partition", "offset", "timestamp")

    def process(self, record):
        # Parse errors go straight to quarantine
        if "_parse_error" in record:
            yield beam.pvalue.TaggedOutput("quarantine", {
                "event_id": None,
                "source_path": record.get("_raw_object_path", "?"),
                "reason": f"parse: {record['_parse_error']}",
                "record_json": None,
                "_quarantined_at": datetime.now(timezone.utc).isoformat(),
                "_pipeline_version": "kp011-v1",
            })
            return

        errors = []
        for f in self.REQUIRED:
            if not record.get(f):
                errors.append(f"missing_{f}")
        km = record.get("_kafka_metadata") or {}
        if not isinstance(km, dict):
            errors.append("invalid_kafka_metadata_type")
            km = {}
        for f in self.BQ_REQUIRED_IN_KAFKA:
            if km.get(f) in (None, ""):
                errors.append(f"missing_kafka_{f}")
        if not record.get("_raw_object_path"):
            errors.append("missing_raw_object_path")

        if errors:
            yield beam.pvalue.TaggedOutput("quarantine", {
                "event_id": record.get("event_id"),
                "source_path": record.get("_raw_object_path", "?"),
                "reason": "; ".join(errors),
                "record_json": json.dumps(record, default=str)[:9000],
                "_quarantined_at": datetime.now(timezone.utc).isoformat(),
                "_pipeline_version": "kp011-v1",
            })
            return

        try:
            partition = int(km["partition"])
            offset = int(km["offset"])
            if isinstance(km["partition"], bool) or isinstance(km["offset"], bool) or partition < 0 or offset < 0:
                raise ValueError("negative or boolean Kafka identity")
            if not isinstance(km["topic"], str) or not km["topic"].strip():
                raise ValueError("invalid Kafka topic")
            payload = record.get("payload") or {}
            if not isinstance(payload, dict):
                raise ValueError("invalid payload type")
        except (TypeError, ValueError, KeyError) as exc:
            yield beam.pvalue.TaggedOutput("quarantine", {
                "event_id": str(record.get("event_id")) if record.get("event_id") is not None else None,
                "source_path": str(record.get("_raw_object_path") or "?"),
                "reason": f"invalid_record: {exc}",
                "record_json": json.dumps(record, default=str)[:9000],
                "_quarantined_at": datetime.now(timezone.utc).isoformat(),
                "_pipeline_version": "kp011-v1",
            })
            return
        yield {
            "event_id": record["event_id"],
            "message_id": record.get("message_id"),
            "event_timestamp": record["timestamp"],
            "source_system": record["source_system"],
            "message_type": record["message_type"],
            "event_family": record.get("event_family"),
            "payload_amount": payload.get("amount"),
            "payload_currency": payload.get("currency"),
            "payload_debtor": payload.get("debtor"),
            "payload_creditor": payload.get("creditor"),
            "payload_status": payload.get("status"),
            "payload_x_attributes": json.dumps(payload.get("x_attributes") or {}),
            "event_data": record.get("event_data"),
            "parsed_event_data": record.get("parsed_event_data"),
            "_kafka_topic": km["topic"],
            "_kafka_partition": partition,
            "_kafka_offset": offset,
            "_kafka_timestamp": km["timestamp"],
            "_kafka_category": km.get("category"),
            "_kafka_source_group": km.get("source_group"),
            "_kafka_source_system": km.get("source_system"),
            "_raw_object_path": record["_raw_object_path"],
            "_bronze_pipeline_version": "kp011-v1",
            "_bronze_loaded_at": datetime.now(timezone.utc).isoformat(),
        }

# ----------------------------------------------------------------------
# Pipeline
# ----------------------------------------------------------------------
def run(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--raw_pattern", required=True,
                    help="glob, e.g. gs://bucket/raw/v1/**/record.avro")
    ap.add_argument("--bronze_table", default=None,
                    help="project:dataset.table (live only)")
    ap.add_argument("--quarantine_table", default=None,
                    help="project:dataset.table (live only)")
    ap.add_argument("--output_jsonl", default=None,
                    help="offline DirectRunner output path")
    ap.add_argument("--runner", default="DirectRunner")
    ap.add_argument("--execution_mode", choices=("offline", "live"), default="offline")
    ap.add_argument("--allow_gcp_operations", action="store_true")
    ap.add_argument("--staging_table", default=None, help="isolated BigQuery staging table, live only")
    ap.add_argument("--region", default="europe-west2")
    ap.add_argument("--temp_location", default=None)
    ap.add_argument("--staging_location", default=None)
    known, pipeline_args = ap.parse_known_args(argv)
    if known.execution_mode == "offline":
        if known.allow_gcp_operations or known.runner != "DirectRunner" or not known.output_jsonl:
            ap.error("offline requires DirectRunner and --output_jsonl; GCP operations forbidden")
        if known.raw_pattern.startswith("gs://") or known.output_jsonl.startswith("gs://"):
            ap.error("offline mode requires local input and output paths")
        if known.bronze_table or known.staging_table or known.quarantine_table:
            ap.error("offline mode forbids BigQuery table arguments")
        if any(x.startswith(("--project=", "--temp_location=gs://", "--staging_location=gs://")) for x in pipeline_args):
            ap.error("unexpected cloud pipeline option")
    else:
        # Live mode is intentionally disabled pending separate cloud approval,
        # audited staging lifecycle, merge orchestration, and reconciliation.
        ap.error("live mode BLOCKED: Stage 14E authorises offline tests only")

    options = PipelineOptions(
        pipeline_args,
        project=known.project,
        region=known.region,
        runner=known.runner,
        temp_location=known.temp_location,
        staging_location=known.staging_location,
    )

    schema_path = __file__.rsplit("/", 1)[0] + "/bronze_schema.json"
    with open(schema_path) as f:
        bronze_schema = json.load(f)
    q_schema_path = __file__.rsplit("/", 1)[0] + "/quarantine_schema.json"
    with open(q_schema_path) as f:
        quarantine_schema = json.load(f)

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "Match" >> MatchFiles(known.raw_pattern)
            | "Read"  >> ReadMatches()
            | "Parse" >> beam.ParDo(ParseOcf())
        )

        valid, quarantine = (
            parsed
            | "Validate" >> beam.ParDo(Validate()).with_outputs("quarantine", main="valid")
        )

        # Deduplicate by Kafka identity (topic, partition, offset).
        # Same identity can only occur if a retry produced a duplicate; taking
        # the first is deterministic.
        deduped = (
            valid
            | "KeyByKafkaId" >> beam.Map(
                lambda r: ((r["_kafka_topic"], r["_kafka_partition"], r["_kafka_offset"]), r)
            )
            | "GroupByKafkaId" >> beam.GroupByKey()
            | "TakeFirst" >> beam.Map(lambda kv: sorted(kv[1], key=lambda r: r["_raw_object_path"])[0])
        )

        if known.execution_mode == "offline":
            (deduped
             | "ToJson" >> beam.Map(lambda r: json.dumps(r, default=str))
             | "WriteJsonl" >> beam.io.WriteToText(
                 known.output_jsonl, num_shards=1, file_name_suffix=".jsonl"))
            (quarantine
             | "QToJson" >> beam.Map(lambda r: json.dumps(r, default=str))
             | "WriteQJsonl" >> beam.io.WriteToText(
                 known.output_jsonl + "_quarantine", num_shards=1, file_name_suffix=".jsonl"))
        else:
            (deduped | "WriteBronzeStaging" >> WriteToBigQuery(
                table=known.staging_table,
                schema=bronze_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            ))
            (quarantine | "WriteQuarantine" >> WriteToBigQuery(
                table=known.quarantine_table,
                schema=quarantine_schema,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                method="STREAMING_INSERTS",
            ))

if __name__ == "__main__":
    run(sys.argv[1:])
