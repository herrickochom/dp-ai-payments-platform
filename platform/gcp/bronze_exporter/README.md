# Guarded Bronze exporter

Offline default: `python platform/gcp/bronze_exporter/exporter.py --limit 10` prints SQL and makes no cloud calls.

Live requires BOTH `--live` and `KP014B_ALLOW_BQ_READ=YES`; it executes a BigQuery dry run then a bounded BigQuery read. **BigQuery queries may be billable.** The default maximum bytes billed is 100,000,000 bytes; this is not a currency ceiling or a guarantee of zero charges. The BigQuery query job is a single statement (consistent source snapshot for that statement), not a guarantee that later exports use the same snapshot. The local export does not commit Iceberg data.

Suggested first approved sample:

`KP014B_ALLOW_BQ_READ=YES python platform/gcp/bronze_exporter/exporter.py --live --limit 100 --expected-rows 100 --output runtime/iceberg-catalogue-validation/sample-export-100`

Then stage with existing writer:

`python platform/gcp/staging_bronze_writer/writer.py --input runtime/iceberg-catalogue-validation/sample-export-100/bronze.jsonl --output runtime/iceberg-catalogue-validation/sample-stage-100 --parquet`

Review the manifests and quarantine counts. Full export only after review, e.g. `--expected-rows 20403` and a new output directory. The default query cap may need adjustment after a dry run, with explicit approval. Do not rerun the installer: it is intentionally non-overwriting.
