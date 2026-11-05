# KP-014 offline Bronze-to-Iceberg staging contract

This is a **local staging and validation writer**, not a BigLake/Iceberg REST commit client.
It takes a locally exported JSONL file with the 24 verified BigQuery Bronze columns,
checks required values and types, converts timestamps to UTC, canonicalizes JSON,
and rejects duplicate Kafka identities **within one input file**.

Usage:

```bash
python platform/gcp/iceberg_writer/writer.py \
  --input /path/to/bronze_export.jsonl --output /path/to/new-output-directory
```

Use `--parquet` only if `pyarrow` is already installed. Outputs are local only.
Exit 3 means quarantine contains rejected records. The original input is never modified.

## Explicitly NOT implemented
- BigQuery export or source snapshot consistency
- Iceberg REST catalog creation, authentication, transaction commit, metadata snapshots
- Cross-run deduplication and concurrent writer coordination
- Cloud object upload, BigLake IAM, GCP cost or API enablement
- Full production audit logging, replay orchestration, deletion, upserts, schema evolution

**Do not upload staging Parquet as though it were a committed Iceberg table.**
Before cloud execution, approve writer runtime and credentials, implement Iceberg commit
and replay safety, and reconcile all 20,403 source records against committed table snapshots.
