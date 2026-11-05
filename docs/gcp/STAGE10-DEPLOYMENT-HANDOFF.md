# Stage 10 deployment handoff. Not a deployment approval.

The local implementation includes a PostgreSQL-fenced reconciliation store, a read-only Dataflow provider, a scheduled reconciliation DAG, a BigQuery lineage table definition, and read-only evidence inspection. It **does not** yet contain a safely proven initial Dataflow submission path or a producer-side BigQuery lineage writer.

## Blocking gaps

1. Validate Dataflow submission semantics for the actual launch mode (Flex Template/classic template/Beam job), including uncertain response, provider-side identity, name reuse, retention and eventual visibility. Only then enable the durable initial submission state machine; no blind retries.
2. Instrument the actual Dataflow pipeline to write execution and attempt identifiers, job ID, spec digest and event-level output hashes. Independently reconcile expected input and output manifests, not merely existence of lineage rows.
3. Package the DAG and runtime in the chosen Composer image and verify Airflow SDK and provider versions, connection backend, Secret Manager access, and network reachability.
4. Validate IAM privileges, service accounts, budget alerts, quotas, and an external shutdown mechanism. GCP budget alerts are **not** hard spending caps.
5. Run disposable PostgreSQL concurrency/crash tests and Stage 8/9/10 regressions; then conduct approved, limited live GCP integration tests.

Do not mark `provider_duplicate_semantics`, `bigquery_evidence_integration`, `composer_automatic_scheduler` or `explicit_gcp_approval` as passed solely on the basis of this script.
