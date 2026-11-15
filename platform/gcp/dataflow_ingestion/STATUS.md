# dataflow-raw-ingestion implementation status
Implemented: Pub/Sub -> Dataflow -> immutable GCS Raw v1, reusing exact Raw v1 contract.
NOT IMPLEMENTED: authoritative source -> Kafka publisher for all datasets; full historical candidate reconciliation; durable Dataflow restart coordination.
NOT VERIFIED: Dataflow execution, 24-dataset delivery, historical records, IAM access, dependency packaging on workers.
Existing 907 objects were not touched by this installation.
Do not report dataflow-raw-ingestion as complete. Live launch requires an approved deployment plan and verification of the source and history ledger.
