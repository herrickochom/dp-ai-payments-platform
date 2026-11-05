# KP-014D offline transaction admission simulator

This is **not an Apache Iceberg writer**, BigLake integration, snapshot creation, or a production distributed lock. It verifies the previously reconciled full export and Parquet, then optionally records source and Kafka/event identity reservations in a **local SQLite database**. A repeated source digest is idempotent in this one ledger; conflicting identities are rejected transactionally. PREPARED is not a cloud commit; never promote to committed without an actual catalogue commit protocol.

Offline verification (no ledger write):

`python platform/gcp/iceberg_commit_planner/planner.py`

Local admission simulation (creates SQLite ledger):

`python platform/gcp/iceberg_commit_planner/planner.py --record`

Do not use the ledger as proof of cross-process or distributed Iceberg idempotency. No GCP calls, IAM changes, Terraform applies, or Iceberg commits are performed.
