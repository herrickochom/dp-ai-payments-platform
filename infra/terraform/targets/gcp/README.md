# GCP Terraform Target

Classification: cloud deployment target.

Implementation follows the local reference implementation.

Provider-specific resources may be introduced only inside the GCP target and
governed reusable modules.

Selection of GCP does not by itself replace Kafka with Pub/Sub, Iceberg with
BigQuery, or other reference components. Such substitutions require a
separate architecture decision and conformance evidence.

Production use requires approved remote state, IAM, KMS/secrets, encryption,
observability, backup/recovery and protected deployment approval.
