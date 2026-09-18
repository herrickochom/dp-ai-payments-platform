# AWS Terraform Target

Classification: cloud deployment target.

Implementation follows successful local reference acceptance.

Provider-specific resources may be introduced only inside the AWS target and
governed reusable modules.

The AWS implementation must preserve the common platform architecture,
security boundaries, Raw-first ingestion model, CDC activation controls,
MDM privacy boundary and deployment quality gates.

Production use requires approved remote state, IAM, KMS/secrets, encryption,
observability, backup/recovery and protected deployment approval.
