# Azure Terraform Target

Classification: cloud deployment target.

Implementation follows the local reference implementation.

Provider-specific resources may be introduced only inside the Azure target
and governed reusable modules.

The Azure implementation must preserve the common platform architecture,
security boundaries, Raw-first ingestion model, CDC activation controls,
MDM privacy boundary and deployment quality gates.

Production use requires approved remote state, managed identity/RBAC,
Key Vault or approved equivalent, encryption, observability,
backup/recovery and protected deployment approval.
