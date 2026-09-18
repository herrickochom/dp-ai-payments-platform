# Terraform Backend Governance

Production Terraform state must use a remote backend selected as part of the
approved deployment target.

The backend must provide:

1. encryption at rest
2. encryption in transit
3. state locking or equivalent concurrency protection
4. versioning or recoverability
5. least privilege access
6. auditability
7. separation between environments

Local Terraform state is not an approved production backend.

No backend block is committed at this stage because the production
infrastructure provider has not yet been selected.

Backend configuration must not contain plaintext production secrets.

The CI/CD deployment path must initialise the approved backend only after
provider and environment governance has passed.
