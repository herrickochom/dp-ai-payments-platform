# Governed GCS Bucket Terraform Module.

## Purpose.

Reusable Google Cloud Storage bucket configuration for the
Payments Data AI Platform hybrid reference architecture.

## Security Baseline.

- Uniform bucket-level access is mandatory.
- Public access prevention is enforced.
- Terraform force destruction is disabled.
- Terraform resource destruction is protected.
- Object versioning is enabled by default.
- Resource labels support ownership and governance.

## Governance.

The module defaults to governance_approved = false.

This is a Terraform configuration gate, not an independent
authorisation system. A caller could change the input, so
deployment approval must also be enforced through the
deployment workflow and IAM permissions.

No lifecycle rules, automatic deletion, bucket retention
policies or retention locks are configured.

Dataset retention schedules remain proposed and must not
be translated into destructive lifecycle rules without
separate approval.

Object versioning can increase storage costs. Version
cleanup requires a separately approved policy.

## Deployment.

This module is not referenced by the development
environment. Creating these files provisions no resources.

Future integration requires:

1. Approved bucket topology and naming.
2. Approved Terraform state management.
3. Governance and security review.
4. Cost estimate and teardown strategy.
5. Explicit deployment authorisation.

## References.

- ADR-000: Enterprise Platform Architecture.
- ADR-GCP-001: Incremental Hybrid Deployment.
- ADR-GCP-002: Lakehouse Storage Interoperability.
- ADR-GCP-003: Governed Cloud Storage and Retention.
