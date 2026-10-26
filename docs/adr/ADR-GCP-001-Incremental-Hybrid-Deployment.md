# ADR-GCP-001: Incremental Hybrid GCP Reference Deployment.

**Status:** Proposed.
**Classification:** Child Architecture Decision Record.
**Parent:** ADR-000.
**Date:** 26 October 2026.
**Scope:** Google Cloud reference deployment.

## 1. Context.

The Payments Data AI Platform currently uses an open-source
reference implementation developed locally.

Google Cloud will be evaluated as an additional deployment
environment without disrupting existing platform capabilities
or replacing established technologies by default.

The initial GCP environment is intended for controlled
architecture validation, integration testing and learning,
rather than continuous production hosting.

## 2. Decision.

Adopt an incremental hybrid deployment strategy.

The existing local platform remains the operational
reference implementation.

GCP infrastructure shall be introduced progressively
through Terraform, with each deployment subject to
security, interoperability, operational and cost review.

Cloud-native technology substitutions require separate
architectural decisions and conformance evidence.

## 3. Deployment Boundaries.

The initial deployment shall:

1. Use the existing GCP development project.
2. Prefer the europe-west2 region.
3. Preserve existing data and integration contracts.
4. Separate local platform operations from GCP
   infrastructure experiments.
5. Avoid continuously running cloud workloads unless
   explicitly justified and approved.
6. Support repeatable deployment and teardown.
7. Avoid exposing local services directly to the
   public internet.

Full cloud deployment remains a future option,
subject to further architectural evaluation.

## 4. Infrastructure and Security.

Terraform shall manage approved GCP infrastructure.

Infrastructure design shall address:

- Least-privilege IAM and service identities.
- Network isolation and controlled connectivity.
- Encryption, secrets and key management.
- Storage lifecycle and data protection.
- Approved Terraform state management.
- Resource naming, ownership and labelling.
- Auditability and infrastructure change control.

Local Terraform state may be used during initial
configuration-only validation.

Persistent cloud infrastructure requires an approved
state strategy before deployment.

## 5. Platform Integration.

Existing technologies, including Kafka, Flink,
Iceberg, Nessie, Trino, dbt and Airflow, remain
the reference implementation.

GCP services may provide supporting infrastructure
where interoperability and cost are acceptable.

Any managed-service replacement must demonstrate
equivalent functional, governance, security and
operational capabilities.

## 6. Operations and Cost Controls.

The development environment shall:

1. Minimise chargeable infrastructure.
2. Review estimated costs before deployment.
3. Use budget alerts and usage monitoring.
4. Apply resource lifecycle controls.
5. Automate routine operational recovery.
6. Maintain observable deployment and runtime states.
7. Require explicit approval before introducing
   persistent or continuously billed resources.

Budget alerts are notifications, not spending caps.

The current development budget alert is £20 monthly.
It does not authorise expenditure up to that amount.

## 7. Consequences.

Advantages:

- Lower initial cloud expenditure.
- Reduced disruption to existing development.
- Incremental validation of cloud capabilities.
- Preservation of deployment portability.
- Clearer evidence for future technology decisions.

Trade-offs:

- Hybrid connectivity introduces integration complexity.
- Local infrastructure remains a development dependency.
- Initial tests do not prove full-cloud scalability.
- Some operational characteristics require later
  cloud-hosted validation.

## 8. Deferred Decisions.

Separate decisions shall address:

- GCP networking and private connectivity.
- Compute and workload deployment topology.
- Lakehouse object storage integration.
- IAM, secrets and key management implementation.
- Terraform remote state and deployment identity.
- Backup, recovery and disaster recovery.
- Managed-service substitutions, where justified.

Digital Assets and Smart Ledgers are outside
the scope of this GCP deployment decision.

## 9. Validation Criteria.

This decision is considered validated when:

1. Terraform configuration passes validation.
2. Approved resources can be deployed reproducibly.
3. Hybrid integration contracts are demonstrated.
4. Security and governance controls are verified.
5. Resource costs are measured and reviewed.
6. Teardown and recovery procedures are tested.
7. Existing platform functionality remains intact.

Production readiness requires separate approval.
