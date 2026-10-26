# ADR-GCP-002: Hybrid Lakehouse Storage Interoperability.

<!-- architecture-navigation:start -->

[Architecture Overview](../architecture/gcp/README.md) · [Architecture Diagrams](../architecture/gcp/README.md#architecture-diagrams) · [Decision Records](../architecture/gcp/README.md#architecture-decision-records)

> **Status:** Proposed  
> **Deployment:** Design and validation only  
> **Cloud provisioning:** Not authorised

**Decision at a glance.** Phased validation of bounded object exchange followed by separate Apache Iceberg metadata, catalogue and query interoperability tests.

---

<!-- architecture-navigation:end -->

**Status:** Proposed.
**Classification:** Child Architecture Decision Record.
**Parent:** ADR-000.
**Related:** ADR-GCP-001.
**Date:** 26 October 2026.
**Scope:** MinIO, Iceberg, Nessie, Trino and GCP storage.

## 1. Context.

The Payments Data AI Platform uses an open-source
lakehouse architecture.

MinIO provides S3-compatible object storage.
Nessie exposes an Iceberg REST catalogue.
Trino accesses Iceberg tables through that catalogue.

The existing Nessie configuration defines a named
warehouse, payments, whose physical location is
provided through WAREHOUSE_URI.

Flink checkpoint and savepoint storage uses
the existing MinIO infrastructure.

The GCP reference deployment must preserve
these operational contracts.

## 2. Decision.

Adopt a phased, isolated approach to evaluating
Google Cloud Storage interoperability.

Phase 1 shall validate bounded object exchange
between the local development environment and
an isolated GCS test location.

Phase 2 may evaluate Iceberg table portability,
catalogue registration and query interoperability
after Phase 1 has been validated.

GCS shall not replace the existing MinIO warehouse
as part of the initial experiment.

## 3. Protected Boundaries.

The experiment shall not:

1. Modify existing Flink checkpoint locations.
2. Modify existing Flink savepoint locations.
3. Rewrite production-style Iceberg metadata.
4. Change existing Nessie references or branches.
5. Redirect the existing payments warehouse.
6. Change Kafka consumer offsets.
7. Expose local services publicly.
8. Require continuously running cloud compute.

Only explicitly approved, non-sensitive test
objects may be transferred.

## 4. Phase 1: Object Interoperability.

The initial test shall demonstrate:

1. Controlled access to an isolated GCS location.
2. Transfer of a bounded test object set.
3. Verification of object integrity.
4. Least-privilege authentication.
5. Recorded storage and request costs.
6. Repeatable cleanup and lifecycle controls.

Object transfer alone does not establish
Iceberg table portability.

## 5. Phase 2: Iceberg Interoperability.

A separate test shall evaluate:

1. Iceberg metadata and manifest references.
2. Physical object URI compatibility.
3. Nessie catalogue configuration.
4. Catalogue registration and isolation.
5. Trino authentication and object access.
6. Snapshot consistency and query correctness.
7. Failure recovery and rollback.

The existing payments warehouse shall remain
unchanged throughout the experiment.

## 6. Security.

Use dedicated test identities and least-privilege
access where feasible.

Do not introduce long-lived service account keys
as the default authentication mechanism.

Do not commit credentials, tokens, Terraform
state files or sensitive execution plans.

Review the effective Trino runtime configuration
before relying on catalogue authentication controls.

## 7. Infrastructure and Cost.

Approved GCP infrastructure shall be managed
through Terraform.

Before deployment, document:

- Required GCP APIs and IAM permissions.
- Storage location and naming conventions.
- Estimated storage and operation charges.
- Network transfer and retrieval charges.
- Resource lifecycle and cleanup arrangements.
- Terraform state and recovery controls.

The existing monthly budget alert is £20.
It is not a spending cap.

No GCP resources are authorised by this ADR.

## 8. Validation and Exit Criteria.

The experiment is successful when:

1. Approved test objects transfer correctly.
2. Object integrity is independently verified.
3. Access controls are validated.
4. Costs are recorded and reviewed.
5. Cleanup is demonstrated.
6. Existing local operations remain unaffected.
7. Test evidence is retained.

Iceberg interoperability requires separate
validation of catalogue and metadata semantics.

## 9. Consequences.

Advantages:

- Preserves the existing open-source platform.
- Limits initial cloud expenditure.
- Tests interoperability incrementally.
- Reduces operational disruption.
- Provides evidence for future cloud decisions.

Trade-offs:

- Hybrid integration adds operational complexity.
- Object interoperability does not prove full
  lakehouse portability.
- Iceberg catalogue integration requires
  additional testing.

## 10. Deferred Decisions.

The following remain subject to separate review:

- GCS bucket provisioning and IAM implementation.
- Terraform remote state.
- Private hybrid connectivity.
- Iceberg warehouse migration.
- Managed catalogue substitutions.
- Full-cloud deployment topology.

Production adoption requires separate approval.

## Architecture and Data-Flow Diagrams

These diagrams form part of the architecture decision and are displayed inline for review.

### Iceberg Lakehouse Interoperability.

![Iceberg Lakehouse Interoperability](../architecture/gcp/diagrams/iceberg-lakehouse.svg)

[Open full-size SVG](../architecture/gcp/diagrams/iceberg-lakehouse.svg) · [View editable Graphviz source](../architecture/gcp/diagrams/iceberg-lakehouse.dot)

### End-to-End Data Flow.

![End-to-End Data Flow](../architecture/gcp/diagrams/end-to-end-data-flow.svg)

[Open full-size SVG](../architecture/gcp/diagrams/end-to-end-data-flow.svg) · [View editable Graphviz source](../architecture/gcp/diagrams/end-to-end-data-flow.dot)

> **Architecture boundary:** Existing local capabilities and proposed GCP integrations
> must not be confused with deployed cloud resources. These diagrams do not authorise provisioning.
