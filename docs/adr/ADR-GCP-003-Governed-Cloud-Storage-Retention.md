# ADR-GCP-003: Governed Cloud Storage and Retention.

**Status:** Proposed.
**Parent:** ADR-000.
**Related:** ADR-GCP-001 and ADR-GCP-002.
**Date:** 26 October 2026.

## Context.

The Payments Data AI Platform requires governed storage
across local and Google Cloud environments.

Data categories have different operational, business,
audit and regulatory retention requirements.

Storage class transitions must not be confused with
mandatory data retention periods.

## Decision.

Adopt a policy-driven retention architecture.

The authoritative configuration shall be:

platform/config/governance/gcp_retention_schedule.yaml

The architecture shall support:

1. Data classification.
2. Approved dataset-specific retention schedules.
3. Standard, Nearline, Coldline and Archive storage.
4. Configurable lifecycle transition policies.
5. Legal holds and retention protection.
6. Auditable retention and disposal decisions.
7. Automated policy enforcement.
8. Storage cost monitoring.
9. Recovery-state protection.
10. Iceberg snapshot-aware maintenance.

## Storage Lifecycle.

The proposed review intervals are:

- 30 days.
- 60 days.
- 90 days.
- 3600 days.

These are policy parameters, not universal
storage transitions or mandatory retention periods.

Google Cloud Storage minimum storage durations
and early deletion charges must be considered
when approving transitions.

## Data Protection.

No automatic deletion shall be enabled for
a dataset without an approved retention schedule.

Active Iceberg data and metadata must not be
deleted solely because of object age.

Flink checkpoints and savepoints shall remain
under the existing recovery architecture.

Legal holds shall prevent disposal where applicable.

Retention locking requires separate approval
because of its irreversible implications.

## Terraform.

Terraform shall manage approved GCS infrastructure,
bucket policies, IAM and supported lifecycle controls.

The policy catalogue shall be validated before
any corresponding infrastructure deployment.

No GCP resources are authorised by this ADR.

## Automation.

Approved retention and lifecycle operations shall
run automatically without routine manual intervention.

Exceptions, legal holds and disposal decisions
must be recorded for auditability.

## Cost Controls.

The existing £20 monthly development budget alert
is not a hard spending limit.

Cloud storage, retrieval, operations, early deletion
and network transfer costs require assessment.

## Consequences.

The design supports consistent governance across
hybrid storage environments.

Additional policy validation and operational
integration are required before enforcement.

## Deferred Decisions.

- Actual retention durations by dataset.
- GCS bucket topology.
- Bucket retention policies and locking.
- IAM implementation.
- Terraform remote state.
- Deployment approval.

## Proposed Baseline Retention Schedule.

| Dataset | Storage class | Retention |
|---|---|---|
| Temporary test data | Standard | 30 days |
| Operational events | Standard | 60 days |
| Historical analytics | Nearline | 90 days |
| Historical payment records | Coldline | 365 days |
| Long-term financial archives | Archive | 3600 days |
| Audit evidence | Archive | 3600 days |
| Iceberg tables | Standard | Snapshot-governed |
| Flink recovery state | Existing MinIO | Recovery-governed |

These periods are proposed defaults and require
records-owner and compliance approval.

Storage-class minimum durations are billing
conditions, not legal retention requirements.

Automatic deletion remains disabled until the
relevant schedule and disposal controls are approved.

Iceberg maintenance must remain snapshot-aware.
Flink recovery state must remain protected.

Retention locks require separate explicit approval.

## Architecture and Data-Flow Diagrams

The following diagrams illustrate the architectural decision and its relevant data flows.

- [Governed storage architecture](../architecture/gcp/diagrams/governed-storage.svg)

**Deployment status:** Diagrams distinguish existing local components from proposed or future GCP services. They do not authorise cloud provisioning.
