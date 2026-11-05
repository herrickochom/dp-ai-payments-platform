# Cloud Composer Infrastructure Contract

Status: PROPOSED
Environment: dev
Region: europe-west2
Deployment: NOT AUTHORISED

## 1. Purpose

Define a governed Cloud Composer orchestration environment for the
DP AI Payments Platform's GCP workloads.

This contract does not authorise resource provisioning.

## 2. Architecture boundaries

Existing local Airflow remains unchanged.

Cloud Composer will orchestrate GCP-specific workloads involving:

- Google Cloud Storage Bronze payment evidence.
- Bounded Apache Beam / Dataflow ingestion.
- BigQuery landing and governed transformations.
- Data quality and reconciliation.
- Operational evidence and acceptance checks.

Cloud Composer must not depend on local Docker DNS names,
internal MinIO endpoints, or local PostgreSQL services.

## 3. Infrastructure requirements

The future Terraform implementation must define:

- An explicitly supported Cloud Composer version.
- A dedicated Composer service account.
- Least-privilege IAM permissions.
- Private networking and controlled egress.
- Approved GCP API prerequisites.
- Environment configuration and resource limits.
- Logging, monitoring and operational alerting.
- Explicit dependency and deployment controls.

All infrastructure changes require review before deployment.

## 4. Security requirements

- No plaintext credentials in Terraform or DAG source code.
- No unauthorised public service exposure.
- Use managed identities wherever supported.
- Apply least privilege to GCS, Dataflow and BigQuery.
- Keep development and production identities separate.
- Do not expose existing local execution endpoints.

## 5. Orchestration requirements

The GCP payment DAG must support:

1. Automatic, governed workflow initiation.
2. Verification of immutable, bounded Bronze input manifests.
3. Stable idempotency and execution identifiers.
4. Controlled Dataflow submission.
5. Dataflow completion and failure observation.
6. BigQuery landing verification.
7. Reconciliation and data quality.
8. Recorded acceptance evidence.

No unbounded wildcard replay is permitted.

## 6. Retry and recovery ownership

Airflow coordinates workflow dependencies.

The durable execution layer owns execution retries,
lease recovery and reconciliation.

Submission must be idempotent.

A failed or orphaned execution must not cause uncontrolled
duplicate processing.

## 7. Deployment restrictions

Until explicitly approved:

- Do not enable the Cloud Composer API.
- Do not provision a Composer environment.
- Do not run terraform apply.
- Do not submit billable Dataflow workloads.
- Do not introduce unattended cloud execution.
- Do not modify the existing local Airflow pipeline.

## 8. Cost controls

Before provisioning:

- Confirm trial-credit eligibility and remaining balance.
- Estimate Composer recurring environment costs.
- Estimate supporting compute, storage and networking costs.
- Define a monthly budget and billing alerts.
- Review environment shutdown and deletion procedures.
- Obtain explicit deployment approval.

Budget alerts alone do not enforce a spending cap.

## 9. Validation gates

The implementation must pass:

- Terraform formatting and validation.
- IAM and networking review.
- DAG import and dependency tests.
- Idempotency and replay-safety tests.
- Existing GCP CI checks.
- Security and cost approval.

## 10. Implementation status

Architecture discovery: COMPLETE
Infrastructure contract: PROPOSED
Composer Terraform implementation: NOT STARTED
Composer DAG implementation: NOT STARTED
Cloud deployment: NOT AUTHORISED
