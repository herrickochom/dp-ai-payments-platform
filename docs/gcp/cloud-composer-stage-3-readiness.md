# Cloud Composer Stage 3: Security and Cost Readiness

Status: REVIEW REQUIRED
Environment: dev
Region: europe-west2
Cloud deployment: NOT AUTHORISED

## 1. Current Terraform position

The Composer module is isolated from the active development root.

It defines:
- A dedicated Composer service account.
- The Composer worker IAM role.
- Private environment configuration.
- Configurable VPC and subnet identifiers.
- A pinned-image input requirement.
- Terraform destruction protection.

Terraform schema validation does not prove deployment readiness.

## 2. Security assessment

Before deployment, verify:

- Composer service-agent prerequisites.
- Runtime service-account permissions.
- Google-managed service identities.
- Network and subnet ownership.
- Private Google API connectivity.
- DNS, firewall and egress policies.
- Web-server access restrictions.
- Secret Manager access boundaries.
- Logging, monitoring and audit requirements.

No public exposure of local platform services is authorised.

The Composer worker role does not automatically establish
least-privilege Dataflow and BigQuery workload permissions.

## 3. Network assessment

The proposed environment uses private networking.

Required evidence:
- Approved VPC identifier.
- Approved regional subnet identifier.
- Available address capacity.
- Connectivity to required Google APIs.
- Approved outbound routing.
- No dependency on local Docker DNS.

These items remain unresolved.

## 4. Version compatibility

Existing local Airflow:
Apache Airflow 3.1.8 with Python 3.12.

Composer image:
NOT SELECTED.

Verify the supported Composer 3 image, Airflow version,
Python version and provider packages before deployment.

## 5. Cost assessment

Current deployment authorisation: NONE.

Before deployment, record:
- Remaining GCP trial credits and expiry.
- Composer environment recurring charges.
- Scheduler, worker and web-server resource sizing.
- Environment storage and logging costs.
- Network and egress costs.
- Dataflow processing costs.
- BigQuery storage and query costs.
- Expected operating hours.
- Monthly budget and billing alerts.

No monetary estimate is asserted without verified
pricing and an approved configuration.

Billing alerts are not hard spending limits.

## 6. Stage 3 decision

Terraform module: SCHEMA VALIDATED
IAM readiness: BLOCKED
Network readiness: BLOCKED
Image compatibility: BLOCKED
Cost approval: BLOCKED
Deployment authorisation: BLOCKED

Next gate:
Review and approve the outstanding items before
connecting the Composer module to the active root.
