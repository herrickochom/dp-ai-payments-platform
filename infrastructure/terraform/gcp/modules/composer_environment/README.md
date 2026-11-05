# GCP Cloud Composer Terraform Module

Status: IMPLEMENTATION DRAFT

This module is not connected to any Terraform environment.

## Deployment restrictions

- Do not run terraform apply.
- Do not enable GCP APIs.
- Do not provision Cloud Composer.
- Do not add this module to an active root without approval.

## Outstanding approvals

1. Confirm supported Composer 3 image and Airflow version.
2. Approve VPC, subnet, routing and private connectivity.
3. Review service account and additional workload permissions.
4. Review Composer environment sizing.
5. Confirm trial credits, pricing and budget controls.
6. Approve an explicit deployment procedure.

## Current scope

- Dedicated service account.
- Composer worker IAM role.
- Private Composer environment configuration.
- Destruction protection.
- Fully pinned image version required.

This module is not production-ready.
