# GCP Terraform Deployment Controls.

**Architecture:** ADR-000 and ADR-GCP-001.
**Environment:** Development.
**Deployment strategy:** Incremental hybrid.

## Authentication.

Local development uses Google Cloud Application Default
Credentials.

Long-lived service account keys are not permitted as
the default authentication mechanism.

Future automated deployments should use short-lived
credentials and least-privilege deployment identities.

## Terraform State.

The initial configuration-only environment does not
require a remote backend.

Before persistent cloud resources are introduced,
the team shall approve:

- State storage and access controls.
- Encryption and recovery arrangements.
- State locking and concurrent deployment controls.
- State retention and deletion policies.
- Separation between development and production state.

Terraform state files and plans must not be committed
to source control.

## Deployment Approval.

The permitted validation sequence is:

1. terraform fmt
2. terraform validate
3. terraform plan, after authentication and cost review
4. Review the proposed resource changes and estimated cost
5. Explicit approval
6. terraform apply

No resource creation is authorised by this document.

## Cost Controls.

The development budget alert is £20 per month.

Budget notifications are not hard spending limits.

Chargeable resources require an explicit cost estimate,
deployment duration and teardown strategy.

Persistent or continuously billed resources require
separate approval.

## Platform Protection.

The existing local platform remains authoritative
for development and integration.

GCP experiments must not mutate protected local
datasets, change Kafka offsets or interfere with
existing production-style workflows.

Routine production recovery must be automated.
