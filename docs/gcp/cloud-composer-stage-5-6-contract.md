# Cloud Composer Stages 5 and 6

Status: LOCAL IMPLEMENTATION AND OFFLINE READINESS GATE
Deployment: NOT AUTHORISED

## Stage 5: Functional local controller

Implemented:

- Strict bounded manifest structure.
- Explicit GCS object URI requirements.
- Generation identifier validation.
- SHA-256 field format validation.
- Duplicate object rejection.
- Deterministic execution identifiers.
- SQLite-backed duplicate admission detection.
- Fail-closed execution state.
- Standard-library offline tests.

Not implemented:

- Live GCS generation or checksum verification.
- GCS IAM and bucket policy enforcement.
- Manifest signing or trusted provenance.
- Actual Dataflow submission.
- Dataflow job reconciliation.
- PostgreSQL production durable queue integration.
- BigQuery reconciliation.
- Airflow runtime integration.

SQLite is a local development ledger only.
It is not the production distributed recovery mechanism.

## Stage 6: Deployment-readiness gate

Implemented:

- Explicit approval checklist.
- Missing-approval reporting.
- Fail-closed default.
- Offline tests.

The gate assesses a supplied approval document.
It does not independently verify evidence.
It does not execute Terraform or deploy resources.

An all-true approval file must not be treated as
proof of deployment readiness without independent review.

## Existing Airflow DAG

The existing Stage 4 DAG remains unchanged,
unscheduled and fail-closed.

The local controller is not yet wired into that DAG.
This avoids presenting a local proof of concept
as a production Composer integration.

## Next technical integration

1. Verify real immutable GCS object generations.
2. Implement the approved durable queue interface.
3. Replace wildcard Dataflow input with bounded inputs.
4. Implement stable cloud job submission identities.
5. Observe and reconcile terminal job states.
6. Verify BigQuery landing and quarantine.
7. Integrate the controller with Airflow.
8. Test under a compatible Composer Airflow runtime.
9. Complete IAM, network, pricing and billing review.

No cloud deployment is authorised.
