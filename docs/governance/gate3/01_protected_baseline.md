# Gate 3.1 protected baseline

Status: DEFINED. Technical protection enforcement: NOT VERIFIED. This documentation authorises no maintenance.
Date: 2026-09-16.

| Immutable reference | Protected value |
|---|---|
| Gate 2 implementation commit | `ba080c74b351ce995649b2e3cd7b2d340bb2aca5` |
| Gate 2 final acceptance commit | `3284e2649e9a51cf25c4f6b89a0c2207f5333833` |
| Frozen Nessie main data-state hash | `ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba` |
| Pre-Gate2 recovery hash | `f85893b23c441db5abd0d2400da134cbb5fc4d0cb52a1e3c24e3ae02d85b35df` |
| Protected recovery tag | `pre-gate2-20260915T155541Z` |
| Protected backup bucket | `dp-ai-payment-gate2-backup` |
| Protected backup prefix | `gate2-preflight/20260915T155602Z` |
| Protected backup prefix | `gate2-additional-prebuild/20260916T100622Z/` |

Protection includes complete prefix subtrees regardless of trailing slash, object versions if present, integrity manifests, and metadata/snapshots/data files necessary to recover both protected states. The two Gate 2 backup prefixes must never become ordinary retention candidates. Age, storage pressure or generic retention declarations cannot override their protection.

## Evidence and freeze

Authority: [final acceptance](../../evidence/gate2/11_gate2_final_acceptance.md), supported by [incidents and recovery](../../evidence/gate2/10_incidents_and_recovery.md). Historical backup verification: 68/68 original objects and 92/92 additional objects. This is recorded integrity evidence, not a new live inspection or complete restore drill. Final acceptance supersedes preparation wording in the evidence README; preserve historical evidence unchanged.

No dbt execution, Kafka replay/offset reset, token-link materialisation, ML retraining/rescoring, snapshot expiry, orphan removal, GC, object archival/deletion, bucket-policy/retention changes or reference movement/deletion is permitted by Gate 3.1. Do not weaken privacy/tokenisation/RBAC or change Gate 2 evidence. Reproducing accepted counts is not a reason to mutate data.

## Fail-closed maintenance requirement

Lifecycle maintenance must fail closed if it cannot prove that a candidate object, snapshot or reference is outside the protected baseline and all retained recovery dependencies. Current main alone is insufficient: enumerate retained branches/tags, protected hashes, metadata/files and backup dependencies. Unknown reachability, ownership, holds, paths or manifests means BLOCKED, not eligible.

A future planner must record protected-inventory version, observed reference hashes, exact candidates and reachability evidence. Changed references, hold state or dependency evidence invalidate the plan and approval; replan before execution. No timeout or convenience override may bypass exclusions.

Separately scoped authorisation is required for future mutation. Any change to protected Gate 2 state or privacy/transformation contracts requires explicit freeze reopening; retain original evidence and recovery records. Technical tag immutability, storage object lock/versioning, backup IAM and automatic maintenance exclusions are NOT VERIFIED; this document does not activate them.

## Gate 3.2 factual verification update

VERIFIED: Git HEAD and Nessie main match the frozen references. DRIFT / DEFECT: complete live reference inventory contains only main; direct protected-tag GET returned HTTP 404. Expected backup bucket `dp-ai-payment-gate2-backup` and both prefix listings returned NoSuchBucket at the active endpoint. Protected identifiers above remain required exclusions; historical Gate 2 evidence is unchanged. Recovery availability is currently blocked, not verified.

See [runtime verification](06_runtime_verification.md) for scope, methods and limitations. These observations authorise no remediation or lifecycle mutation.

## Gate 3.3 reconciliation update

Historical target lookup using explicit Nessie ref@hash returns commit-not-found; NESSIE TAG RECREATION BLOCKED. Backup location is BACKUP LOCATION INCONCLUSIVE: active bucket absent, current MinIO volume postdates both backup timestamps, no older MinIO candidate identified in local container/volume metadata; privileged filesystem inspection unavailable. Cause is not proven. Historical verification evidence remains valid as a record, but backup is not currently available for restore.

Consumer logging source remediation uses metadata-only allowlisting and omits exception text/tracebacks; all 48 selected tests passed, including 13 data-protection tests. DLQ/replay content and commit/retry semantics remain unchanged. Running consumer was not restarted; runtime activation remains outstanding. No tag or backup was created. See [Gate 3.3 report](07_recovery_reconciliation_and_logging_remediation.md). Earlier Gate 3.2 observations remain historical, not overwritten.
