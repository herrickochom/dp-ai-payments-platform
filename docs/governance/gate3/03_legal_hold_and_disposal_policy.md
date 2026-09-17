# Gate 3.1 legal hold and disposal policy

Status: DEFINED. Hold registry, disposal executor and end-to-end physical erasure: NOT IMPLEMENTED in the inspected repository. Role assignments and retention decisions require owner approval. No regulatory period is asserted.

| State | Meaning and transition |
|---|---|
| ACTIVE | Approved active use. May enter ARCHIVED or HOLD; expiry requires all eligibility gates. |
| ARCHIVED | Approved restricted archival use and recovery. Original clock continues unless an approved policy explicitly says otherwise. May enter HOLD or EXPIRY_ELIGIBLE. |
| HOLD | Scoped preservation requirement; suspend destructive actions. Record prior state; explicit authorised release returns to that state and recomputes eligibility. |
| EXPIRY_ELIGIBLE | Approved clock elapsed; holds, ownership, dependencies and protected exclusions resolved. Eligibility is not execution authority. |
| DISPOSAL_APPROVED | Time-bounded approval covers an exact candidate manifest and method. Changed scope/references/dependencies/holds revoke approval. |
| DISPOSED | Approved scope completed and independently reconciled. Partial deletion cannot receive this state. |

HOLD overrides expiry/deletion, snapshot expiry, GC and archival actions that could lose evidence. A held dependency blocks affected disposal. Review dates do not automatically release holds. Protected Gate 2 exclusions remain excluded after hold release.

## Mandatory fail-closed decisions

| Unknown condition | Decision |
|---|---|
| Hold status/scope | BLOCKED; legal/privacy hold authority must resolve. |
| Asset ownership/deletion authority | BLOCKED; assign an approved accountable owner. |
| Downstream dependencies/copies | BLOCKED; complete lineage and copy inventory. |
| Nessie reachability/reference inventory | BLOCKED; prove reachability across retained branches/tags and protected hashes. |
| Backup/recovery dependency | BLOCKED; reconcile recovery requirements and manifests. |
| Clock/policy precedence | BLOCKED; obtain approved asset-specific policy. |
| Disposal suppression evidence during replay/restore | BLOCKED; do not publish recovered data. |

## Hold record, approvals and evidence

A future hold registry must record ID, authority/reason, entity/asset/time scope, issuer/time, dependencies, access restrictions, reviews and explicit release evidence. Conflicting instructions remain blocked.

Proposed approvals: accountable data owner confirms business eligibility; privacy/legal or regulatory liaison confirms retention obligations and hold clearance; recovery custodian confirms recovery independence; security approves restricted/sensitive audit disposal; authorised operations executes only the approved manifest. Assign named people before use. Restricted disposal must prevent operator self-approval. These are requirements, not existing approval services.

Required evidence: policy/version/clock; owners; current hold clearance; copy/lineage inventory; protected exclusions; complete reference-aware reachability; backup dependencies; dry-run manifest; exact method/scope; time-bounded approvals; execution receipts/failures; independent reconciliation. Retain sanitized evidence under its own approved policy.

## Disposal scope and residual copies

Assess source files, Raw, Kafka domain/retry/DLQ and replay inputs, offsets/recovery behaviour, current tables, Iceberg history/data/metadata, Nessie references, restricted mappings, ML inputs/features/models/predictions, logs, backups, exports/caches and other derivatives. Dropping a link, replacing a table, pseudonymising or deleting a current row is not proof of physical erasure.

If scoped removal is unavailable (for example individual Kafka records), record approved containment/eventual expiry, residual exposure and verification deadline. Held or retained backup copies remain explicitly tracked and restricted. Do not label complete scope DISPOSED while recoverable copies remain within it. Exceptions need owner/legal approval and cannot override frozen Gate 2 protection.

Before deletion is enabled, design an access-restricted, minimal disposal suppression record sufficient to prevent resurrection; approve its retention and key/version compatibility. It is NOT IMPLEMENTED. Physical erasure, backup-wide erasure and secure media sanitisation are not claimed as implemented. Dry-run eligibility is not disposal completion.

## Gate 3.2 factual verification update

VERIFIED inventory reveals missing protected tag and unavailable backup bucket (DRIFT / DEFECT); recovery dependencies remain unresolved and fail-closed disposal remains BLOCKED. No hold registry or disposal executor was introduced. Runtime inspection does not establish physical erasure.

See [runtime verification](06_runtime_verification.md) for scope, methods and limitations. These observations authorise no remediation or lifecycle mutation.
