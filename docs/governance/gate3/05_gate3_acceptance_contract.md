# Gate 3 acceptance contract

Status: DEFINED. Documentation does not establish acceptance. Gate 3.1 performs no runtime implementation or verification.

Statuses: DEFINED = requirement written; NOT VERIFIED = implementation/declaration exists but required evidence absent; NOT IMPLEMENTED = complete control absent in discovery; BLOCKED = unresolved prerequisite; POC LIMITATION = scoped explicitly accepted exception; PRODUCTION FOLLOW-UP = production engineering obligation. No criterion is marked PASS.

## A. Required for Gate 3 POC acceptance

| ID | Criterion and required evidence | Current status |
|---|---|---|
| G3-01 | Approved lifecycle inventory: all matrix assets, named owners, classification, clocks, copies and recovery dependencies; recorded owner/legal approvals. | DEFINED; approval BLOCKED |
| G3-02 | Resolve/document retention precedence between governance periods and MinIO 365-day declaration with asset-specific decisions; distinguish effective settings from intent. | BLOCKED |
| G3-03 | Hold authority/scope/release and state semantics; planner tests prove unknown-status blocking. | DEFINED; enforcement NOT IMPLEMENTED |
| G3-04 | Protected Gate 2 exclusions in planner, including both prefix subtrees and all snapshot/file/recovery dependencies. | DEFINED; enforcement NOT IMPLEMENTED |
| G3-05 | Read-only effective Kafka retention/cleanup/partitions/RF evidence, relevant internal-topic/offset policies, timestamp/provenance and manifest comparison. | NOT VERIFIED |
| G3-06 | Read-only effective MinIO bucket policy, lifecycle, versioning/lock and backup-protection evidence; record absent controls accurately. | NOT VERIFIED |
| G3-07 | Read-only Nessie branch/tag/hash inventory; verify protected tag/hash and retained dependencies. | NOT VERIFIED |
| G3-08 | Read-only Iceberg metadata/snapshot ID/time/file inventory and reachability across retained/protected references. | NOT VERIFIED |
| G3-09 | Investigate initializer `mc policy set download` and live bucket access; resolve confirmed protected-data exposure through authorised remediation and negative access evidence. | BLOCKED pending investigation |
| G3-10 | Investigate consumer failure-envelope logging of encoded payload/key and error text; resolve exposure paths and prove metadata-only/redacted logging with focused tests. | BLOCKED pending resolution |
| G3-11 | Dry-run lifecycle planner produces scoped candidates/exclusions/reasons and policy/inventory versions without mutation. | NOT IMPLEMENTED |
| G3-12 | Planner tests reject unknown holds/owners/dependencies/recovery, incomplete reachability, stale plans, unresolved policy and protected candidates. | NOT IMPLEMENTED |
| G3-13 | Reference-aware maintenance tests on disposable fixtures preserve retained tag/branch files, held assets and protected dependencies; no frozen-state maintenance. | NOT IMPLEMENTED |
| G3-14 | Replay safeguards: complete inspection, durable execution audit/deduplication, explicit acknowledgement, bounded authority, suppression and separately controlled offset resets. | NOT IMPLEMENTED; existing partial controls NOT VERIFIED |
| G3-15 | Approved durable audit-retention design: persistence/access/rotation/clock/integrity/recovery; justify or explicitly supersede seven-year declaration. | NOT IMPLEMENTED |
| G3-16 | Backup/restore runbook: coverage, immutable manifests, access/hold/expiry, approved RPO/RTO and ordered metadata/file/key recovery. | NOT IMPLEMENTED; historical integrity evidence exists |
| G3-17 | Isolated restore demonstration: checksums/manifests, reference/file recovery, serving isolation, suppression and privacy/RBAC checks; original backups unchanged. | NOT VERIFIED |
| G3-18 | ML release provenance: accepted artefact hashes, feature/input provenance, actual environment versions and Git/Nessie linkage where known; disclose unknown history, no retraining/rescoring for evidence. | NOT IMPLEMENTED; partial metadata present |
| G3-19 | Operational runbooks/evidence cover readiness/freshness, lag versus retention, DLQ failure, capacity and restart; approved alert/manual-check owners. | NOT VERIFIED; partial checks present |
| G3-20 | Final before/after read-only evidence proves protected hashes/tag, backup objects/manifests and Gate 2 accepted data state unchanged; privacy/RBAC preserved. Repository diff alone is insufficient. | NOT VERIFIED |
| G3-21 | Copy-aware disposal/resurrection policy covers all matrix assets; residual copies and limitations recorded; physical erasure claimed only with complete scope evidence. | DEFINED; execution NOT IMPLEMENTED |

Evidence must identify scope/date/reviewer/tools, sanitized results, failures/remediation and limitations. Documentation cannot replace tests or live evidence. Later verification/fixes require separate authorisation.

POC acceptance need not expire/delete frozen data. Maintenance safety may be demonstrated entirely on disposable fixtures. Any deferred real-data disposal enforcement needs an explicit exception with destructive maintenance disabled; mandatory planner/hold/protection tests cannot be waived by a generic POC label.

## B. Accepted POC limitations

Gate 2 records local single-broker/RF=1 scale, production TLS/IAM/KMS/encryption/monitoring gaps and unpinned ML environment/version warnings as scoped follow-ups. Gate 3 must record continuing scope and owner acknowledgement; these are not production-readiness claims.

Ephemeral staging, manual operational checks, design-only audit retention and disabled real-data disposal are proposed Gate 3 limitations requiring explicit owner acceptance, compensating safeguards and follow-up owners. They are not accepted merely by documenting them. Public protected-data access, payload logging, unknown holds and unsafe recovery-reference cleanup cannot be excused by POC status.

## C. Production follow-ups

- Legal/privacy-approved jurisdiction-specific schedules, hold registry and complete disposal/physical-erasure evidence.
- Enterprise IAM/TLS/KMS, secure key recovery/rotation and least-privilege maintenance identities.
- Kafka replication/availability, monitored replay windows and durable replay audit service.
- Durable tamper-resistant audit retention and central monitoring/alert response.
- Reference-aware maintenance service with fresh-plan validation and appropriate storage lock/versioning.
- Scheduled immutable/off-site backups as approved, regular recovery drills and approved RPO/RTO.
- Immutable ML releases, compatible dependency pinning, complete provenance and promotion/rollback.

Production follow-ups cannot silently waive required POC criteria.

## Proposed order after Gate 3.1

1. Approve owners, clocks, precedence, hold authority and protected inventory.
2. Collect effective policies/inventories read-only; investigate access/logging blockers.
3. Separately authorise bounded fixes, dry-run planner and fail-closed tests.
4. Demonstrate maintenance/replay safety on disposable fixtures.
5. Complete audit design, operational/restore runbooks and isolated restore evidence.
6. Capture accepted ML provenance, approved exceptions and unchanged-Gate-2 evidence.

Gate 3.1 authorises no implementation, runtime mutation or commit.

## Gate 3.2 factual verification update

G3-05: managed-topic effective policy verified MATCH; current lag/range evidence remains unverified. G3-06: active bucket policy/lifecycle/versioning/lock inspected; backup bucket absent. G3-07: main verified MATCH, protected tag missing (DRIFT / DEFECT); recovery criterion BLOCKED. G3-08: 105 persistent tables/104 snapshots inventoried without metadata errors; this does not establish cross-history cleanup safety. G3-09: no anonymous bucket policy on active data bucket; potential script-based public exposure not demonstrated in current policy. G3-10: source logging DEFECT CONFIRMED; remediation/tests outstanding. G3-15: effective audit persistence UNVERIFIED (no container); default design ephemeral. G3-17: protected backup availability BLOCKED. G3-18: artefact/metadata hashes and 20-field contract recorded; installed feature-container sklearn 1.9.1 verified, live scorer Python/sklearn and training sklearn unverified. G3-20: Git/main match but full frozen recovery-state confirmation BLOCKED by missing tag/backups. No criterion is converted to PASS.

See [runtime verification](06_runtime_verification.md) for scope, methods and limitations. These observations authorise no remediation or lifecycle mutation.

## Gate 3.3 reconciliation update

Historical target lookup using explicit Nessie ref@hash returns commit-not-found; NESSIE TAG RECREATION BLOCKED. Backup location is BACKUP LOCATION INCONCLUSIVE: active bucket absent, current MinIO volume postdates both backup timestamps, no older MinIO candidate identified in local container/volume metadata; privileged filesystem inspection unavailable. Cause is not proven. Historical verification evidence remains valid as a record, but backup is not currently available for restore.

Consumer logging source remediation uses metadata-only allowlisting and omits exception text/tracebacks; all 48 selected tests passed, including 13 data-protection tests. DLQ/replay content and commit/retry semantics remain unchanged. Running consumer was not restarted; runtime activation remains outstanding. No tag or backup was created. See [Gate 3.3 report](07_recovery_reconciliation_and_logging_remediation.md). Earlier Gate 3.2 observations remain historical, not overwritten.

## Gate 3.4 attempted current-state baseline

Preflight/quiescence checks succeeded, but the new tag creation request returned HTTP 400. Complete read-only verification confirms no new tag and unchanged frozen main. Fail-closed stop occurred before backup or runtime activation. Logging remains IMPLEMENTED_AND_TESTED, not active. No new protected recovery baseline is declared. See [Gate 3.4 report](08_current_recovery_baseline_and_logging_activation.md).

## Gate 3.5C offline planner implementation evidence

`services/shared/lifecycle_planner.py` implements a deterministic supplied-metadata dry-run eligibility function; `tests/test_lifecycle_planner.py` verifies 69 offline synthetic cases. Existing data-protection selection: 13 passed. See [planner contract and limitations](09_lifecycle_planner.md).

G3-03/G3-04: hold/protection flags and conservative unknown handling are IMPLEMENTED_AND_TESTED within this function, not a live hold/protected-asset registry. G3-11: decision function implemented; complete candidate/exclusion manifest with policy/inventory versions remains outstanding. G3-12: supplied-metadata blockers, clock/approval validation and precedence tested; live incomplete-reachability/stale-plan detection remains outside this implementation. G3-13 reference-aware maintenance fixtures remain outstanding. No complete acceptance criterion is converted to PASS solely by this implementation. Deletion remains disabled; no executor or infrastructure mutation exists in the planner. Historical evidence is unchanged.
