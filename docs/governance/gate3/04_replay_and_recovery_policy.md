# Gate 3.1 replay and recovery policy

Status: DEFINED. Existing safeguards have partial repository implementations; full operational/replay/disposal verification remains absent. This policy authorises no operation.

| Operation | Meaning | Proposed authorisation | Required safeguards/evidence |
|---|---|---|---|
| Transport retry | Retry delivery/processing of the same in-flight record/coordinate. | Service within owner-approved bounded configuration; platform owner approves changes. | Durable-before-commit, bounded backoff, failure identity and fail-stop on failed DLQ acknowledgement. Does not authorise producer rerun. |
| DLQ replay | Republish a failed payload at a new source offset. | Data owner and operations approver; privacy/hold review for restricted content. | Exact partition/offset/failure ID, reason, complete inspection, durable execution ledger, duplicate checks, delivery acknowledgement and disposal suppression. |
| Source reprocessing | Reread source/Raw and regenerate derivatives, potentially rerunning producer. | Source/data owner and platform change authority; explicit freeze reopening for protected Gate 2 changes. | Checksummed provenance, bounded scope/selectors, semantic deduplication, holds/suppression and downstream reconciliation. |
| Disaster recovery | Restore services/catalog/storage following failure. | Incident commander, data owner and recovery custodian. | Isolated restore, compatible metadata/files/keys, integrity/ordering, approved RPO/RTO, disposal reconciliation before serving. |
| Nessie rollback | Reassign a serving branch to a prior catalog commit. | Catalog owner, recovery custodian and data-owner/change approval; freeze authority for protected state. | File availability, complete dependencies/holds and disposal suppression. Never move/delete protected recovery tags. |
| Iceberg maintenance | Snapshot expiry, orphan cleanup or reference-aware GC; separate from replay. | Data owner, catalog/recovery custodian and maintenance operator. | Approved dry-run manifest, retained-reference reachability, grace window, protection/holds, fresh plan and disposable-fixture tests. |

Roles require named approval. Services cannot self-authorise offset resets, replay execution, source reruns, rollback, GC or protected-state changes.

## Present safeguards and unresolved gaps

`services/kafka-consumer-events/kafka_consumer_events.py` implements deterministic coordinate-based Raw paths and existing-object checks, disabled auto commit, synchronous commit after durable Raw or acknowledged DLQ, bounded retries and fail-stop when final DLQ publication fails. Producer idempotence does not deduplicate a later manual rerun.

`services/kafka-consumer-events/replay_dlq.py` defaults to inspection, requires exact partition/offset/reason and detects duplicate failure envelopes. A durable executed-replay ledger is NOT IMPLEMENTED; its time-bounded scan does not prove completeness. New-offset replay escapes same-coordinate sink idempotency. Complete-scan fail-closed behaviour, explicit acknowledgement and repeat-execution protection require focused verification/implementation. No tool is executed by this contract.

Read-only effective offset/topic policy evidence is required. Offset reset needs separate approval; `auto.offset.reset=earliest` cannot substitute for a disposal-safe recovery decision when offsets disappear.

## Preventing resurrection

Compare each replay/restore source with current holds and approved disposal suppression records. Unknown, inaccessible or incompatible suppression state blocks publication. Restore into isolation first; apply separately authorised exclusions and validate all derivatives before serving. Cover source, Kafka/DLQ, Raw, mappings, table/history, ML and backup copies.

A pre-disposal catalog hash is not sufficient authority to serve historic rows. Any required transformation/key/token-link changes need separately scoped approval, respecting freeze/privacy/RBAC. Record held/backup residual copies and access limitations; never claim physical erasure when recoverable copies remain.

## Gate 2 restrictions and recovery runbook

[Protected baseline](01_protected_baseline.md) applies to all operations. Do not repeat the Gate 2 Agent replay, rematerialise token links, rebuild accepted tables, retrain/rescore, move recovery tags or mutate protected backup prefixes to recreate evidence. Historical counts remain historical.

A future backup/restore runbook must cover PostgreSQL/Nessie catalog, MinIO objects/manifests, Kafka data/KRaft/offsets, schema registry, BI metadata, audit records, ML releases and secure key recovery. State what is backed up, reproducible or unavailable; obtain owner-approved RPO/RTO without inventing numeric targets. Demonstrate ordering, integrity, suppression and least privilege in isolation; source backups/references remain unchanged.

## Gate 3.2 factual verification update

VERIFIED: effective Kafka settings match managed-topic declarations, but a stalled consumer can lose unarchived events through topic retention. DRIFT / DEFECT: protected recovery tag is missing and backup bucket is absent. Recovery/replay prerequisites remain blocked; no rollback or replay occurred. DEFECT: acknowledged-DLQ source logging emits reversible encoded original payload/key; publication payload must remain restricted and logging must be separately reviewed.

See [runtime verification](06_runtime_verification.md) for scope, methods and limitations. These observations authorise no remediation or lifecycle mutation.

## Gate 3.3 reconciliation update

Historical target lookup using explicit Nessie ref@hash returns commit-not-found; NESSIE TAG RECREATION BLOCKED. Backup location is BACKUP LOCATION INCONCLUSIVE: active bucket absent, current MinIO volume postdates both backup timestamps, no older MinIO candidate identified in local container/volume metadata; privileged filesystem inspection unavailable. Cause is not proven. Historical verification evidence remains valid as a record, but backup is not currently available for restore.

Consumer logging source remediation uses metadata-only allowlisting and omits exception text/tracebacks; all 48 selected tests passed, including 13 data-protection tests. DLQ/replay content and commit/retry semantics remain unchanged. Running consumer was not restarted; runtime activation remains outstanding. No tag or backup was created. See [Gate 3.3 report](07_recovery_reconciliation_and_logging_remediation.md). Earlier Gate 3.2 observations remain historical, not overwritten.
