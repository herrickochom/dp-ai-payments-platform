# Gate 3.3 recovery reconciliation and logging remediation

Date: 2026-09-16. Status: recovery reconciliation BLOCKED; logging source remediation VERIFIED by focused unit tests, runtime activation NOT IMPLEMENTED in this phase. No commit, tag recreation, replacement backup or service restart.

## Phase A: exact historical facts

Sources: frozen `docs/evidence/gate2/10_incidents_and_recovery.md` and `11_gate2_final_acceptance.md`; neither changed.

| Fact | Exact recorded value |
|---|---|
| Historical pre-Gate2 Nessie hash | `f85893b23c441db5abd0d2400da134cbb5fc4d0cb52a1e3c24e3ae02d85b35df` |
| Protected tag | `pre-gate2-20260915T155541Z` |
| Backup bucket | `dp-ai-payment-gate2-backup` |
| Original prefix | `gate2-preflight/20260915T155602Z` |
| Original verification | 17 affected locations; 68 / 68 objects integrity verified |
| Additional prefix | `gate2-additional-prebuild/20260916T100622Z/` |
| Additional verification | 23 existing tables; 92 / 92 objects verified; 595,935 bytes; path, size, ETag and full SHA256 verified |
| Original checksum details | Aggregate integrity statement only in these files; per-object hashes, total bytes and manifest contents not recorded there |
| MinIO endpoint/storage identity in these files | No specific endpoint, volume identity or credential values recorded; bucket/prefixes above identify logical location |
| Registry compatibility history | Local MinIO image changed to corresponding quay.io images after Docker Hub availability issues |

Repository inspection: `git log` and bounded Gate 2 implementation diff show `minio/minio:latest` changed to `quay.io/minio/minio:latest`, with matching mc image change. That diff does not change MinIO named-volume binding, endpoint, bucket initializer or root-credential environment-variable wiring. No tracked changes after the acceptance commit exist for those files. Actual historical uncommitted configuration/secret values are not preserved/proven; no credentials were displayed or inferred from Git.

Current Docker metadata: MinIO container created `2026-09-16T12:23:10.625398033Z`, image `quay.io/minio/minio:latest`, volume `dp-ai-payments-platform_minio-data`, mount destination `/data`, storage path `/var/lib/docker/volumes/dp-ai-payments-platform_minio-data/_data`. Volume CreatedAt is `2026-09-16T13:23:09+01:00` (12:23:09 UTC), after both original backup timestamp 2026-09-15T15:56:02Z and additional backup timestamp 2026-09-16T10:06:22Z. Thus both backup timestamps precede creation of the currently registered volume. This suggests replacement/unavailability of the historical storage instance, but does not prove deletion or its cause.

All local Docker container/volume names and relevant mount metadata were examined. No older MinIO container or named MinIO volume was identified. Eleven anonymous volumes were enumerated: eight are attached to Kafka/schema/PostgreSQL components; three are unattached and all report creation after both backup timestamps. None has metadata identifying historical MinIO storage. No old MinIO candidate was found to mount or modify. A read-only privileged top-level directory-marker check could not run because sudo requires interactive authentication; no volume contents were read, no permission changes made and no alternative container was started.

Current S3 ListBuckets returns only `dp-ai-payment`; both protected-prefix presence checks return NoSuchBucket for `dp-ai-payment-gate2-backup`. No objects or payloads were listed to output.

Classification: **BACKUP LOCATION INCONCLUSIVE**. The historical backup is unavailable at the inspected active endpoint; metadata suggests the historical storage instance was replaced, but filesystem access limitations and unexamined external/remote copies prevent a global loss claim. Registry change alone does not explain volume recreation. No destructive-command execution history or deletion timestamp is proven.

HISTORICAL GATE 2 BACKUP EVIDENCE REMAINS VALID AS A RECORD OF THE
VERIFICATION PERFORMED AT THAT TIME, BUT THE BACKUP IS NOT CURRENTLY
AVAILABLE FOR RESTORE.

## Phase B: Nessie reconciliation

Exact target/name are proven by both frozen evidence sources. Current complete reference inventory contains only main at `ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`.

Correct explicit read-only lookup: GET `/api/v2/trees/main@f85893b23c441db5abd0d2400da134cbb5fc4d0cb52a1e3c24e3ae02d85b35df/history?max-records=1` returned HTTP 404, REFERENCE_NOT_FOUND, with `Commit '<historical hash>' not found`. The historical target is not currently resolvable through the active Nessie store.

Earlier probes using camelCase `hashOnRef`/`maxRecords` query names did not select the target: history started at current main and entries returned current-state entries. Their HTTP success is not evidence of historical resolution. A `/log` probe was also an unsupported-path 404, not commit-loss evidence. The explicit ref@hash lookup above is the decisive result; no recreation command is prepared as executable while its prerequisite fails.

**NESSIE TAG RECREATION BLOCKED**: original hash and intended name are unambiguous, but commit resolution fails. No reference mutation or rollback occurred. An eventual authorised tag recreation must resolve the exact historical commit first, create only a tag at that hash, leave main/table contents unchanged, and independently verify the result. Do not substitute current main for the original target.

## Phase C: logging-only remediation

Implementation: `services/kafka-consumer-events/kafka_consumer_events.py` adds `log_processing_error()` with explicit field selection: event category, topic, partition, offset, retry count, DLQ destination and a known bounded error class. It excludes original key/payload/base64, business/event identifiers, free-form exception text, arbitrary exception class names and tracebacks.

Affected emissions: acknowledged-DLQ envelope log; fail-stop DLQ log (formerly included business_key); serialization/storage error logs; Kafka poll-error text; writer-close traceback. DLQ/retry envelope generation/publication, Raw content, commit ordering, retries, schemas, business semantics and analytical contracts are unchanged. Known error classes come from an explicit class set; unknown types log only Exception. Reduced error-text detail is intentional to avoid source-value disclosure; operational categories/coordinates remain available.

Focused synthetic tests in `tests/test_kafka_logging_privacy.py` cover seven cases, including acknowledged publication, both unacknowledged/raising DLQ fail-stop paths, serialization exceptions/close failures and Raw/legacy DLQ storage errors. They check representative NIN/account/payload/beneficiary/name/phone/email markers and their encoded copies are absent; no traceback is emitted. They also prove restricted publication retains original payload/key and business/error fields, deterministic failure key/header, synchronous commit after acknowledgement and no commit/later poll on fail-stop. All external clients are mocks; no Kafka/MinIO connections are opened by these tests.

Command: `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m pytest -q -p no:cacheprovider tests/test_kafka_logging_privacy.py tests/test_red_kafka_fixes.py tests/test_kafka_architecture.py tests/test_data_protection.py`.

Result: **48 passed in 2.34s**: 7 new logging privacy cases, 17 existing RED Kafka correctness tests, 11 Kafka architecture tests and 13 existing data-protection tests. No dbt or live replay/offset changes. Runtime consumer was not rebuilt/restarted: current-container activation remains outstanding; historical log exposure/cleanup was not investigated by reading sensitive logs.

## Phase D: recovery governance

Do not rewrite historical Gate 2 evidence or describe any future replacement as the historical backup. A future current-state backup requires separate authorisation, a new timestamp/prefix, manifest and integrity record, explicitly labelled Gate 3/current-state. Never reuse either protected historical prefix. No backup was created in Gate 3.3.

Protected hashes/prefixes remain fail-closed exclusions even while their current recovery availability is blocked. Missing historic metadata/storage is not evidence that physical cleanup is safe.

Remaining blockers: unavailable original recovery commit/tag and backup; unresolved owner/legal retention precedence/holds; absent dry-run planner/fail-closed/reference-aware fixtures; incomplete replay execution ledger/suppression; durable audit design; isolated restore and complete ML provenance. Source logging defect is fixed/tested, but runtime deployment and any historical exposure response require separately scoped authorisation.

## Phase E: change and safety scope

This phase modifies only consumer logging, one focused test file and Gate 3 documentation. It does not rewrite Git frozen commits or Gate 2 evidence. Nessie main remained at the frozen hash before/after checks. No Nessie reference mutation, MinIO mutation, backup creation/deletion, Kafka mutation/replay/offset change, dbt execution, Iceberg maintenance, token materialisation, ML execution or service restart was performed.

Phase-specific files: consumer implementation; new `tests/test_kafka_logging_privacy.py`; factual addenda in documents 01, 04, 05 and 06; this new document 07. Documents 02/03 remain unchanged during Gate 3.3. All earlier Gate 3 documents remain uncommitted/untracked.

## Gate 3.4 attempted current-state baseline

Preflight/quiescence checks succeeded, but the new tag creation request returned HTTP 400. Complete read-only verification confirms no new tag and unchanged frozen main. Fail-closed stop occurred before backup or runtime activation. Logging remains IMPLEMENTED_AND_TESTED, not active. No new protected recovery baseline is declared. See [Gate 3.4 report](08_current_recovery_baseline_and_logging_activation.md).
