# Gate 3.6A — replay safety and durable ledger contract

Discovery date: 2026-09-17. Scope: static repository inspection and this new
governance document only. No replay utility, tests, consumer, producer, Docker,
dbt, ML or infrastructure command was executed. Infrastructure mutations: 0;
Kafka messages replayed: 0; offsets reset: 0. This document authorises no replay.
Gate 3.5 implementation/evidence and the payment consumer remain unchanged.

## Evidence and classification rules

IMPLEMENTED_AND_TESTED means source exists with relevant repository tests and
prior recorded test evidence where identified below; it does not assert a fresh
test run or end-to-end runtime proof in this phase.
IMPLEMENTED_NOT_RUNTIME_VERIFIED means source/configuration exists but the
specific operational guarantee has not been demonstrated here. PARTIAL means
the implementation covers only part of the control. DOCUMENTED_ONLY means a
policy or procedure exists without enforcement. NOT_IMPLEMENTED means no
implementation was found in the searched repository. POC_LIMITATION identifies
a development constraint, not an acceptance waiver for unsafe replay.

Inspected sources: `services/kafka-consumer-events/replay_dlq.py` (entire file),
`services/kafka-consumer-events/kafka_consumer_events.py` (settings, identity,
Raw storage, failure envelopes, retry/publication/commit and main-loop paths),
`services/payment-producer/kafka_producer.py` (keys, event construction, delivery
tracker, configuration and send/flush paths),
`services/kafka-consumer-bronze/kafka_consumer_bronze.py` (Redis cache and commit
paths), `platform/kafka/topics.yaml`, `platform/kafka/topic_admin.py`,
`platform/kafka/production.env.example`, `platform/kafka/consumer-lag.sh`,
`platform/docker/dockerfiles/Dockerfile.payment-consumer-events`,
`docker-compose.yaml`, `README.md`, `services/shared/data_protection.py`,
`platform/config/governance/data_classification_part2.yaml`,
`tests/test_kafka_architecture.py`, `tests/test_red_kafka_fixes.py`,
`tests/test_kafka_logging_privacy.py`; Gate 3 documents 04, 05, 06 and 08;
Gate 2 evidence 02, 06, 08 and 10. Repository-wide searches also covered replay
identities/status, DLQ/retry, duplicate/idempotency and offset-reset utilities.
Search hits for analytics duplicate detection, generator retry attributes and
service-startup retries are not replay execution controls.

## 1. Existing architecture and retention

The payment producer reads XML/JSON, selects business keys separately from
event IDs, and publishes business/technical events. Event construction uses
UUID event IDs; business/message/correlation keys are not proof of unique
execution. `DeliveryTracker` counts callbacks; flush/outstanding messages and
delivery failures cause a non-zero result. Producer defaults include
`acks=all` and enabled idempotence. These protect producer-session delivery,
not later manual source reruns.

The Compose payment consumer subscribes to 24 domain topics using group
`payment-events-consumer`. It archives Raw Avro at deterministic coordinate
paths or publishes a terminal failure envelope to DLQ before committing.
Transient processing retries occur inline with bounded exponential backoff.
`payment-events.retry` records failure envelopes; it is not an implemented
delayed-retry worker. Retry publication's boolean result is ignored by
`process_message`; a raised publication exception can stop processing.
No dedicated retry-topic consumer was found.

| Declaration (`platform/kafka/topics.yaml`) | Partitions / replication | Cleanup | Retention |
| --- | --- | --- | --- |
| payment-events.retry | 8 / 1 | delete | 604800000 ms (7 days) |
| payment-events.dlq | 8 / 1 | delete | 2592000000 ms (30 days) |
| wendi.camt052 | 4 / 1 | delete | 86400000 ms (1 day) |
| wendi.camt053; agent.transactions | 4 / 1; 8 / 1 | delete | 30 days |
| Other declared domain topics | 4, 6 or 8 / 1 | delete | 7 days |

`topic_admin.py` applies/verifies partitions and replication and configures
retention/cleanup; replication can be overridden by environment. Document 06
records an earlier effective-settings match and seven-day broker offset
retention. These are historical observations, not current runtime checks.
Compacted internal `_schemas` and `__consumer_offsets` are not replay ledgers;
reported inherited retention does not mean time deletion on compact-only topics.
Expiry can remove unarchived events during a prolonged consumer outage.

`replay_dlq.py` is packaged in the consumer image and invoked manually, as shown
in README. It accepts required `--partition`, `--offset`, `--reason`, optional
`--execute` and `--allow-duplicate-replay`. Without execute it still connects to
Kafka, fetches a DLQ record and scans the topic. It is inspection mode, not an
offline dry run. Execution decodes the restricted original key/payload and
publishes to `envelope['original_topic']` at a new offset. No alternate target
resolution/allowlist or independently approved routing is implemented.

## 2. Existing controls, evidence and exact gaps

| Control / path and relevant code | Classification | Current behaviour and gap |
| --- | --- | --- |
| Consumer `deterministic_failure_id`; RED identity tests | IMPLEMENTED_AND_TESTED | Topic:partition:offset identity, DLQ key and x-failure-id are deterministic. Physical duplicates remain possible on delete topics. |
| Consumer `deterministic_s3_key`, `store_event_to_s3`; architecture tests | IMPLEMENTED_AND_TESTED | Coordinate/date path and head-object check skip an existing Raw object; stable timestamp fallback exists. Does not verify existing content or suppress republication at new offsets. |
| Consumer `store_then_commit`, `publish_failure`, `publish_dlq_then_commit`; architecture/RED tests | IMPLEMENTED_AND_TESTED | Auto commit disabled; synchronous commit follows Raw write/existence or callback-called, error-free DLQ flush. Failed final DLQ publication raises fail-stop before later offsets. Not an atomic Kafka/storage transaction. |
| Consumer `retry_delay`, `should_send_to_dlq`; architecture tests; Compose retries default 3, delays 1/2/30 | IMPLEMENTED_AND_TESTED | Bounded inline retries; permanent failures go immediately to DLQ. Retry topic publication is audit-oriented and not guaranteed when boolean result is false. |
| Producer `DeliveryTracker`, produce/flush; architecture callback tests | IMPLEMENTED_AND_TESTED | Delivery success/failure counted rather than queue acceptance. No durable per-event replay outcome/history. |
| Producer idempotence / acks settings | IMPLEMENTED_NOT_RUNTIME_VERIFIED | Implemented configuration; no cross-run semantic deduplication or exactly-once claim. |
| Replay `failure_id_from_envelope`, `group_by_failure_id`, `duplicate_groups`; RED grouping/legacy/parser tests | PARTIAL | Tested helpers identify multiple envelopes, including legacy identity reconstruction. They do not identify a prior successful replay of a single envelope. Module/README idempotency language overstates this guarantee. |
| Replay `scan_dlq_envelopes` | PARTIAL | Twenty-second scan returns accumulated records without completeness evidence; metadata errors return empty lists; malformed records and poll errors are skipped. No captured watermark boundary; EOF reporting is not explicitly enabled. Unknown/incomplete scans can look duplicate-free. |
| Replay exact DLQ lookup and parser | PARTIAL | Offset checked after manual assignment; no complete coordinate schema validation, nonnegative validation, cross-check of supplied failure ID against original coordinates, or record schema/target validation. |
| Replay delivery callback and `flush(30)` | PARTIAL | Remaining messages/error checked, but no explicit callback-called success flag, durable result or target coordinate capture. Missing acknowledgement cannot be accepted solely from initially null error. |
| Replay no auto commit / no explicit commit/reset | IMPLEMENTED_NOT_RUNTIME_VERIFIED | Inspector consumers disable auto commit; no offset-reset code in utility. This does not authorise inspection consumption or group changes in this phase. Lag shell script describes groups only. |
| Consumer `log_processing_error`; logging privacy tests | IMPLEMENTED_AND_TESTED | Allows topic/partition/offset/retry/destination and known error class; excludes payload/key/business ID and arbitrary exception text/traceback. Document 08 records 7 privacy + 13 protection tests and activated startup observations, not a live poison-record test here. |
| Replay summary/error printing | PARTIAL | Omits full encoded payload but prints business_key, original_event_id and failure_reason; prints arbitrary scan/delivery exception text. Protected values can escape even in inspection. Consumer fix does not fix this utility. |
| Producer `DeliveryTracker.callback` / producer failure logs | PARTIAL | Logs business_key, event_id and arbitrary error text. Cannot reuse as a privacy-safe replay audit sink without separate approved remediation. |
| Restricted consumer `failure_envelope` / classification YAML | DOCUMENTED_ONLY | Recoverable payload/key, business key, exception text and correlation/trace IDs are deliberately retained in restricted failure envelopes. Classification forbids protected logs; topic-specific Kafka ACL enforcement was not established by these sources. Trino RBAC evidence is not Kafka ACL evidence. |
| Bronze `IdempotencyCache` | PARTIAL | Optional Redis event-ID exists/setex with 604800-second TTL; unavailable Redis disables checks; check/write is non-atomic. Writes then marks then commits. Source exists but service activation was not established, and cache is neither durable replay ledger nor permanent suppression. |
| Replay headers | PARTIAL | x-dlq-replay, free-text reason, timestamp, failure ID; no request/batch identity, authenticated operator, approvals, attempt chain or persisted status. |
| Durable execution ledger / prior-success suppression / concurrency guard | NOT_IMPLEMENTED | No durable replay-request schema/store, unique execution reservation, persisted success lookup or transaction history found. Retained failure envelopes do not prove execution. |
| Policy/hold/disposal suppression and replay approvals | DOCUMENTED_ONLY | Gate 3 document 04 requires named authority, hold/suppression checks and separate reset authority. Current utility enforces none; execute flag and duplicate override are not approvals. |
| Offline replay decision dry run | NOT_IMPLEMENTED | Existing inspection consumes Kafka; lifecycle demo is a different frozen control and must not be adapted here. |
| Single replica / local transport defaults | POC_LIMITATION | Manifest replication 1 and default PLAINTEXT do not provide production HA/IAM/transport guarantees. Production env example is advisory, not activated proof. |

## 3. Current auditability and frozen controls

The envelope captures original topic/partition/offset/event ID, failure ID,
retry count, first/last failure timestamps and correlation/trace references.
Replay adds reason/timestamp/failure-ID headers to the target record and prints
delivery status. This is partial provenance, not a durable executed-replay
ledger. A retained DLQ record is a failure record on an expiring delete topic;
the utility's claim of an immutable audit record is not an audit-retention
guarantee. Repeated execution of one remaining envelope is not prevented.

Gate 2 evidence 10 records exactly one authorised Agent source replay (151
delivered, zero failed) and prohibits repeating it to recreate evidence.
Evidence 06 preserves controlled Raw/Bronze fidelity while excluding protected
PLM identity from ordinary analytics; evidence 02/08 preserves privacy and
least privilege. This contract adds no access or identity-export entitlement.
Document 08 is the later consumer logging activation record; older defect
observations in 04/06 remain historical and must not be rewritten.
Frozen data-state baseline:
`ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`.
No replay may modify that state without separately reopening its authority.

## 4. Proposed metadata-only durable ledger contract

This is a design requirement, not implemented storage or execution authority.

| Field | Required interpretation / validation |
| --- | --- |
| replay_batch_id | Required opaque batch identity; never a business identifier. |
| replay_request_id | Required stable opaque request identity reused for retries, not regenerated to bypass suppression. |
| source_topic, source_partition, source_offset | Required allowlisted original Kafka coordinate; partition/offset are nonnegative integers. Also persist separate dlq_topic/dlq_partition/dlq_offset for the selected failure envelope. |
| source_event_id or approved non-sensitive event reference | Optional only after documented sensitivity review; never assume event/message/correlation IDs are safe. Omit if unapproved; coordinates remain authoritative. |
| target_topic | Required independently resolved, policy-allowlisted route; frozen-state impacts reviewed. |
| requested_at, started_at, completed_at | UTC timestamps; requested required, others populated on respective transitions; no invented completion time before confirmation. |
| replay_status | Closed state enum below; unknown values rejected. |
| replay_reason_code | Required allowlisted operational reason code; no free-text payment/identity content. |
| requested_by_service_or_role | Required verified actor/service/role from trusted authorisation context, not self-asserted CLI text. |
| attempt_number | Positive integer; increments under atomic persistence for explicitly permitted retries. |
| previous_replay_reference | Nullable durable link to preceding attempt; no payload. |
| result_code | Closed sanitised outcome code; no arbitrary exception messages. |
| approval_reference, policy_version/reference | Non-sensitive durable evidence references; authority, scope, validity and policy resolution must be verifiable. |
| delivery_acknowledged, target_partition, target_offset | Future explicit delivery evidence; metadata only, populated from confirmed acknowledgement. |

Never store raw or encoded payload, identity-bearing Kafka keys, beneficiary
IDs/tokens, NIN, accounts, wallets, sensitive business/payment IDs or protected
exception text in ledger/logs. No beneficiary token is justified for this
contract. Enforce an allowlisted schema, bounded strings and enumerated reasons/
results; unknown fields are rejected, not silently persisted. Restricted
payload access remains separately controlled and never passes through ledger
records. Approval evidence cannot embed protected payload either.

Replay identity is a stable request bound immutably to original coordinates,
target and policy-approved operation. Enforce unique request identity and an
additional source-coordinate/target operation guard so new request/batch IDs
cannot bypass a prior successful replay. Batch ID alone is not deduplication.
Topic namespace/cluster identity must be resolved before cross-cluster support.
Any exceptional repeat is a distinct, explicitly authorised policy operation,
not the existing duplicate override flag.

Durability requires a transactional persistent store, atomic reservation and
unique constraints across concurrent callers; an in-memory set, duplicate
scan, log line or TTL cache is insufficient. Persist each transition with
append-only audit history and controlled writes. Define independent approved
audit retention, holds, backups/restore and access permissions before live use;
numeric periods/store technology are unresolved and must not be invented.

## 5. Replay states and fail-closed rules

| State | Meaning / allowed progression |
| --- | --- |
| REQUESTED | Valid metadata durably registered; no execution. To APPROVED, BLOCKED_POLICY or SUPPRESSED_DUPLICATE. |
| APPROVED | Required approvals and policy verified and durably recorded; no execution. To IN_PROGRESS only after fresh checks and atomic reservation; otherwise BLOCKED_POLICY or SUPPRESSED_DUPLICATE. |
| IN_PROGRESS | Durable attempt reservation exists; future executor only under separate authority. To SUCCEEDED or FAILED. |
| SUCCEEDED | Explicit broker delivery acknowledgement and durable outcome confirmed. Terminal; repeated identity is suppressed. Does not assert downstream processing completed. |
| FAILED | Sanitised known failure or delivery/persistence outcome uncertainty. No automatic resend; reconcile uncertain delivery first. Any permitted next attempt has a linked durable history and renewed checks. |
| SUPPRESSED_DUPLICATE | Prior success or duplicate execution identity prevented execution. Terminal for this attempt. |
| BLOCKED_POLICY | Preconditions unresolved/denied; no publication. Remediation requires a newly validated decision and preserved history. |

Block before publication when request or batch identity is missing; source
coordinates are incomplete/invalid/inconsistent; prior success exists for the
same replay identity or guarded operation; required approval is absent,
untrusted, expired or out of scope; target is unresolved/unapproved; policy,
hold, disposal suppression or frozen-state authority is unresolved; logging
could expose protected payload data; or durable ledger persistence/readability
is unavailable. Also block concurrent/in-progress reservations, incomplete
inspection, unapproved identifier fields and missing explicit delivery evidence.
If ledger is unavailable, return a sanitised blocked result without claiming
that a block was durably recorded. Never fall back to publication or logs as
the ledger.

Kafka acknowledgement and external ledger writes are not an atomic transaction.
A crash after publication but before success persistence leaves an uncertain
IN_PROGRESS/FAILED outcome. Fail closed, preserve reservation and reconcile;
do not automatically replay or claim exactly-once delivery. A future live
design must resolve this crash window and downstream duplicate semantics before
acceptance. Offset reset and consumer-group administration remain separate
explicit operations, never consequences of approval or ledger recovery.

**DRY RUN != REPLAY.** Offline metadata decisions publish/consume nothing and
do not persist an executed success. Kafka inspection is not offline dry run.

**APPROVAL != EXECUTION.** Approval grants scoped eligibility for a separately
authorised executor; it neither invokes it nor weakens freeze restrictions.

**DUPLICATE SCAN != DURABLE LEDGER.** Envelope multiplicity is not persisted
execution history, especially after expiry or an incomplete scan.

**DLQ RETENTION != REPLAY AUDIT RETENTION.** Audit retention is independently
approved and must outlive relevant investigation/suppression obligations.

## 6. Future boundary and acceptance checklist

Gate 3.6B should first implement the metadata-only ledger and fail-closed
decision/state logic in isolation, with transactional local persistence and no
live Kafka replay or infrastructure clients. Gate 3.6C may perform synthetic/
offline tests only. Any live replay demonstration requires separate explicit
authorisation and is outside this task.

- Validate required metadata, closed enums, topic resolution and identifier
  sensitivity; reject protected/unknown fields and untrusted approval context.
- Demonstrate durable restart recovery, immutable request binding, atomic
  concurrent reservation, attempt links and prior-success suppression even
  when callers change batch/request IDs or DLQ envelopes expire.
- Cover all seven states and every fail-closed rule, persistence outages and
  crash/uncertain-delivery recovery without automatic resend.
- Prove zero network connections, zero Kafka production/consumption, zero
  offsets/group changes and zero infrastructure mutations in offline phases.
- Capture synthetic privacy markers to prove they cannot reach ledger/logs;
  test reason/result allowlists and no arbitrary exception text.
- Before any separately authorised live phase, remediate utility/producer
  logging under a new scope, validate complete bounded inspection, explicit
  acknowledgement, approved audit retention/RBAC/recovery and downstream
  reconciliation. Do not alter the frozen consumer or Gate 3.5 here.

The existing targeted tests cover consumer invariants and grouping helpers;
they do not cover a durable ledger, replay-main privacy/completeness, concurrent
execution or acknowledgement/persistence crash recovery. These remain acceptance
blockers rather than POC waivers. Production follow-up includes HA persistence,
Kafka ACLs/transport, monitored lag/expiry, independently governed audit
retention and tested ledger restore; no live guarantee is asserted by discovery.
