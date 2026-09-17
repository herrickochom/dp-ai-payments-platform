# Gate 3.7 — Operational Audit Persistence

## Status

**CLOSED / PASS**

Gate 3.7 validates operational audit persistence for the local POC runtime.

The control demonstrates that the Agent API audit ledger is stored on a
persistent Docker named volume, survives controlled Agent API container
replacement, and remains appendable after replacement.

This is a local POC persistence control and must not be interpreted as
production-grade immutable or disaster-recovery audit storage.

---

## 3.7A — Audit Discovery

Status: **PASS**

The Agent API uses a central JSONL audit store.

The original default audit location was ephemeral container storage.

The persistence requirement was therefore to move the runtime audit ledger
onto storage whose lifecycle is independent of the Agent API container.

---

## 3.7B — Implementation Inspection

Status: **PASS**

The central audit implementation was inspected before runtime activation.

Audit writes use the central JsonlAuditStore.

The store creates its parent directory as required, appends JSONL records,
flushes writes, and performs fsync.

The control was addressed at the central store and runtime configuration
boundary rather than by introducing multiple audit writers.

---

## 3.7C — Persistent Runtime Configuration

Status: **PASS**

Runtime audit path:

    /var/lib/dp-agent/audit/audit.jsonl

Persistent Docker named volume:

    dp-ai-payments-platform_agent-audit-data

Container mount:

    /var/lib/dp-agent/audit

The volume is mounted read/write and has a lifecycle independent of an
individual Agent API container instance.

---

## 3.7D — Static Persistence and Privacy Validation

Status: **PASS**

Agent Phase 9 tests:

    21/21 PASS

Data-protection tests:

    13/13 PASS

Kafka logging-privacy tests:

    7/7 PASS

A dedicated persistence test verified that the JsonlAuditStore can be
reopened against the same persistent filesystem and append subsequent audit
records without losing the existing ledger.

This test validates store reopen and append behaviour. By itself it does not
prove persistence across Docker container replacement; that was validated
separately in Gate 3.7E.

---

## 3.7E — Runtime Activation

Status: **PASS**

The Agent API was activated with the persistent audit volume.

Final runtime state:

    Agent status: running
    Agent health: healthy
    Agent restart count: 0

    Trino status: running
    Trino health: healthy

The Agent uses the configured persistent audit path.

---

## Corrected Runtime Persistence Proof

Status: **PASS**

A clean runtime baseline was established before the proof:

    BASELINE_AUDIT_RECORD_COUNT=0
    CLEAN_BASELINE=PASS
    AUDIT_VOLUME_PRE=PASS

### Record 1

One synthetic metadata-only audit record was written through the real
JsonlAuditStore.

Results:

    SYNTHETIC_AUDIT_WRITE_1=PASS
    AUDIT_WRITE_1_RC=0
    RECORD_COUNT=1
    REQUEST1_MATCHES=1
    REQUEST1_METADATA_ONLY=PASS

Record fingerprint before container replacement:

    6dbaea190bec46e674ea4a1372ba3df58a45c12d287077c9ecfe36f6b0868afb

Audit-file SHA256 before container replacement:

    ed1332a2ae2051cf2cc83719af66fe3bb7b0af23766abd6cf2944082f99196c5

### Controlled Agent API Container Replacement

Exactly one controlled Agent API container recreation was performed after
record 1 had been durably written.

Result:

    AGENT_RECREATE_RC=0

The Agent returned to healthy state:

    AGENT_POST_RECREATE_HEALTH=PASS

No Trino restart or other platform recreation was required.

### Persistence Verification

After Agent API container recreation:

    RECORD_COUNT_AFTER_RECREATE=1
    REQUEST1_MATCHES_AFTER_RECREATE=1
    REQUEST1_SURVIVED_RECREATION=PASS
    REQUEST1_METADATA_ONLY_AFTER=PASS

Record 1 fingerprint after recreation:

    6dbaea190bec46e674ea4a1372ba3df58a45c12d287077c9ecfe36f6b0868afb

The before and after fingerprints matched:

    REQUEST1_FINGERPRINT_STABLE=PASS

Audit-file SHA256 after recreation:

    ed1332a2ae2051cf2cc83719af66fe3bb7b0af23766abd6cf2944082f99196c5

The complete audit file therefore remained byte-for-byte unchanged across
the controlled container replacement:

    AUDIT_FILE_SURVIVED_BYTE_FOR_BYTE=PASS

### Append After Container Replacement

A second synthetic metadata-only audit record was written after the Agent
API container replacement.

Results:

    SYNTHETIC_AUDIT_WRITE_2=PASS
    AUDIT_WRITE_2_RC=0

Final ledger validation:

    FINAL_AUDIT_RECORD_COUNT=2
    REQUEST1_FINAL_MATCHES=1
    REQUEST2_FINAL_MATCHES=1
    PROTECTED_PAYLOAD_FIELD_RECORDS=0
    APPEND_AFTER_RECREATION=PASS
    METADATA_ONLY_LEDGER=PASS
    FINAL_LEDGER_VERIFY_RC=0

The persistent volume remained available:

    AUDIT_VOLUME_FINAL=PASS

Final infrastructure state:

    AGENT_STATUS=running
    AGENT_RESTARTS=0
    AGENT_HEALTH=healthy

    TRINO_STATUS=running
    TRINO_HEALTH=healthy

---

## Safety Evidence

The corrected proof recorded:

    SYNTHETIC_AUDIT_RECORDS_WRITTEN=2
    AUDIT_CONTENT_PRINTED=0
    CONTROLLED_AGENT_RECREATIONS=1
    VOLUME_DELETED=0
    TRINO_RESTARTED=0
    RBAC_CHANGED=0
    KAFKA_REPLAY=0
    OFFSET_RESET=0
    DBT_RUN=0
    ML_RUN=0

Final result:

    GATE_3_7E_AUDIT_PERSISTENCE_PROOF=PASS

---

## Test-Harness Incident

An earlier persistence-proof attempt produced a false-positive result.

The proof used Python heredocs with `docker exec` without the `-i` option.
The heredoc content was therefore not delivered to `python -`.

The commands returned successfully without executing the intended Python
audit operations.

Consequently:

- no synthetic audit record was written;
- the audit file did not exist;
- the before and after SHA256 variables were empty;
- comparison of the two empty variables incorrectly produced a PASS result.

This was a **test-harness defect**, not an Agent API persistence defect.

A subsequent read-only reconciliation established:

    AUDIT_FILE_EXISTS=no
    AUDIT_FILE_BYTES=0
    AUDIT_RECORD_COUNT=0
    DOCKER_EXEC_STDIN_DELIVERY=PASS
    STDIN_TEST_RC=0

The reconciliation therefore established a clean baseline.

The corrected proof used:

    docker exec -i

and included explicit record-count, request-match, fingerprint, file-existence
and SHA256 verification.

The corrected proof is the accepted Gate 3.7E evidence.

The earlier false-positive result is not accepted as evidence.

---

## Acceptance

Gate 3.7 demonstrates that:

1. the operational audit ledger is stored outside the Agent API container;
2. the persistent Docker named volume remains available across container
   replacement;
3. an existing audit record survives controlled Agent API replacement;
4. the surviving record remains unchanged;
5. the audit file remains byte-for-byte unchanged during replacement;
6. new audit records can be appended after replacement;
7. the validated synthetic ledger contains metadata-only records and no
   protected payload fields.

Therefore:

**GATE 3.7 — OPERATIONAL AUDIT PERSISTENCE: CLOSED / PASS**

---

## POC Limitation

This evidence demonstrates persistence across controlled Agent API container
replacement using a Docker named volume.

It does **not** demonstrate:

- Docker-host-loss recovery;
- external immutable or WORM audit storage;
- multi-node production durability;
- approved regulatory audit-retention duration;
- backup and restore of the audit volume;
- geographically redundant audit storage.

Those capabilities require separate production and recovery controls and
must not be inferred from this POC acceptance result.
