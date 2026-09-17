# Gate 3.8 — Recovery and Restore Drill

## Status

**Gate 3.8: CLOSED / PASS WITH DOCUMENTED APPLICATION-RECOVERY LIMITATION**

Gate 3.8 demonstrates that the protected Gate 3 MinIO backup can be
restored into an isolated object-storage namespace and verified against
the protected source.

The drill does not claim that the current Iceberg/Nessie application
state can be queried directly from the isolated restored bucket.

A read-only inspection demonstrated that restored Iceberg metadata
retains absolute references to the live data bucket. Application-level
isolated catalogue recovery therefore requires an additional recovery
design and was deliberately not simulated by querying the live paths.

---

## 1. Protected Recovery Baseline

Protected Nessie recovery reference:

- Tag: `gate3-recovery-20260916T200450Z`
- Type: `TAG`
- Frozen hash:
  `ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`

The `main` branch and protected recovery tag were both verified at the
same frozen hash during the recovery drill.

Protected MinIO backup:

- Bucket: `dp-ai-payment-gate3-backup`
- Root: `gate3-current-state/20260916T201500Z`
- Objects: 29,279
- Bytes: 129,431,489
- Raw objects: 26,335
- Raw bytes: 101,024,167
- Warehouse objects: 2,944
- Warehouse bytes: 28,407,322

Protected metadata-manifest SHA256:

`97dac8a1255328c202a104b90287e3988237cdb76f7a45a965de973106d7b33c`

This hash represents the deterministic metadata manifest used for
path/size/ETag verification. It is not an independent SHA256 digest of
every object payload.

---

## 2. Recovery Preflight

The recovery preflight verified:

- the protected Nessie reference;
- the current protected backup inventory;
- Raw and Warehouse component reconciliation;
- separation of source and restore target;
- absence of the restore target before authorised creation;
- prohibition on restoring over the live data bucket.

### Harness defects encountered and reconciled

Three test-harness issues were encountered during preflight.

1. The initial Nessie parser expected `hash` and `type` at the root of
   the Nessie v2 response. The actual response exposes these values
   inside the top-level `reference` object. The parser was corrected and
   the protected branch and tag were verified successfully.

2. The `local` MinIO client alias failed to list the MinIO namespace even
   though it reported the expected endpoint. The existing `gate3` alias
   successfully accessed the same MinIO service and was used for the
   recovery drill. This was classified as an alias/configuration issue,
   not evidence of data loss.

3. A shell reconciliation loop printed a false component-count failure
   because dynamically named shell variables were echoed rather than
   assigned. A separate arithmetic reconciliation confirmed:

   - 29,279 total objects;
   - 129,431,489 total bytes;
   - zero recovery-data defects.

These were harness defects only. No protected recovery data was changed.

---

## 3. Mirror Dry-Run Prerequisite Discovery

An initial `mc mirror --dry-run` was attempted while the isolated target
bucket did not exist.

Result:

- mirror return code: 1;
- error: destination bucket did not exist;
- restore objects written: 0;
- target bucket created by dry run: 0;
- protected backup modified: 0;
- live objects modified: 0.

This established that the installed MinIO client requires the
destination bucket to exist before the mirror dry run can calculate the
restore operation.

This was a tooling prerequisite discovery, not a recovery-data failure.

---

## 4. Isolated Restore Target

The following target bucket was created explicitly for the drill:

`dp-ai-payment-gate3-restore-test`

Safety checks proved that it was distinct from:

- live bucket `dp-ai-payment`;
- protected backup bucket `dp-ai-payment-gate3-backup`.

Immediately after creation:

- objects: 0;
- bytes: 0;
- errors: 0.

Only target-bucket creation was authorised at this stage.

---

## 5. Successful Mirror Dry Run

With the empty isolated target present, the mirror dry run completed
successfully.

Results:

- source path guard: PASS;
- target path guard: PASS;
- protected-target guard: PASS;
- target objects before dry run: 0;
- target bytes before dry run: 0;
- mirror dry-run return code: 0;
- target objects after dry run: 0;
- target bytes after dry run: 0;
- restore writes: 0.

The exact restore direction was therefore validated before the real
copy:

`protected backup -> isolated restore-test`

---

## 6. Isolated Object Restore

The protected backup was mirrored into the isolated restore target.

No `--remove` or `--overwrite` option was used.

Restore result:

- mirror return code: 0;
- transferred: approximately 123.42 MiB;
- duration: approximately 27 seconds;
- restored objects: 29,279;
- restored bytes: 129,431,489;
- restore errors: 0.

The protected source inventory after the operation remained:

- objects: 29,279;
- bytes: 129,431,489;
- errors: 0.

The live bucket was never a restore target.

---

## 7. Restore Integrity Verification

The protected backup and isolated restore were compared across the
complete object inventory.

Results:

- source objects: 29,279;
- target objects: 29,279;
- source bytes: 129,431,489;
- target bytes: 129,431,489;
- missing paths: 0;
- extra paths: 0;
- size mismatches: 0;
- ETag mismatches: 0;
- missing source ETags: 0;
- missing target ETags: 0.

Source metadata-manifest SHA256:

`97dac8a1255328c202a104b90287e3988237cdb76f7a45a965de973106d7b33c`

Restored-target metadata-manifest SHA256:

`97dac8a1255328c202a104b90287e3988237cdb76f7a45a965de973106d7b33c`

Result:

**FULL PATH / SIZE / ETAG VERIFICATION: PASS**

**RESTORE METADATA INTEGRITY: PASS**

The SHA256 value is a hash of the deterministic metadata manifest. The
drill does not claim independent SHA256 verification of all 29,279
object payloads.

---

## 8. Restored Iceberg Warehouse Structure

The isolated restored Warehouse was inspected read-only.

Results:

- Warehouse objects: 2,944;
- Warehouse bytes: 28,407,322;
- metadata-component objects: 2,207;
- data-component objects: 737;
- Iceberg metadata JSON objects: 733;
- Iceberg Avro objects: 1,474;
- Parquet objects: 737;
- inventory errors: 0.

Result:

**ICEBERG STRUCTURAL EVIDENCE: PASS**

The protected Nessie `main` branch and Gate 3 recovery tag remained at
the frozen recovery hash following the restore.

---

## 9. Iceberg Location Safety Inspection

All 733 restored Iceberg `.metadata.json` files were inspected
read-only for storage-location fields.

Results:

- metadata files inspected: 733;
- location records: 733;
- parse errors: 0;
- metadata files without a location: 0;
- references to live data bucket: 733;
- references to isolated restore target: 0;
- references to protected backup bucket: 0;
- location scheme: `s3`;
- distinct authorities: 1.

Result:

**ISOLATED QUERY WITH UNMODIFIED RESTORED METADATA: UNSAFE**

Reason:

The restored Iceberg metadata retains absolute storage references to
the live `dp-ai-payment` bucket.

A query using the unmodified restored metadata could therefore resolve
data files from live storage rather than the isolated restore target.

Such a query could create a false recovery PASS and was deliberately
not performed.

No Trino query, dbt execution, Nessie reference mutation or Iceberg
metadata rewrite was performed as part of this test.

---

## 10. Recovery Capability Demonstrated

Gate 3.8 demonstrates:

1. the protected backup exists and is accessible;
2. the protected recovery inventory is internally reconciled;
3. restore source and target can be safely separated;
4. the backup can be copied into an isolated MinIO namespace;
5. all 29,279 objects can be restored;
6. restored aggregate object and byte counts match the protected source;
7. every restored relative path matches;
8. every restored object size matches;
9. every restored ETag matches;
10. source and restored metadata-manifest SHA256 values match;
11. the restored Warehouse contains the expected Iceberg physical
    structure;
12. the protected Nessie recovery anchor remains unchanged;
13. the recovery process detects unsafe live-storage references before
    application-level validation.

---

## 11. Explicit Limitation

Gate 3.8 does **not** demonstrate full isolated application/catalogue
recovery.

The current Iceberg metadata contains absolute object locations under
the live data bucket.

A complete application-level recovery design would therefore require a
safe method for restoring catalogue state and resolving Iceberg
locations inside an isolated storage environment without falling back
to live objects.

Possible future approaches must be evaluated separately. They may
include an isolated storage endpoint capable of presenting the restored
data under the original bucket namespace, or a controlled and validated
catalogue/metadata recovery mechanism.

No metadata rewrite is authorised by this Gate 3.8 drill.

---

## 12. Safety Record

During the recovery drill:

- the protected backup was not deleted or moved;
- the protected Nessie recovery tag was not moved or recreated;
- the live MinIO bucket was not a restore target;
- no MinIO object deletion was performed;
- no Kafka replay was performed;
- no Kafka offset reset was performed;
- no dbt build was performed;
- no ML scoring or retraining was performed;
- no token-link materialisation was performed;
- no Trino recovery query was performed;
- no Nessie recovery reference was created or modified;
- no Iceberg metadata was rewritten.

The only intentional recovery mutations were:

1. creation of the isolated restore-test bucket;
2. copying the protected backup into that isolated bucket.

---

## 13. Acceptance Decision

### Object-level recovery

**PASS**

The protected Gate 3 backup was successfully restored and verified in
an isolated object-storage namespace.

### Restored Iceberg physical structure

**PASS**

Expected Iceberg metadata, manifest and Parquet structures are present
in the isolated restored Warehouse.

### Isolated application/catalogue recovery

**NOT DEMONSTRATED**

This is an explicitly documented POC recovery limitation rather than a
failed object restore.

### Overall Gate 3.8 status

**CLOSED / PASS WITH DOCUMENTED APPLICATION-RECOVERY LIMITATION**

The drill provides evidence that the current backup is restorable at
the object-storage layer and that unsafe catalogue/location behaviour
is detected before an application-level recovery test is attempted.

---

## 14. Machine-readable Acceptance Values

The following values support deterministic acceptance verification.

GATE38_RESTORED_OBJECTS=29279
GATE38_RESTORED_BYTES=129431489
GATE38_WAREHOUSE_OBJECTS=2944
GATE38_WAREHOUSE_BYTES=28407322
GATE38_ICEBERG_METADATA_JSON=733
GATE38_METADATA_MANIFEST_SHA256=97dac8a1255328c202a104b90287e3988237cdb76f7a45a965de973106d7b33c

These values duplicate the human-readable recovery evidence above; no recovery operation was rerun.
