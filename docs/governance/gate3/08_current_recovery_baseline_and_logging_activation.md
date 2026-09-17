# Gate 3.4 current recovery baseline and logging activation

Date: 2026-09-16. Result: BLOCKED at new-tag creation. No backup or runtime activation proceeded. No commit.

## Preflight

Gate 3.4A verified every running project container and relevant host processes quiescent. This attempt reconfirmed Git HEAD `3284e2649e9a51cf25c4f6b89a0c2207f5333833`, authorised working-tree scope, frozen Nessie main, healthy running MinIO/Nessie, and successful process inspection across all eight running containers with no prohibited commands found. No unresolved jobs were identified. No resolved Compose configuration or secret values were displayed.

## New recovery tag attempt

Intended tag: `gate3-recovery-20260916T200118Z`.
Intended and observed main hash: `ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`.

The intended name/hash/main were printed before sending POST `/api/v2/trees?source-ref=main%40<hash>` with JSON type TAG, name and hash. The API returned HTTP 400 Bad Request. The response body was not captured, so the exact validation reason is UNVERIFIED; this report does not attribute the failure to unavailable data or permissions. A prior read-only `/q/openapi` schema probe returned an HTTP error and did not establish the create-reference schema.

Subsequent read-only reference enumeration returned only main at the frozen hash, hasMore=false. The attempted tag does not exist. Tag verification did not succeed. No retry mutation was issued; execution stopped before backup/runtime activation as required by the fail-closed contract.

No GATE 3 PROTECTED RECOVERY BASELINE can be declared. The intended name is an unsuccessful request record, not a protected recovery reference.

## Backup and activation

Backup bucket/prefix: NOT CREATED.
Expected/copied object counts and byte counts: NOT ESTABLISHED in this attempt.
Path/size/ETag/SHA256 verification: NOT PERFORMED.
Recovery manifest/hash: NOT CREATED.
Logging status: IMPLEMENTED_AND_TESTED; runtime activation outstanding. No build, recreate or restart occurred. Post-activation focused tests were not run because activation was blocked; prior evidence remains 35 focused Kafka and 13 privacy tests, 48 total passed in Gate 3.3.

## Historical truth and remaining blockers

The historical pre-Gate2 commit remains unavailable by the Gate 3.3 explicit lookup. Historical tag `pre-gate2-20260915T155541Z` was not recreated. Historical Gate 2 backup availability remains unavailable/inconclusive. Historical verification evidence remains valid as a record of the verification at that time. This failed new current-state recovery attempt does not restore historical recovery controls.

Required next work is read-only reconciliation of the installed Nessie create-reference request schema before any separately continued tag attempt. New tag/physical backup/integrity manifest and logging activation remain outstanding, along with previously documented lifecycle-policy/planner/replay/audit/restore/ML provenance requirements.

## Safety and change scope

Only this Gate 3 report and factual addenda in documents 05/07 were written in this attempt. Existing authorised consumer/test changes and earlier Gate 3 documents remain uncommitted. No historical tag/backup mutation, main movement, Kafka replay/mutation/offset reset, dbt execution, Iceberg expiry/orphan deletion/GC, token materialisation, ML execution, unrelated restart or Gate 2 evidence change occurred. The attempted POST did not create a reference according to complete before/after inventories. No MinIO mutation or backup creation/deletion occurred.

## Gate 3.4B read-only API diagnosis

Result: TAG CREATION REQUEST PROVEN against the installed API contract; execution NOT PERFORMED. This addendum does not replace the failed-attempt record above or declare a recovery baseline.

### Running version and API

Docker image: `dp-ai-payments-platform-nessie`; image ID `sha256:555d3f7315ec3629344b008a295aa9f2c5ed226eb4de36eb6bb6c20ca321d164`. Installed `/deployments/app/nessie-quarkus-0.108.4.jar` and model/rest JAR filenames establish release 0.108.4, matching the Dockerfile base `ghcr.io/projectnessie/nessie:0.108.4`. Embedded API specification info.version is also 0.108.4. No version label was available; the image revision label `ubi9` is not a Nessie release version.

GET `/api/v2/config` reports minSupportedApiVersion=1, maxSupportedApiVersion=2, actualApiVersion=2, specVersion=2.2.0; GET `/api/v1/config` also succeeds. The v2 base is `http://localhost:19120/api/v2`. Spec version 2.2.0 is not server release 0.108.4. Only safe API/version fields were printed. Automatic review rejected an initial unfiltered config-output query; the safe allowlisted query succeeded.

### Installed contract proof

HTTP discovery `/q/openapi`, `/openapi`, `/openapi.json`, `/api/v2/openapi.json`, `/swagger.json` and `/api/v2/swagger.json` returned 404. Instead, the installed `/deployments/lib/main/org.projectnessie.nessie.nessie-model-0.108.4.jar` was read via `docker cp ... -` into an in-memory tar/ZIP stream. No files were extracted. Its `META-INF/openapi/openapi.json` contains the exact v2 contract: POST `/v2/trees`, operationId `createReferenceV2`, required query `name` and `type`, required JSON source `Reference` body. The server's API prefix makes the live path `/api/v2/trees`. Installed TreeApi constants confirm type validation accepts TAG/tag or BRANCH/branch.

The query name/type determine the NEW reference. The body identifies its source commit/reference. The body discriminator uses BRANCH, TAG or DETACHED; an explicit source hash selects that commit, while omitted hash uses source HEAD. Therefore a source body describing main as BRANCH does not create a branch when query type=TAG. Creation does not assign/move main. Contract lists HTTP 409 for an existing name, 401 for invalid credentials and 403 for denied creation. No expectedHash query or If-Match header is required by this creation contract; explicit source hash pins the intended commit. Permissions and execution success have not been tested by mutation.

### Failed shape and diagnosis

Failed request: POST `/api/v2/trees?source-ref=main%40ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`, header Content-Type application/json, body `{"type":"TAG","name":"gate3-recovery-20260916T200118Z","hash":"ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba"}`. No authentication credentials were present in the recorded request. It was not resent.

PROVEN contract violations: required name/type query parameters omitted; source-ref is not a declared v2 create parameter; body describes the intended destination instead of an existing source reference. Endpoint and API version were correct, TAG discriminator and hash syntax valid, and the attempted name meets the installed naming rule. Missing required queries is a supported explanation for HTTP 400. Exact original server validation message/cause remains unproven because the response body was not captured. Do not claim permission, unresolved current hash or invalid name as the cause.

Repository search found no known-good exact-version tag request or pynessie/CLI recovery implementation. `platform/entrypoints/nessie-docker-entrypoint.sh` contains a legacy branch-create attempt with `ref`/type body, not proof of a correct v2 create contract. Historical evidence records tag availability, not a reproducible request shape.

### One proposed request: NOT EXECUTED

```http
POST /api/v2/trees?name=gate3-recovery-20260916T200450Z&type=TAG HTTP/1.1
Host: localhost:19120
Content-Type: application/json
Accept: application/json

{"type":"BRANCH","name":"main","hash":"ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba"}
```

Proposed new name passes the installed Reference name regex via full matching, starts with a letter, uses allowed characters, has no double dot/trailing dot/slash, is not HEAD/DETACHED or a hash representation, and was absent from complete current reference enumeration. Target resolution is proven by GET `/api/v2/trees/main@<exact hash>/history?max-records=1`, whose first commit hash equals the exact supplied current baseline. Current main remains that hash.

Before any later authorised execution, recheck main, source resolution and name availability; do not fall back to a different hash or reinterpret a conflict as permission to move an existing tag. After creation verify tag type/hash and unchanged main before backup. This phase sends GETs only; no tag/branch, backup, runtime activation, application-code change, Gate 2 evidence change or commit occurred. Only this document was updated.

## Gate 3.4C — Current-state Nessie recovery anchor

Status: PASS

A new current-state recovery tag was verified:

- Tag: `gate3-recovery-20260916T200450Z`
- Type: `TAG`
- Target: `ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`
- Nessie `main` remained at the same frozen commit.
- Git HEAD remained `3284e2649e9a51cf25c4f6b89a0c2207f5333833`.

The historical pre-Gate2 tag was not recreated or moved.

## Gate 3.4D — Current-state physical recovery baseline

Status: PASS

A new physical backup was created independently of the unavailable historical
Gate2 backup:

- Bucket: `dp-ai-payment-gate3-backup`
- Root: `gate3-current-state/20260916T201500Z/`
- Protected prefixes: `raw/` and `warehouse/`
- Source objects: 29,279
- Backup objects: 29,279
- Source bytes: 129,431,489
- Backup bytes: 129,431,489
- Missing paths: 0
- Extra paths: 0
- Size mismatches: 0
- ETag mismatches: 0
- ETags unavailable: 0
- Inventory parse errors: 0

Verification results:

- Path and size verification: PASS
- ETag verification: PASS
- Object integrity verification: PASS
- Source metadata manifest SHA256:
  `97dac8a1255328c202a104b90287e3988237cdb76f7a45a965de973106d7b33c`
- Backup metadata manifest SHA256:
  `97dac8a1255328c202a104b90287e3988237cdb76f7a45a965de973106d7b33c`

The matching manifest digests cover the ordered object path, size and ETag
metadata for the complete 29,279-object recovery set.

A full per-object payload SHA256 verification was started but deliberately
stopped because the initial implementation required approximately 58,558
separate `docker exec`/`mc cat` operations. No source or backup object was
modified by that read-only attempt.

Accordingly, Gate 3 records this control as full path/size/ETag verification
with matching metadata-manifest SHA256, not as full independent payload SHA256
verification.

No source or backup objects were deleted. No Iceberg maintenance, Kafka replay,
offset reset, dbt execution or ML execution was performed during recovery
baseline creation.

## Gate 3.4E — Payment consumer logging privacy activation

Status: PASS

The metadata-only logging remediation was activated by rebuilding and
recreating only the `payment-consumer-events` service.

Runtime activation constraints:

- No Kafka replay was performed.
- No Kafka offset reset was performed.
- No producer was executed.
- No dbt transformation was executed.
- No ML job was executed.
- No other platform service was intentionally restarted.
- The recovery baseline remained protected throughout activation.

Final runtime state:

- Service: `payment-consumer-events`
- Status: running
- Health: healthy
- Restart count: 0
- Startup Avro OCF self-test: PASS
- Kafka subscription established for 24 configured topics.

Privacy verification:

- Focused Kafka logging privacy tests: 7/7 PASS.
- Data protection regression tests: 13/13 PASS.
- Recent runtime startup logs contained operational metadata and configuration
  information only; no payment payload, Kafka message key, beneficiary/business
  identifier, exception text or traceback was observed.

The remediation replaces failure-path logging of replay content and arbitrary
exception text with an allowlisted operational metadata record. Restricted DLQ
failure-envelope content remains available to the controlled DLQ processing
path and was not removed from recovery semantics.

Historical sensitive-log exposure remains unverified. Gate 3 therefore records
the current logging defect as remediated and runtime activated, without making
a claim about historical log contents.

The payment consumer is frozen at this checkpoint unless a genuine Gate 3
acceptance defect requires another controlled change.
