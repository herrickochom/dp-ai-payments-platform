# 10 - Incidents and Recovery

This file records material Gate 2 incidents rather than hiding them from
the final acceptance record.

## Recovery baselines

Git clean recovery baseline:

`500b95e9f15450d0e2f4de009c1eab463871d411`

Pre-Gate2 Nessie recovery hash:

`f85893b23c441db5abd0d2400da134cbb5fc4d0cb52a1e3c24e3ae02d85b35df`

Recovery tag:

`pre-gate2-20260915T155541Z`

Current pre-freeze Nessie main hash:

`ddd3b9b6bfd9db5b00ac6304d74124b249176ec0ddab1464adba8b8e69ee96ba`

## MinIO backups

Original Gate 2 backup:

bucket:
`dp-ai-payment-gate2-backup`

root:
`gate2-preflight/20260915T155602Z`

Validation:

- 17 affected locations
- 68 / 68 objects integrity verified

Additional pre-build backup:

`gate2-additional-prebuild/20260916T100622Z/`

Validation:

- 23 existing tables
- 92 / 92 objects verified
- 595,935 bytes
- path, size, ETag and full SHA256 verified

Backups were not mutated during subsequent remediation.

## AI Bronze accidental drop/restore

`br_pdm_ai_default_risk` was accidentally dropped during investigation
despite an existing Consumption dependency.

It was restored through the controlled dbt path.

This is recorded as an authorised destructive exception/incident and is not
hidden from the acceptance history.

The table is ML-file-derived and is not part of the Kafka-derived Bronze
lineage audit.

## Agent ingestion incident

`payment-consumer-events` lacked Agent transaction Raw data.

The generator produced 151 XML files.

The producer initially failed because the Avro `message_id` contract
required a string.

The producer was corrected to:

- recognise the generic XML input;
- provide/cast the message ID correctly;
- fail closed on file/delivery failure;
- preserve required business keys.

A controlled replay was executed exactly once:

- delivered: 151
- failed: 0

The consumer subsequently persisted the Raw Avro data.

Incident status: CLOSED.

The replay must not be repeated for Gate 2 evidence generation.

## Token-link materialisation attempts

Early materialisation attempts failed without mutation.

After correcting the tokeniser job contract and adding the bounded
materialisation entry point, the authorised run completed:

250 rows / 250 unique IDs / 250 unique tokens / 0 nulls.

The materialiser must not be rerun merely to reproduce evidence.

## ML Compose attempts

Two scorer attempts using only the ML profile failed Compose validation
because of the Trino service/profile dependency.

No scorer mutation occurred during those failed attempts.

The successful invocation used the required profiles.

## Cline incident

A Cline-assisted attempt partially modified source/token-link support and
encountered Nessie tag/API issues.

No successful Gate 2 model build resulted from that attempt.

The live main reference at Codex takeover was preserved and controlled
recovery continued from the known state.

A Trino rules file was accidentally replaced on disk during this work but
was restored before activation/restart. The controlled candidate policy was
later activated explicitly.

## Temporary evidence loss

Temporary evidence under `tmp/gate2-remediation` was accidentally cleared
and was not recoverable through Git.

Permanent evidence is therefore stored under:

`docs/evidence/gate2/`

## Broad dbt selector incident

A downstream selector expanded well beyond the intended geography scope.

The expanded builds exposed genuine stale contracts and the Gold payment
surrogate-key defect, but also caused large numbers of downstream skips.

The failures were investigated with bounded read-only diagnostics and
targeted remediation.

No further broad build is required for final Gate 2 acceptance.

## MinIO registry compatibility

Docker Hub availability for the MinIO images was unreliable in the local
environment.

The POC configuration was switched to the corresponding quay.io MinIO
images.

This is an infrastructure compatibility change and not a Gate 2 privacy
semantic change.
