# 04 - Transformation Builds

## Controlled Gate 2 build

The controlled Gate 2 boundary was reduced from an initially broader plan
to 45 justified models:

- 17 original affected models
- 23 additional existing dependencies
- 5 new models

The controlled build completed:

`45 / 45 PASS`

with one thread and fail-fast enabled.

The build created only authorised storage objects for the selected
locations.

## Staging and Bronze

Combined Staging/Bronze validation:

`49 / 49 PASS`

## Silver and Silver Vault

Validation:

`29 / 29 PASS`

## Gold

Validation:

`10 / 10 PASS`

Subsequent payment-grain remediation is documented below.

## Consumption

A later broad Consumption build completed:

- 37 Iceberg models
- 1 seed
- 74 tests
- 112 / 112 PASS

This broader Consumption execution is distinct from the original controlled
45-model Gate 2 boundary.

## Broad-selector incident

A geography-oriented selector using downstream expansion unexpectedly
selected a much broader graph.

One run contained 622 nodes and exposed three errors, including a stale
geography hierarchy assertion.

After the hierarchy correction, a subsequent 622-node run completed:

- PASS: 486
- ERROR: 2
- SKIP: 134

The two remaining findings were:

1. stale universal `transaction_id NOT NULL` expectation for
   `slv_pdm_payments_transactions`;
2. stale `token_version NOT NULL` expectation for the mandatory Unknown
   beneficiary dimension member.

A later one-shot DuckDB dependency build expanded to 623 nodes and
completed:

- PASS: 553
- ERROR: 2
- SKIP: 68

At that stage the two errors were:

1. stale `gld_dim_pdm_beneficiary.token_version NOT NULL` contract;
2. duplicate `gld_fct_pdm_payments.payment_sk` groups.

The Silver record-identifier correction was already proven PASS within that
run.

No further broad build is required for Gate 2 acceptance.

## Silver heterogeneous payment identifier

`slv_pdm_payments_transactions` contains heterogeneous payment messages.

pain.001/VPM initiation records legitimately have NULL `transaction_id`.
They carry alternative identifiers.

The corrected natural identifier is:

`record_identifier = coalesce(transaction_id, instruction_id, end_to_end_id, message_id)`

Final Silver grain is:

`source_system + record_identifier`

The stale universal transaction-id not-null test was replaced by the
correct source-system/record-identifier contract.

## Gold payment surrogate key

The Gold fact originally generated:

`payment_sk = surrogate(source_system, transaction_id)`

This collapsed pain.001 rows with NULL transaction IDs.

Read-only diagnosis showed:

- ICMN_VPM: 238 rows, 238 NULL transaction IDs, 1 distinct payment_sk
- WENDI_PAIN001: 238 rows, 238 NULL transaction IDs, 1 distinct payment_sk

The Gold surrogate key was corrected to:

`surrogate(source_system, record_identifier)`

Targeted Gold model rebuild:

`PASS=1 ERROR=0`

Targeted payment tests:

`PASS=8 ERROR=0`

including:

`unique_gld_fct_pdm_payments_payment_sk` PASS

Independent physical validation:

- total rows: 1103
- unique payment_sk: 1103
- NULL payment_sk: 0
- legitimate NULL transaction_id: 476

Source-level result:

- AGENT: 151 rows / 151 unique keys
- AIRTEL_PACS008: 16 / 16
- ICMN_VPM: 238 / 238
- MTN_PACS008: 222 / 222
- WENDI_PAIN001: 238 / 238
- WENDI_WALLET: 238 / 238

No pain.001 records were filtered and no transaction IDs were fabricated.

## Gold beneficiary Unknown member

`gld_dim_pdm_beneficiary` contains 251 rows:

- 250 canonical beneficiaries
- 1 mandatory Unknown dimension member

The Unknown member intentionally has no beneficiary token or token version.

The stale `token_version NOT NULL` test was removed.

`dbt ls` confirmed that
`not_null_gld_dim_pdm_beneficiary_token_version`
is no longer part of the contract.

Targeted beneficiary tests:

`PASS=10 ERROR=0`
