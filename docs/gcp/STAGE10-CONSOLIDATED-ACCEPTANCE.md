# Stage 10 consolidated implementation and acceptance

## Installed
- Fenced, single-attempt Dataflow Flex Template submission adapter, with
  durable UNCERTAIN transition before the provider call, no SDK retry and
  project/region/worker-shape constraints.
- Independent expected/observed event manifest comparison primitive.
- Regression tests and Terraform validation.

## Not yet accepted
- No approved Flex Template or Beam producer is included in the source archive.
- Dataflow label values cannot hold full SHA-256 identities (63-char limit).
  Existing cancellation/watchdog modules use full hashes as labels and must be
  reconciled with durable identity before enabling unattended cancellation.
- The provided payment events table has no execution-bound output columns.
  Lineage must be emitted by the real Beam producer, and its manifest must be
  independently verified; a hash of existing rows is not enough.
- Billing export freshness and the currency/credit accounting source have not
  been validated against live Cloud Billing.
- Composer environment, Airflow connections, runtime packaging, migrations,
  private PostgreSQL networking and IAM have not been deployed or accepted.
- A Flex Template request is not provider-side exactly-once execution. Ambiguous
  submission must remain UNCERTAIN and must never be blindly retried.
- No real cloud end-to-end acceptance or teardown was executed.

## Next acceptance gate
Approve the precise Terraform plan, Flex Template artifact and expected
pilot costs. Then run the real synthetic-payment workload, verify outputs,
exercise crash/recovery and confirm automated teardown with live evidence.
