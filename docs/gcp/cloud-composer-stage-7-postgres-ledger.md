# Stage 7 — PostgreSQL GCP payment execution ledger

Status: OFFLINE CONTRACT ONLY. Migration NOT APPLIED. Cloud deployment BLOCKED.

The GCP ledger is separate from the dbt transform queue. It borrows the established
PostgreSQL transactional-claim, database-time lease and fencing patterns.

## Explicit limitations

- This code is not wired into Airflow, Dataflow or BigQuery.
- No live PostgreSQL integration, concurrent-worker or crash tests have run.
- No production database migration has been applied.
- `record_job` is a persistence primitive, NOT a safe submission protocol.
  A process crash between remote submission and recording the job ID remains unsafe.
- External verification is represented by caller-supplied evidence, not independently
  validated against GCP. Do NOT connect untrusted evidence to `complete`.
- Expired leases are quarantined; no automatic requeue or external submission.
- Automated reconciliation, outbox/intent, external job discovery, retries,
  roles/grants, retention and deployment approvals are still required.
- The migration requires an approved dedicated PostgreSQL database and role.
- The psycopg3 dependency and connection security must be validated in the
  actual target runtime before database access.

## Next gates

1. Review schema and migration in a dedicated local PostgreSQL test database.
2. Run real concurrent claims, expiry, stale-token and transaction rollback tests.
3. Define stable Dataflow submission intent and job-discovery semantics.
4. Implement trusted GCP reconciliation adapters and bounded BigQuery evidence.
5. Enable automated reconciliation under a dedicated worker and security role.
6. Approve IAM, network, billing and deployment separately.
