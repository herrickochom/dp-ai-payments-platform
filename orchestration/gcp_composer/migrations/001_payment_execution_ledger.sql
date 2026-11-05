-- ADDITIVE ONLY. Assessment artifact: do not apply without database approval.
-- Dedicated GCP ledger; never changes transform_runs or batch_executions.
CREATE TABLE IF NOT EXISTS gcp_payment_executions (
    execution_id char(64) PRIMARY KEY,
    manifest_json jsonb NOT NULL,
    status text NOT NULL DEFAULT 'QUEUED'
      CHECK (status IN ('QUEUED','RUNNING','RECONCILIATION_REQUIRED','ACCEPTED')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at timestamptz,
    lease_owner text,
    lease_token text,
    lease_expires_at timestamptz,
    dataflow_job_id text UNIQUE,
    evidence_json jsonb,
    CONSTRAINT gcp_lease_pair CHECK ((lease_owner IS NULL) = (lease_token IS NULL)),
    CONSTRAINT gcp_accepted_evidence CHECK (status <> 'ACCEPTED' OR evidence_json IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS gcp_payment_claim_idx
    ON gcp_payment_executions(accepted_at,execution_id) WHERE status='QUEUED';
CREATE INDEX IF NOT EXISTS gcp_payment_expiry_idx
    ON gcp_payment_executions(lease_expires_at) WHERE status='RUNNING';
-- Production hardening pending: dedicated roles, RLS/grants, migration runner,
-- submission intent/outbox, durable reconciliation polling and retention.
