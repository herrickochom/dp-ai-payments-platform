-- ADDITIVE, REVIEW ONLY. Apply only to disposable PostgreSQL for local tests.
-- Existing 007/008 tables are not altered.
CREATE TABLE IF NOT EXISTS gcp_stage9_recovery_leases (
 execution_id char(64) NOT NULL,
 attempt_number integer NOT NULL,
 lease_owner text NOT NULL,
 lease_token text NOT NULL,
 lease_expires_at timestamptz NOT NULL,
 claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (execution_id, attempt_number),
 FOREIGN KEY (execution_id,attempt_number)
 REFERENCES gcp_dataflow_submission_intents(execution_id,attempt_number)
);
CREATE INDEX IF NOT EXISTS gcp_stage9_recovery_expiry_idx
 ON gcp_stage9_recovery_leases(lease_expires_at);
