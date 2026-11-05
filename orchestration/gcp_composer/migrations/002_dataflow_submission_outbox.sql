-- Stage 9 additive design migration. REVIEW ONLY; NOT applied by this script.
-- Requires migration 007. Existing transform ledgers are untouched.
-- One durable submission identity per execution/attempt; no automatic re-submission
-- after an ambiguous external request until provider reconciliation is conclusive.
CREATE TABLE IF NOT EXISTS gcp_dataflow_submission_intents (
    execution_id char(64) NOT NULL REFERENCES gcp_payment_executions(execution_id),
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    job_name text NOT NULL,
    project_id text NOT NULL,
    region text NOT NULL,
    spec_digest char(64) NOT NULL,
    state text NOT NULL CHECK (state IN (
      'PREPARED','SUBMISSION_UNCERTAIN','OBSERVING','VERIFYING',
      'RETRY_ELIGIBLE','QUARANTINED','COMPLETED')),
    dataflow_job_id text,
    evidence_digest char(64),
    uncertainty_started_at timestamptz,
    next_reconcile_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_observed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (execution_id, attempt_number),
    UNIQUE (project_id,region,job_name),
    UNIQUE (dataflow_job_id),
    CONSTRAINT gcp_submission_uncertain_timestamp CHECK (
      state <> 'SUBMISSION_UNCERTAIN' OR uncertainty_started_at IS NOT NULL),
    CONSTRAINT gcp_submission_completed_evidence CHECK (
      state <> 'COMPLETED' OR (dataflow_job_id IS NOT NULL AND evidence_digest IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS gcp_submission_reconcile_due_idx
 ON gcp_dataflow_submission_intents(next_reconcile_at,execution_id,attempt_number)
 WHERE state IN ('SUBMISSION_UNCERTAIN','OBSERVING','VERIFYING','RETRY_ELIGIBLE');
-- Required implementation constraints (not satisfied by this DDL alone):
-- 1. Commit PREPARED before provider call; atomically transition to
--    SUBMISSION_UNCERTAIN before network submission, fenced by live lease.
-- 2. Use database server time and SELECT FOR UPDATE SKIP LOCKED for recovery claims.
-- 3. Recover expired RUNNING/RECONCILIATION_REQUIRED records with new lease tokens.
-- 4. A missing provider listing is NOT proof of no submission.
-- 5. Do not retry after ambiguous outcomes without a documented provider-side
--    duplicate-rejection guarantee and a conclusive reconciliation protocol.
-- 6. Bind output manifest and BigQuery evidence to execution and attempt.
-- 7. Enforce bounded backoff and escalation; never mark complete on job state alone.
