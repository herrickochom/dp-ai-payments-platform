CREATE TABLE IF NOT EXISTS protected_bootstrap_executions (
    bootstrap_execution_id text PRIMARY KEY CHECK (bootstrap_execution_id ~ '^be_[a-f0-9]{32}$'),
    transform_run_id text NOT NULL REFERENCES transform_runs(transform_run_id),
    caller_identity text NOT NULL,
    idempotency_key text NOT NULL,
    operation_id text NOT NULL,
    operation_version integer NOT NULL CHECK (operation_version > 0),
    authority text NOT NULL CHECK (authority = 'restricted_identity_transform'),
    status text NOT NULL,
    publication_state text NOT NULL,
    nessie_branch text NOT NULL CHECK (nessie_branch ~ '^transform_tr_[a-f0-9]{32}$'),
    accepted_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    failure_class text,
    heartbeat_at timestamptz NOT NULL,
    lease_owner text,
    lease_token text,
    lease_expires_at timestamptz,
    cancel_requested boolean NOT NULL DEFAULT false,
    UNIQUE (caller_identity, idempotency_key),
    UNIQUE (transform_run_id, operation_id, operation_version)
);

CREATE INDEX IF NOT EXISTS protected_bootstrap_claimable
    ON protected_bootstrap_executions(status, accepted_at)
    WHERE status = 'QUEUED';

INSERT INTO transform_schema_version(version) VALUES (4)
ON CONFLICT(version) DO NOTHING;
