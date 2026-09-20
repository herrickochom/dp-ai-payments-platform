CREATE TABLE IF NOT EXISTS transform_schema_version (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO transform_schema_version(version)
VALUES (1)
ON CONFLICT (version) DO NOTHING;

ALTER TABLE batch_executions
    ADD COLUMN IF NOT EXISTS cancel_requested boolean
    NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS batch_execution_queue
    ON batch_executions(status, accepted_at)
    WHERE status IN ('QUEUED','RUNNING','TESTING');

INSERT INTO transform_schema_version(version)
VALUES (2)
ON CONFLICT (version) DO NOTHING;
