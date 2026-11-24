"""Stage 8 to Stage 9 transactional submission-intent handoff.

Offline implementation. No GCP SDK, network access, migration or
external job submission.

The caller must already hold a live Stage 8 execution lease.
The PostgreSQL connection factory must return a transactional
psycopg3-style connection with autocommit disabled.
"""

import re


class HandoffError(RuntimeError):
    pass


class HandoffFenced(HandoffError):
    pass


class HandoffConflict(HandoffError):
    pass


class Stage89Handoff:
    def __init__(self, connection_factory):
        self.connection_factory = connection_factory

    def prepare(
        self,
        execution_id,
        attempt_number,
        worker,
        lease_token,
        job_name,
        project,
        region,
        spec_digest,
    ):
        """Atomically verify Stage 8 lease and prepare Stage 9 intent."""

        if not isinstance(execution_id, str) or not re.fullmatch(
            r"[0-9a-f]{64}", execution_id
        ):
            raise ValueError("Invalid execution ID")

        if (
            not isinstance(attempt_number, int)
            or isinstance(attempt_number, bool)
            or attempt_number < 1
        ):
            raise ValueError("Invalid attempt number")

        if not isinstance(worker, str) or not worker.strip():
            raise ValueError("Invalid worker")

        if not isinstance(lease_token, str) or not lease_token.strip():
            raise ValueError("Invalid lease token")

        if not isinstance(job_name, str) or not re.fullmatch(
            r"[a-z][a-z0-9-]{2,62}", job_name
        ):
            raise ValueError("Invalid job name")

        if not isinstance(spec_digest, str) or not re.fullmatch(
            r"[0-9a-f]{64}", spec_digest
        ):
            raise ValueError("Invalid specification digest")

        if not isinstance(project, str) or not project.strip():
            raise ValueError("Invalid project")

        if not isinstance(region, str) or not region.strip():
            raise ValueError("Invalid region")

        with self.connection_factory() as db:
            if getattr(db, "autocommit", False):
                raise HandoffError(
                    "Autocommit must be disabled for atomic handoff"
                )

            # Serialize handoffs for this execution and fence stale workers.
            execution = db.execute(
                """
                SELECT execution_id, attempts
                FROM gcp_payment_executions
                WHERE execution_id=%s
                  AND status='RUNNING'
                  AND attempts=%s
                  AND lease_owner=%s
                  AND lease_token=%s
                  AND lease_expires_at>=clock_timestamp()
                FOR UPDATE
                """,
                (
                    execution_id,
                    attempt_number,
                    worker,
                    lease_token,
                ),
            ).fetchone()

            if execution is None:
                raise HandoffFenced(
                    "Execution missing, lease expired or attempt changed"
                )

            # Check existing identity before attempting insertion.
            existing = db.execute(
                """
                SELECT job_name, project_id, region, spec_digest, state
                FROM gcp_dataflow_submission_intents
                WHERE execution_id=%s AND attempt_number=%s
                FOR UPDATE
                """,
                (execution_id, attempt_number),
            ).fetchone()

            identity = (job_name, project, region, spec_digest)

            if existing is not None:
                if tuple(existing[:4]) != identity:
                    raise HandoffConflict(
                        "Conflicting durable submission identity"
                    )

                return {
                    "execution_id": execution_id,
                    "attempt_number": attempt_number,
                    "decision": "DUPLICATE",
                    "state": existing[4],
                }

            inserted = db.execute(
                """
                INSERT INTO gcp_dataflow_submission_intents
                    (
                        execution_id,
                        attempt_number,
                        job_name,
                        project_id,
                        region,
                        spec_digest,
                        state
                    )
                VALUES (%s,%s,%s,%s,%s,%s,'PREPARED')
                ON CONFLICT DO NOTHING
                RETURNING execution_id
                """,
                (
                    execution_id,
                    attempt_number,
                    job_name,
                    project,
                    region,
                    spec_digest,
                ),
            ).fetchone()

            if inserted is None:
                # Could be an identity conflict or another uniqueness
                # constraint. Fail closed; do not infer success.
                raise HandoffConflict(
                    "Submission identity uniqueness conflict"
                )

            return {
                "execution_id": execution_id,
                "attempt_number": attempt_number,
                "decision": "NEW",
                "state": "PREPARED",
            }
