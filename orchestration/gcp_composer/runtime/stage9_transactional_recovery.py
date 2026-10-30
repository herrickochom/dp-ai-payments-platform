"""Stage 9C: PostgreSQL-fenced submission outbox and reconciliation claims.
No GCP SDK. A provider adapter must guarantee safe external submission semantics.
"""
import json
from uuid import uuid4

class RecoveryError(RuntimeError): pass
class Fenced(RecoveryError): pass

class RecoveryStore:
    def __init__(self, connection_factory): self.connection_factory = connection_factory

    def prepare(self, eid, attempt, job_name, project, region, digest):
        """Idempotently persist intent. Caller admits execution in Stage 8 first."""
        if not isinstance(attempt, int) or attempt < 1: raise ValueError('invalid attempt')
        with self.connection_factory() as db:
            row = db.execute('''INSERT INTO gcp_dataflow_submission_intents
                (execution_id,attempt_number,job_name,project_id,region,spec_digest,state)
                VALUES (%s,%s,%s,%s,%s,%s,'PREPARED')
                ON CONFLICT (execution_id,attempt_number) DO NOTHING
                RETURNING execution_id''', (eid,attempt,job_name,project,region,digest)).fetchone()
            existing = db.execute('''SELECT job_name,project_id,region,spec_digest,state
                FROM gcp_dataflow_submission_intents WHERE execution_id=%s AND attempt_number=%s''',
                (eid,attempt)).fetchone()
            if existing is None or existing[:4] != (job_name,project,region,digest):
                raise RecoveryError('conflicting durable submission identity')
            return 'NEW' if row else existing[4]

    def claim_due(self, worker, lease_seconds=30):
        """Claim one due intent, including expired claims, with DB-server fencing."""
        if not worker or not 5 <= lease_seconds <= 3600: raise ValueError('invalid claim')
        token = uuid4().hex
        with self.connection_factory() as db:
            row = db.execute('''SELECT i.execution_id,i.attempt_number,i.state,i.job_name,
                i.project_id,i.region,i.spec_digest,i.dataflow_job_id
                FROM gcp_dataflow_submission_intents i
                LEFT JOIN gcp_stage9_recovery_leases l
                  ON l.execution_id=i.execution_id AND l.attempt_number=i.attempt_number
                WHERE i.state IN ('PREPARED','SUBMISSION_UNCERTAIN','OBSERVING','VERIFYING','RETRY_ELIGIBLE')
                  AND i.next_reconcile_at<=clock_timestamp()
                  AND (l.lease_expires_at IS NULL OR l.lease_expires_at<clock_timestamp())
                ORDER BY i.next_reconcile_at,i.execution_id,i.attempt_number
                FOR UPDATE OF i SKIP LOCKED LIMIT 1''').fetchone()
            if row is None: return None
            acquired = db.execute('''INSERT INTO gcp_stage9_recovery_leases
                (execution_id,attempt_number,lease_owner,lease_token,lease_expires_at)
                VALUES (%s,%s,%s,%s,clock_timestamp()+(%s*interval '1 second'))
                ON CONFLICT (execution_id,attempt_number) DO UPDATE
                SET lease_owner=EXCLUDED.lease_owner,lease_token=EXCLUDED.lease_token,
                    lease_expires_at=EXCLUDED.lease_expires_at,claimed_at=clock_timestamp()
                WHERE gcp_stage9_recovery_leases.lease_expires_at < clock_timestamp()
                RETURNING lease_token''',
                (row[0],row[1],worker,token,lease_seconds)).fetchone()
            if acquired is None: return None
        return dict(zip(('execution_id','attempt_number','state','job_name','project','region','digest','job_id'),row)) | {'worker':worker,'token':token}

    def transition(self, claim, expected, new_state, *, job_id=None, evidence_digest=None, delay_seconds=15):
        """Single fenced transition, transactionally updating outbox and next poll."""
        if not 1 <= delay_seconds <= 3600: raise ValueError('invalid delay')
        if new_state not in {'SUBMISSION_UNCERTAIN','OBSERVING','VERIFYING','QUARANTINED','COMPLETED','RETRY_ELIGIBLE'}:
            raise ValueError('invalid state')
        if new_state == 'COMPLETED' and (not job_id or not evidence_digest):
            raise ValueError('completion requires verified job and evidence digest')
        with self.connection_factory() as db:
            locked = db.execute('''SELECT 1 FROM gcp_stage9_recovery_leases
                WHERE execution_id=%s AND attempt_number=%s AND lease_owner=%s
                  AND lease_token=%s AND lease_expires_at>=clock_timestamp() FOR UPDATE''',
                (claim['execution_id'],claim['attempt_number'],claim['worker'],claim['token'])).fetchone()
            if not locked: raise Fenced('expired or stale recovery lease')
            row = db.execute('''UPDATE gcp_dataflow_submission_intents SET state=%s,
                dataflow_job_id=COALESCE(%s,dataflow_job_id),
                evidence_digest=COALESCE(%s,evidence_digest),
                uncertainty_started_at=CASE WHEN %s='SUBMISSION_UNCERTAIN'
                  THEN COALESCE(uncertainty_started_at,clock_timestamp()) ELSE uncertainty_started_at END,
                last_observed_at=clock_timestamp(),
                next_reconcile_at=clock_timestamp()+(%s*interval '1 second'),
                updated_at=clock_timestamp()
                WHERE execution_id=%s AND attempt_number=%s AND state=%s
                  AND (dataflow_job_id IS NULL OR %s::text IS NULL OR dataflow_job_id=%s)
                RETURNING execution_id''',
                (new_state,job_id,evidence_digest,new_state,delay_seconds,
                 claim['execution_id'],claim['attempt_number'],expected,job_id,job_id)).fetchone()
            if not row: raise Fenced('state changed or conflicting job identity')
            db.execute('''DELETE FROM gcp_stage9_recovery_leases WHERE execution_id=%s
                AND attempt_number=%s AND lease_token=%s''',
                (claim['execution_id'],claim['attempt_number'],claim['token']))

    def heartbeat(self, claim, seconds=30):
        if not 5 <= seconds <= 3600: raise ValueError('invalid heartbeat')
        with self.connection_factory() as db:
            return db.execute('''UPDATE gcp_stage9_recovery_leases
                SET lease_expires_at=clock_timestamp()+(%s*interval '1 second')
                WHERE execution_id=%s AND attempt_number=%s AND lease_owner=%s
                  AND lease_token=%s AND lease_expires_at>=clock_timestamp()
                RETURNING lease_token''', (seconds,claim['execution_id'],claim['attempt_number'],
                                          claim['worker'],claim['token'])).fetchone() is not None
