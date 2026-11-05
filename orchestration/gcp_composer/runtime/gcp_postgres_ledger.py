'''GCP payment execution ledger. No GCP API calls or automatic job submission.

SQL is parameterised. All claims and transitions are transactionally fenced
against PostgreSQL server time, not worker clocks.
'''
import json
from uuid import uuid4
from bounded_controller import validate_manifest, execution_id

class LedgerError(RuntimeError): pass
class LeaseLost(LedgerError): pass
class ReconciliationRequired(LedgerError): pass

class GcpPaymentLedger:
    def __init__(self, connection_factory):
        '''Factory returns a psycopg3 connection supporting context manager.'''
        self.connection_factory=connection_factory

    def admit(self, manifest):
        canonical=validate_manifest(manifest)
        eid=execution_id(canonical)
        payload=json.dumps(canonical,sort_keys=True,separators=(',',':'))
        with self.connection_factory() as db:
            row=db.execute('''INSERT INTO gcp_payment_executions
                (execution_id,manifest_json,status) VALUES (%s,%s::jsonb,'QUEUED')
                ON CONFLICT (execution_id) DO NOTHING RETURNING execution_id''', (eid,payload)).fetchone()
            stored=db.execute('''SELECT manifest_json FROM gcp_payment_executions
                WHERE execution_id=%s''',(eid,)).fetchone()
            if stored is None or stored[0]!=json.loads(payload):
                raise LedgerError('Execution identity collision or manifest mismatch')
        return {'execution_id':eid,'decision':'NEW' if row else 'DUPLICATE'}

    def claim(self, worker, lease_seconds=30):
        if not isinstance(worker,str) or not worker.strip() or not isinstance(lease_seconds,int) or not 5<=lease_seconds<=3600:
            raise ValueError('Invalid worker or lease duration')
        token=uuid4().hex
        with self.connection_factory() as db:
            row=db.execute('''SELECT execution_id FROM gcp_payment_executions
                WHERE status='QUEUED' ORDER BY accepted_at,execution_id
                FOR UPDATE SKIP LOCKED LIMIT 1''').fetchone()
            if row is None: return None
            result=db.execute('''UPDATE gcp_payment_executions SET status='RUNNING',
                lease_owner=%s,lease_token=%s,lease_expires_at=clock_timestamp()+(%s*interval '1 second'),
                heartbeat_at=clock_timestamp(),attempts=attempts+1,updated_at=clock_timestamp()
                WHERE execution_id=%s AND status='QUEUED'
                RETURNING execution_id,attempts''',(worker,token,lease_seconds,row[0])).fetchone()
            if result is None: raise LedgerError('Claim lost')
        return {'execution_id':result[0],'attempts':result[1],'worker':worker,'lease_token':token}

    def heartbeat(self,eid,worker,token,lease_seconds=30):
        if not isinstance(lease_seconds,int) or not 5<=lease_seconds<=3600: raise ValueError('Invalid lease duration')
        with self.connection_factory() as db:
            return db.execute('''UPDATE gcp_payment_executions SET heartbeat_at=clock_timestamp(),
                lease_expires_at=clock_timestamp()+(%s*interval '1 second'),updated_at=clock_timestamp()
                WHERE execution_id=%s AND lease_owner=%s AND lease_token=%s
                AND status='RUNNING' AND lease_expires_at>=clock_timestamp()
                RETURNING execution_id''',(lease_seconds,eid,worker,token)).fetchone() is not None

    def record_job(self,eid,worker,token,job_id):
        '''Persist an independently supplied external identity; never submit a job.'''
        if not isinstance(job_id,str) or not job_id.strip() or len(job_id)>256: raise ValueError('Invalid job ID')
        with self.connection_factory() as db:
            row=db.execute('''UPDATE gcp_payment_executions SET dataflow_job_id=%s,
                updated_at=clock_timestamp() WHERE execution_id=%s AND lease_owner=%s
                AND lease_token=%s AND status='RUNNING'
                AND lease_expires_at>=clock_timestamp()
                AND (dataflow_job_id IS NULL OR dataflow_job_id=%s)
                RETURNING execution_id''',(job_id,eid,worker,token,job_id)).fetchone()
            if row is None: raise LeaseLost('Lease lost or conflicting external job identity')

    def complete(self,eid,worker,token,evidence):
        '''Require explicit verified reconciliation evidence; no inferred success.'''
        if not isinstance(evidence,dict) or evidence.get('verified') is not True or evidence.get('bigquery_verified') is not True or not evidence.get('dataflow_job_id'):
            raise ReconciliationRequired('Verified Dataflow and BigQuery evidence required')
        payload=json.dumps(evidence,sort_keys=True)
        with self.connection_factory() as db:
            row=db.execute('''UPDATE gcp_payment_executions SET status='ACCEPTED',
                evidence_json=%s::jsonb,lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
                updated_at=clock_timestamp() WHERE execution_id=%s AND lease_owner=%s
                AND lease_token=%s AND status='RUNNING'
                AND dataflow_job_id=%s AND lease_expires_at>=clock_timestamp()
                RETURNING execution_id''',(payload,eid,worker,token,evidence['dataflow_job_id'])).fetchone()
            if row is None: raise LeaseLost('Lease lost or evidence job ID mismatch')

    def quarantine_expired(self):
        '''Expired RUNNING becomes RECONCILIATION_REQUIRED, never QUEUED.'''
        with self.connection_factory() as db:
            rows=db.execute('''UPDATE gcp_payment_executions
                SET status='RECONCILIATION_REQUIRED',lease_owner=NULL,lease_token=NULL,
                lease_expires_at=NULL,updated_at=clock_timestamp()
                WHERE execution_id IN (SELECT execution_id FROM gcp_payment_executions
                WHERE status='RUNNING' AND lease_expires_at<clock_timestamp()
                FOR UPDATE SKIP LOCKED)
                RETURNING execution_id''').fetchall()
        return [r[0] for r in rows]

    def inspect(self,eid):
        with self.connection_factory() as db:
            row=db.execute('''SELECT execution_id,status,attempts,dataflow_job_id,evidence_json
                FROM gcp_payment_executions WHERE execution_id=%s''',(eid,)).fetchone()
        return None if row is None else {'execution_id':row[0],'status':row[1],
            'attempts':row[2],'dataflow_job_id':row[3],'evidence':row[4]}
