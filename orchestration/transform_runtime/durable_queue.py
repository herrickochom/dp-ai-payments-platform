"""PostgreSQL durable queue with DB-time fenced leases. ORPHANED is never requeued."""
from __future__ import annotations
import json, os
from contextlib import contextmanager
from uuid import uuid4

TERMINAL={"SUCCEEDED","FAILED","CANCELLED","ORPHANED"}
ACTIVE={"RUNNING","TESTING"}
class DurableQueueError(RuntimeError): pass
class LeaseLost(DurableQueueError): pass

class PostgresDurableQueue:
    def __init__(self,url=None):
        url=(url or os.getenv("TRANSFORM_LEDGER_DATABASE_URL","")).strip()
        if not url: raise DurableQueueError("TRANSFORM_LEDGER_DATABASE_URL is required")
        import psycopg
        from psycopg.rows import dict_row
        self.url=url.replace("postgresql+psycopg://","postgresql://",1)
        self.psycopg=psycopg; self.dict_row=dict_row

    @contextmanager
    def tx(self):
        with self.psycopg.connect(self.url,row_factory=self.dict_row) as db:
            with db.transaction(): yield db

    def _event(self,db,row,event,meta=None):
        db.execute("""INSERT INTO execution_events(event_id,transform_run_id,batch_execution_id,event_type,event_at,metadata_json)
                      VALUES (%s,%s,%s,%s,clock_timestamp(),%s::jsonb)""",
                   (uuid4().hex,row["transform_run_id"],row["batch_execution_id"],event,json.dumps(meta or {},sort_keys=True)))

    def claim(self,worker,lease_seconds):
        token=uuid4().hex
        with self.tx() as db:
            row=db.execute("""SELECT * FROM batch_executions
                              WHERE status='QUEUED' AND cancel_requested=false
                              ORDER BY accepted_at,batch_execution_id
                              FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
            if not row: return None
            out=db.execute("""UPDATE batch_executions
                              SET status='RUNNING',started_at=COALESCE(started_at,clock_timestamp()),
                                  heartbeat_at=clock_timestamp(),lease_owner=%s,lease_token=%s,
                                  lease_expires_at=clock_timestamp()+(%s*interval '1 second')
                              WHERE batch_execution_id=%s AND status='QUEUED'
                              RETURNING *""",(worker,token,lease_seconds,row["batch_execution_id"])).fetchone()
            if not out: return None
            db.execute("UPDATE batch_attempts SET status='RUNNING',heartbeat_at=clock_timestamp() WHERE batch_execution_id=%s AND attempt=%s",
                       (row["batch_execution_id"],row["attempt"]))
            self._event(db,row,"CLAIMED",{"worker_id":worker,"lease_token":token})
            return dict(out)

    def heartbeat(self,eid,worker,token,lease_seconds):
        with self.tx() as db:
            return bool(db.execute("""UPDATE batch_executions
                                      SET heartbeat_at=clock_timestamp(),
                                          lease_expires_at=clock_timestamp()+(%s*interval '1 second')
                                      WHERE batch_execution_id=%s AND lease_owner=%s AND lease_token=%s
                                        AND status IN ('RUNNING','TESTING')
                                        AND lease_expires_at>=clock_timestamp()
                                      RETURNING batch_execution_id""",
                                   (lease_seconds,eid,worker,token)).fetchone())

    def cancel_requested(self,eid,worker,token):
        with self.tx() as db:
            row=db.execute("""SELECT cancel_requested FROM batch_executions
                              WHERE batch_execution_id=%s AND lease_owner=%s AND lease_token=%s
                                AND status IN ('RUNNING','TESTING')""",(eid,worker,token)).fetchone()
            if not row: raise LeaseLost("worker lease lost")
            return bool(row["cancel_requested"])

    def request_cancel(self,eid):
        with self.tx() as db:
            row=db.execute("SELECT * FROM batch_executions WHERE batch_execution_id=%s FOR UPDATE",(eid,)).fetchone()
            if not row: raise DurableQueueError("batch execution not found")
            if row["status"] in TERMINAL: return dict(row)
            if row["status"] in {"ADMITTED","QUEUED"}:
                out=db.execute("""UPDATE batch_executions SET cancel_requested=true,
                                  cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp()),
                                  status='CANCELLED',finished_at=clock_timestamp(),heartbeat_at=clock_timestamp(),
                                  lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
                                  failure_class='OPERATOR_CANCELLED'
                                  WHERE batch_execution_id=%s RETURNING *""",(eid,)).fetchone()
                self._event(db,row,"CANCELLED",{}); return dict(out)
            out=db.execute("""UPDATE batch_executions SET cancel_requested=true,
                              cancel_requested_at=COALESCE(cancel_requested_at,clock_timestamp())
                              WHERE batch_execution_id=%s RETURNING *""",(eid,)).fetchone()
            self._event(db,row,"CANCEL_REQUESTED",{}); return dict(out)

    def finish(self,eid,worker,token,status,test_status=None,failure_class=None):
        if status not in TERMINAL: raise DurableQueueError("terminal status required")
        with self.tx() as db:
            row=db.execute("SELECT * FROM batch_executions WHERE batch_execution_id=%s FOR UPDATE",(eid,)).fetchone()
            if not row or row["lease_owner"]!=worker or row["lease_token"]!=token or row["status"] not in ACTIVE:
                raise LeaseLost("worker lease lost")
            out=db.execute("""UPDATE batch_executions SET status=%s,finished_at=clock_timestamp(),
                              heartbeat_at=clock_timestamp(),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
                              test_status=%s,failure_class=%s
                              WHERE batch_execution_id=%s AND lease_owner=%s AND lease_token=%s
                                AND status IN ('RUNNING','TESTING')
                                AND lease_expires_at>=clock_timestamp()
                              RETURNING *""",
                           (status,test_status,failure_class,eid,worker,token)).fetchone()
            if not out: raise LeaseLost("worker lease lost")
            db.execute("UPDATE batch_attempts SET status=%s,heartbeat_at=clock_timestamp() WHERE batch_execution_id=%s AND attempt=%s",
                       (status,eid,row["attempt"]))
            self._event(db,row,status,{"failure_class":failure_class}); return dict(out)

    def orphan_if_owned(self,eid,worker,token,failure_class):
        with self.tx() as db:
            row=db.execute("""SELECT * FROM batch_executions WHERE batch_execution_id=%s
                              AND lease_owner=%s AND lease_token=%s AND status IN ('RUNNING','TESTING') FOR UPDATE""",
                           (eid,worker,token)).fetchone()
            if not row: return False
            db.execute("""UPDATE batch_executions SET status='ORPHANED',finished_at=clock_timestamp(),
                          heartbeat_at=clock_timestamp(),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
                          failure_class=%s WHERE batch_execution_id=%s AND lease_owner=%s AND lease_token=%s""",
                       (failure_class,eid,worker,token))
            db.execute("UPDATE batch_attempts SET status='ORPHANED',heartbeat_at=clock_timestamp() WHERE batch_execution_id=%s AND attempt=%s",
                       (eid,row["attempt"]))
            self._event(db,row,"ORPHANED",{"failure_class":failure_class}); return True

    def reconcile_expired(self):
        ids=[]
        with self.tx() as db:
            rows=db.execute("""SELECT * FROM batch_executions
                               WHERE status IN ('RUNNING','TESTING') AND lease_expires_at IS NOT NULL
                                 AND lease_expires_at<clock_timestamp()
                               FOR UPDATE SKIP LOCKED""").fetchall()
            for row in rows:
                eid=row["batch_execution_id"]; ids.append(eid)
                db.execute("""UPDATE batch_executions SET status='ORPHANED',finished_at=clock_timestamp(),
                              heartbeat_at=clock_timestamp(),lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
                              failure_class='WORKER_LEASE_EXPIRED_REQUIRES_RECONCILIATION'
                              WHERE batch_execution_id=%s""",(eid,))
                db.execute("UPDATE batch_attempts SET status='ORPHANED',heartbeat_at=clock_timestamp() WHERE batch_execution_id=%s AND attempt=%s",
                           (eid,row["attempt"]))
                self._event(db,row,"ORPHANED",{"failure_class":"WORKER_LEASE_EXPIRED_REQUIRES_RECONCILIATION"})
        return ids
