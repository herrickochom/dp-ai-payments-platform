"""Durable worker. Lease loss and unknown state become ORPHANED, never auto-retried."""
import os,socket,time
try:
    from orchestration.transform_runtime.durable_queue import LeaseLost,PostgresDurableQueue
except ImportError:
    from transform_runtime.durable_queue import LeaseLost,PostgresDurableQueue
from transform_execution import build_transform_command,execute

def enabled():
    return os.getenv("PLATFORM_RUNNER_EXECUTION_ENABLED","false").lower()=="true" and os.getenv("PLATFORM_JOB_EXECUTION_ENABLED","false").lower()=="true"

def run_once(queue=None,worker=None):
    if not enabled(): return False
    q=queue or PostgresDurableQueue(); worker=worker or f"{socket.gethostname()}:{os.getpid()}"
    lease=int(os.getenv("TRANSFORM_WORKER_LEASE_SECONDS","30"))
    q.reconcile_expired(); row=q.claim(worker,lease)
    if not row: return False
    eid=row["batch_execution_id"]; token=row["lease_token"]
    cmd=build_transform_command(row["batch_id"],eid,transform_run_id=row["transform_run_id"])
    last=[time.monotonic()]; lease_lost=[False]
    def cancel():
        if time.monotonic()-last[0]>=max(1,lease//3):
            if not q.heartbeat(eid,worker,token,lease):
                lease_lost[0]=True; return True
            last[0]=time.monotonic()
        try: return q.cancel_requested(eid,worker,token)
        except LeaseLost:
            lease_lost[0]=True; return True
    try:
        r=execute(cmd,timeout_seconds=float(os.getenv("TRANSFORM_BATCH_TIMEOUT_SECONDS","3300")),
                  termination_grace_seconds=float(os.getenv("TRANSFORM_TERMINATION_GRACE_SECONDS","15")),
                  output_cap_bytes=int(os.getenv("TRANSFORM_OUTPUT_CAP_BYTES","262144")),cancel_requested=cancel)
    except BaseException:
        q.orphan_if_owned(eid,worker,token,"WORKER_EXCEPTION_UNKNOWN_WRITE_STATE"); raise
    if lease_lost[0]:
        q.orphan_if_owned(eid,worker,token,"WORKER_LEASE_LOST_REQUIRES_RECONCILIATION")
        return True
    if r.status=="SUCCEEDED": q.finish(eid,worker,token,"SUCCEEDED",test_status="PASSED")
    elif r.status=="FAILED": q.finish(eid,worker,token,"FAILED",test_status="NOT_RUN",failure_class=r.failure_class or "DBT_BUILD_FAILED")
    elif r.status=="CANCELLED": q.finish(eid,worker,token,"CANCELLED",failure_class=r.failure_class or "OPERATOR_CANCELLED")
    else: q.finish(eid,worker,token,"ORPHANED",failure_class=r.failure_class or "UNKNOWN_WRITE_STATE_REQUIRES_RECONCILIATION")
    return True

def main():
    while True:
        if not run_once(): time.sleep(float(os.getenv("TRANSFORM_WORKER_POLL_SECONDS","1")))
if __name__=="__main__": main()
