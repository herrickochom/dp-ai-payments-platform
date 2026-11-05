"""Read-only durable-ledger watchdog. No cancellation without an independently
approved, deployed, audited and fenced mutation path.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from provider_enforcement import CancellationTarget, verify_target, WatchdogBlocked

@dataclass(frozen=True)
class LedgerJob:
    execution_id: str
    attempt_number: int
    job_id: str
    job_name: str
    project: str
    region: str
    spec_digest: str
    state: str

def load_ledger_jobs(connection_factory, *, limit=100):
    if type(limit) is not int or not 1 <= limit <= 100:
        raise WatchdogBlocked('invalid bounded ledger query')
    # SQL parameters only; read-only transaction prevents accidental writes.
    with connection_factory() as db:
        with db.cursor() as cur:
            cur.execute('SET TRANSACTION READ ONLY')
            cur.execute("""
              SELECT execution_id,attempt_number,dataflow_job_id,job_name,
                     project_id,region,spec_digest,state
              FROM gcp_dataflow_submission_intents
              WHERE dataflow_job_id IS NOT NULL
                AND state IN ('OBSERVING','VERIFYING','SUBMISSION_UNCERTAIN')
              ORDER BY updated_at,execution_id,attempt_number LIMIT %s
            """,(limit,))
            rows=cur.fetchall()
    return [LedgerJob(*row) for row in rows]

def assess_ledger_job(client, item: LedgerJob, *, now=None):
    now=now or datetime.now(timezone.utc)
    if not isinstance(item,LedgerJob) or not re.fullmatch(r'[0-9a-f]{64}',item.execution_id):
        raise WatchdogBlocked('invalid durable execution identity')
    if not re.fullmatch(r'[0-9a-f]{64}',item.spec_digest) or not item.job_id:
        raise WatchdogBlocked('invalid durable provider identity')
    if item.state not in {'OBSERVING','VERIFYING','SUBMISSION_UNCERTAIN'}:
        raise WatchdogBlocked('unrecognised ledger state')
    target=CancellationTarget(item.project,item.region,item.job_id,item.job_name,
                              item.execution_id,item.spec_digest)
    try:
        job=client.get_job(request={'project_id':item.project,'location':item.region,'job_id':item.job_id})
    except Exception as exc:
        raise WatchdogBlocked('provider exact-ID observation failed') from exc
    verify_target(job,target,now=now)
    return {'decision':'EXPIRED_VERIFIED_READ_ONLY','job_id':item.job_id,
            'execution_id':item.execution_id,'mutation_allowed':False}

def inspect_ledger(client,connection_factory,*,limit=100,now=None):
    results=[]
    for item in load_ledger_jobs(connection_factory,limit=limit):
        try:
            results.append(assess_ledger_job(client,item,now=now))
        except WatchdogBlocked as exc:
            results.append({'decision':'BLOCKED','job_id':item.job_id,
                            'reason':str(exc),'mutation_allowed':False})
    return results
