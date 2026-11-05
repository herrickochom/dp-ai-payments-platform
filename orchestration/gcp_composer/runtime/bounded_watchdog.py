"""Conservative Dataflow pilot watchdog.

Reads provider jobs and proposes cancellation only for strongly identified,
expired pilot jobs. Default is dry-run. Never cancels unrelated or ambiguous jobs.
A separate scheduled deployment and IAM authorisation are required for autonomy.
"""
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import re

class WatchdogBlocked(RuntimeError): pass

@dataclass(frozen=True)
class WatchdogScope:
    project: str = 'project-e48f194e-f44f-458d-b4f'
    region: str = 'europe-west2'
    max_minutes: int = 30
    label_key: str = 'pdm_pilot'
    label_value: str = 'stage10'

ACTIVE = {'JOB_STATE_PENDING','JOB_STATE_QUEUED','JOB_STATE_RUNNING',
          'JOB_STATE_CANCELLING','JOB_STATE_DRAINING'}
TERMINAL = {'JOB_STATE_DONE','JOB_STATE_FAILED','JOB_STATE_CANCELLED','JOB_STATE_DRAINED','JOB_STATE_UPDATED'}

def _utc(value):
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc)
    if hasattr(value,'ToDatetime'):
        d=value.ToDatetime()
        if d.tzinfo is not None: return d.astimezone(timezone.utc)
    raise WatchdogBlocked('missing or invalid provider creation timestamp')

def _state(value):
    n=getattr(value,'name',None)
    if isinstance(n,str): return n
    return value if isinstance(value,str) else 'JOB_STATE_UNKNOWN'

def assess_job(job, *, project, region, now=None, scope=WatchdogScope()):
    """Returns EXPIRED, WITHIN_LIMIT, TERMINAL, or BLOCKED. No side effects."""
    now=now or datetime.now(timezone.utc)
    if now.tzinfo is None: raise WatchdogBlocked('naive clock')
    if (project,region)!=(scope.project,scope.region): raise WatchdogBlocked('scope mismatch')
    labels=dict(getattr(job,'labels',{}) or {})
    name=str(getattr(job,'name',''))
    job_id=str(getattr(job,'id',''))
    if labels.get(scope.label_key)!=scope.label_value or not re.fullmatch(r'[a-z][a-z0-9-]{2,62}',name) or not job_id:
        return 'BLOCKED'
    state=_state(getattr(job,'current_state',None))
    if state in TERMINAL: return 'TERMINAL'
    if state not in ACTIVE: return 'BLOCKED'
    created=_utc(getattr(job,'create_time',None))
    if created > now.astimezone(timezone.utc): return 'BLOCKED'
    return 'EXPIRED' if now.astimezone(timezone.utc)-created >= timedelta(minutes=scope.max_minutes) else 'WITHIN_LIMIT'

def inspect_jobs(client, *, project, region, now=None, scope=WatchdogScope()):
    """Fail closed if provider listing fails. No provider mutations."""
    if (project,region)!=(scope.project,scope.region): raise WatchdogBlocked('scope mismatch')
    try:
        response=client.list_jobs(request={'project_id':project,'location':region,'filter':'ALL'})
        jobs=list(getattr(response,'jobs',[]) or [])
    except Exception as exc:
        raise WatchdogBlocked('provider listing unavailable') from exc
    return [{'id':str(getattr(j,'id','')), 'name':str(getattr(j,'name','')),
             'decision':assess_job(j,project=project,region=region,now=now,scope=scope)} for j in jobs]

def cancel_expired(*args, **kwargs):
    """Fail closed until deployed scheduler, IAM and independent job ownership checks exist."""
    raise WatchdogBlocked('automatic provider cancellation not authorised or deployed; dry-run only')
