"""14F-H: durable, offline-first remote job observation and evidence contracts.

No SDK client is constructed; no live execution, DDL, DML or BigQuery query is
implemented. Journal writes are local SQLite only. Job observation requires an
injected client and is read-only *by contract*, tested with fake clients.
"""
from __future__ import annotations
import hashlib
import json
import re
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from .durable_execution_adapter import ContractError, RUN_ID
from .cloud_pipeline_integration import CloudPlan, validate_plan

_STATES = {'PLANNED','SUBMITTED','RUNNING','DONE','FAILED','UNKNOWN'}
_OPERATIONS = ('stage','quarantine','merge')


def _digest(plan):
    validate_plan(plan)
    payload={'run_id':plan.run_id,'source_digest':plan.source_digest,
             'staging_table':plan.staging_table,'quarantine_table':plan.quarantine_table,
             'bronze_table':plan.bronze_table,'stage_job_id':plan.stage_job_id,
             'quarantine_job_id':plan.quarantine_job_id,'merge_job_id':plan.merge_job_id,
             'merge_statement':plan.merge_statement,'staged_records':plan.staged_records,
             'quarantined_records':plan.quarantined_records,'duplicate_records':plan.duplicate_records,
             'input_records':plan.input_records}
    return hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':')).encode()).hexdigest()


class JobJournal:
    """Local durable journal; conservative about crash windows and remote ambiguity."""
    def __init__(self,path):
        p=Path(path)
        if not p.parent.is_dir(): raise ContractError('journal parent missing')
        self.db=sqlite3.connect(str(p),timeout=10,isolation_level=None)
        self.db.execute('PRAGMA busy_timeout=10000')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('''CREATE TABLE IF NOT EXISTS job_plans (
            run_id TEXT PRIMARY KEY, plan_digest TEXT NOT NULL, source_digest TEXT NOT NULL)''')
        self.db.execute('''CREATE TABLE IF NOT EXISTS jobs (
            run_id TEXT NOT NULL, operation TEXT NOT NULL, job_id TEXT NOT NULL,
            state TEXT NOT NULL, location TEXT NOT NULL,
            PRIMARY KEY(run_id,operation), UNIQUE(job_id))''')
    def close(self): self.db.close()
    def register(self,plan,*,location):
        digest=_digest(plan)
        if not isinstance(location,str) or not re.fullmatch(r'[a-z]+(?:-[a-z]+[0-9]+)?',location):
            raise ContractError('explicit BigQuery job location required')
        jobs={'stage':plan.stage_job_id,'quarantine':plan.quarantine_job_id,'merge':plan.merge_job_id}
        if len(set(jobs.values()))!=3: raise ContractError('job IDs not unique')
        with self.db:
            self.db.execute('INSERT OR IGNORE INTO job_plans VALUES (?,?,?)',
                            (plan.run_id,digest,plan.source_digest))
            existing=self.db.execute('SELECT plan_digest,source_digest FROM job_plans WHERE run_id=?',
                                     (plan.run_id,)).fetchone()
            if existing!=(digest,plan.source_digest): raise ContractError('run reused with changed plan')
            for op,jid in jobs.items():
                self.db.execute('INSERT OR IGNORE INTO jobs VALUES (?,?,?,?,?)',
                                (plan.run_id,op,jid,'PLANNED',location))
                row=self.db.execute('SELECT job_id,location FROM jobs WHERE run_id=? AND operation=?',
                                    (plan.run_id,op)).fetchone()
                if row!=(jid,location): raise ContractError('job identity/location drift')
    def get(self,run_id,operation):
        if not isinstance(run_id,str) or not RUN_ID.fullmatch(run_id) or operation not in _OPERATIONS:
            raise ContractError('invalid job key')
        row=self.db.execute('SELECT job_id,state,location FROM jobs WHERE run_id=? AND operation=?',
                            (run_id,operation)).fetchone()
        return None if row is None else dict(zip(('job_id','state','location'),row))
    def mark_submitted(self,run_id,operation):
        """Only call immediately BEFORE any external submission in a future approved runner."""
        with self.db:
            cur=self.db.execute("UPDATE jobs SET state='SUBMITTED' WHERE run_id=? AND operation=? AND state='PLANNED'",
                                (run_id,operation))
            if cur.rowcount!=1: raise ContractError('job already submitted or missing')
    def observe(self,run_id,operation,*,client):
        """Read remote job by pinned ID; unknown, missing or failed never permits retry."""
        row=self.get(run_id,operation)
        if row is None or row['state']=='PLANNED': raise ContractError('job not submitted')
        if client is None: raise ContractError('injected observer required')
        try:
            job=client.get_job(row['job_id'],location=row['location'])
            state=getattr(job,'state',None)
            error=getattr(job,'error_result',None)
            if state=='DONE' and error: state='FAILED'
            if state not in ('PENDING','RUNNING','DONE','FAILED'): state='UNKNOWN'
            if state=='PENDING': state='RUNNING'
        except Exception:
            state='UNKNOWN'
        with self.db:
            self.db.execute('UPDATE jobs SET state=? WHERE run_id=? AND operation=?',
                            (state,run_id,operation))
        return state
    def readiness(self,run_id):
        rows=[self.get(run_id,op) for op in _OPERATIONS]
        if any(x is None for x in rows): raise ContractError('missing job registration')
        if all(x['state']=='DONE' for x in rows): return 'REMOTE_JOBS_DONE_RECONCILIATION_REQUIRED'
        return 'BLOCKED_REMOTE_JOB_UNCERTAINTY'


def quarantine_payload(plan,*,timestamp):
    """Prepare exact schema fields, never write or invent a cloud timestamp."""
    validate_plan(plan)
    if not isinstance(timestamp,datetime) or timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ContractError('timezone-aware timestamp required')
    instant=timestamp.astimezone(timezone.utc).isoformat()
    output=[]
    for row in plan.quarantine_rows:
        output.append({'event_id':row.get('event_id'),'source_path':row['source_path'],
                       'reason':row['reason'],'record_json':row.get('record_json'),
                       '_quarantined_at':instant,'_pipeline_version':row['_pipeline_version']})
    return output


def reconcile_evidence(*,plan,observed_stage_count,observed_quarantine_count,
                       observed_bronze_identities,observed_quarantine_source_paths):
    """Compare independent observations with planned Kafka identities.

    Does not claim which Bronze rows were newly inserted. Missing or unexpected
    remote identities fail closed. Observations must come from a separately
    authorised read-only query, never from the local staging list itself.
    """
    validate_plan(plan)
    for n in (observed_stage_count,observed_quarantine_count):
        if type(n) is not int or n<0: raise ContractError('invalid remote count')
    if observed_stage_count!=plan.staged_records or observed_quarantine_count!=plan.quarantined_records:
        raise ContractError('remote staging/quarantine count mismatch')
    expected={(r['_kafka_topic'],r['_kafka_partition'],r['_kafka_offset']) for r in plan.staging_rows}
    if not isinstance(observed_bronze_identities,(list,set,tuple)):
        raise ContractError('remote Bronze identity evidence required')
    actual=set()
    for identity in observed_bronze_identities:
        if not isinstance(identity,(tuple,list)) or len(identity)!=3 or not isinstance(identity[0],str) or type(identity[1]) is not int or type(identity[2]) is not int:
            raise ContractError('invalid remote Bronze identity')
        actual.add(tuple(identity))
    if len(actual)!=len(observed_bronze_identities) or actual!=expected:
        raise ContractError('remote Bronze identities differ from source')
    if not isinstance(observed_quarantine_source_paths,(list,tuple)):
        raise ContractError('remote quarantine source evidence required')
    expected_paths=sorted(row['source_path'] for row in plan.quarantine_rows)
    if sorted(observed_quarantine_source_paths)!=expected_paths:
        raise ContractError('remote quarantine lineage mismatch')
    if plan.input_records!=plan.staged_records+plan.quarantined_records+plan.duplicate_records:
        raise ContractError('source accounting mismatch')
    return {'status':'REMOTE_EVIDENCE_CONSISTENT_NOT_LIVE_ACCEPTED',
            'source_records':plan.input_records,'bronze_identities_verified':len(expected),
            'quarantine_verified':len(expected_paths),'duplicate_records':plan.duplicate_records}


def execution_gate(*,approval=None):
    raise ContractError('LIVE_BLOCKED: this stage contains no approved cloud executor')
