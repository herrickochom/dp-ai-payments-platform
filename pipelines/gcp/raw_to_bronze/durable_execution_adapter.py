"""Offline-only durable run ledger and injected-client adapter; no cloud client imports."""
import hashlib
import json
import re
import sqlite3
from pathlib import Path

RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,95}$")

class ContractError(ValueError):
    pass

class DurableLedger:
    def __init__(self, path):
        self.path = Path(path)
        if not self.path.parent.is_dir():
            raise ContractError("ledger directory does not exist")
        self.db = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        self.db.execute("PRAGMA busy_timeout=10000")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('PREPARED','MERGING','MERGED','COMPLETE','FAILED')),
            expected INTEGER NOT NULL CHECK(expected >= 0),
            before_count INTEGER, after_count INTEGER, error TEXT
        )""")

    def close(self):
        self.db.close()

    def get(self, run_id):
        row = self.db.execute("SELECT fingerprint,state,expected,before_count,after_count,error FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return None if row is None else dict(zip(('fingerprint','state','expected','before_count','after_count','error'), row))

    def prepare(self, run_id, fingerprint, expected):
        if not isinstance(run_id,str) or not RUN_ID.fullmatch(run_id):
            raise ContractError("invalid run id")
        if not isinstance(fingerprint,str) or not re.fullmatch(r'[a-f0-9]{64}',fingerprint):
            raise ContractError("invalid fingerprint")
        if type(expected) is not int or expected < 0:
            raise ContractError("invalid expected count")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO runs(run_id,fingerprint,state,expected) VALUES(?,?,'PREPARED',?)",(run_id,fingerprint,expected))
        existing=self.get(run_id)
        if existing['fingerprint']!=fingerprint or existing['expected']!=expected:
            raise ContractError("run id reused with changed input")
        return existing

    def transition(self, run_id, allowed, state, *, before=None, after=None, error=None):
        if state not in ('PREPARED','MERGING','MERGED','COMPLETE','FAILED'):
            raise ContractError('invalid state')
        if before is not None and (type(before) is not int or before<0):
            raise ContractError('invalid before count')
        if after is not None and (type(after) is not int or after<0):
            raise ContractError('invalid after count')
        with self.db:
            cur=self.db.execute("UPDATE runs SET state=?,before_count=COALESCE(?,before_count),after_count=COALESCE(?,after_count),error=? WHERE run_id=? AND state IN ("+','.join('?' for _ in allowed)+")",(state,before,after,error,run_id,*allowed))
        if cur.rowcount!=1:
            raise ContractError('invalid state transition')


def fingerprint_records(records):
    """Order-independent fingerprint for already validated staging identities."""
    keys=[]
    for row in records:
        identity=(row.get('_kafka_topic'),row.get('_kafka_partition'),row.get('_kafka_offset'))
        if not isinstance(identity[0],str) or not identity[0] or type(identity[1]) is not int or identity[1]<0 or type(identity[2]) is not int or identity[2]<0:
            raise ContractError('invalid Kafka identity')
        keys.append(identity)
    if len(set(keys))!=len(keys):
        raise ContractError('duplicate Kafka identity in staging')
    return hashlib.sha256(json.dumps(sorted(keys),separators=(',',':')).encode()).hexdigest()


class OfflineAdapter:
    """Injected fake client only. SQL must be supplied from reviewed Stage14E.2 contract.

    This adapter intentionally does not build a real BigQuery client or issue queries.
    A production executor and ambiguity-safe recovery remain future work.
    """
    def __init__(self, ledger, client):
        self.ledger=ledger
        self.client=client

    def execute(self, run_id, rows, merge_sql):
        if not isinstance(merge_sql,str) or not merge_sql.strip().upper().startswith('MERGE '):
            raise ContractError('reviewed MERGE SQL required')
        fp=fingerprint_records(rows)
        state=self.ledger.prepare(run_id,fp,len(rows))
        if state['state']=='COMPLETE':
            return 'ALREADY_COMPLETE'
        if state['state'] in ('MERGING','MERGED','FAILED'):
            raise ContractError('ambiguous prior execution; manual offline reconciliation required')
        if state['state']!='PREPARED':
            raise ContractError('unexpected ledger state')
        # No external calls: fake client is supplied by the caller/test.
        before=self.client.count_bronze()
        if type(before) is not int or before<0:
            raise ContractError('invalid sink count')
        self.client.stage(run_id,rows)
        staged=self.client.count_staging(run_id)
        if type(staged) is not int or staged!=len(rows):
            raise ContractError('staging count mismatch')
        self.ledger.transition(run_id,('PREPARED',),'MERGING',before=before)
        try:
            self.client.merge(merge_sql)
        except Exception as exc:
            self.ledger.transition(run_id,('MERGING',),'FAILED',error=type(exc).__name__)
            raise
        self.ledger.transition(run_id,('MERGING',),'MERGED')
        after=self.client.count_bronze()
        if type(after) is not int or after<before or after-before>len(rows):
            raise ContractError('sink count reconciliation failed')
        self.ledger.transition(run_id,('MERGED',),'COMPLETE',after=after)
        return 'COMPLETE'
