"""Stage 14F-B offline cloud adapter contracts. No SDK clients constructed.

These are injected-client building blocks, NOT an authorised live runner.
The caller must implement durable job lifecycle and independently approve cloud use.
"""
from __future__ import annotations
import hashlib
import re
from dataclasses import dataclass
from typing import Any

class AdapterContractError(ValueError):
    pass

_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,1023}$")
_PROJECT = re.compile(r"^[a-z][a-z0-9-]{4,61}[a-z0-9]$")
_GCS = re.compile(r"^raw/v1/[a-zA-Z0-9_./=-]+\.avro$")

def table_id(value: str) -> str:
    if not isinstance(value,str) or len(value)>300: raise AdapterContractError('invalid table id')
    bits=value.split('.')
    if len(bits)!=3 or not _PROJECT.fullmatch(bits[0]) or not all(_IDENTIFIER.fullmatch(x) for x in bits[1:]):
        raise AdapterContractError('table must be project.dataset.table')
    return value

def job_id(run_id: str, operation: str, payload: str) -> str:
    if not isinstance(run_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,96}',run_id):
        raise AdapterContractError('invalid run id')
    if operation not in {'stage','merge','reconcile'}: raise AdapterContractError('invalid operation')
    if not isinstance(payload,str) or not payload: raise AdapterContractError('missing payload')
    digest=hashlib.sha256((run_id+'\x00'+operation+'\x00'+payload).encode()).hexdigest()[:32]
    return f'pdm_{operation}_{digest}'

@dataclass(frozen=True)
class GCSObject:
    bucket: str
    name: str
    generation: int
    content: bytes

class InjectedGCSReader:
    def __init__(self, client: Any, bucket: str):
        if client is None or bucket!='dp-pdm-ai-platform-dev':
            raise AdapterContractError('injected client and canonical bucket required')
        self.client,self.bucket=client,bucket
    def read(self, name: str, generation: int) -> GCSObject:
        if not isinstance(name,str) or not _GCS.fullmatch(name) or '..' in name.split('/'):
            raise AdapterContractError('only canonical raw/v1 Avro paths allowed')
        if type(generation) is not int or generation<=0: raise AdapterContractError('generation must be positive integer')
        blob=self.client.bucket(self.bucket).blob(name,generation=generation)
        content=blob.download_as_bytes()
        if not isinstance(content,bytes): raise AdapterContractError('GCS client returned non-bytes')
        return GCSObject(self.bucket,name,generation,content)

class InjectedBigQueryAdapter:
    """Injectable SDK-compatible facade; no network calls until methods invoked.

    All invocation remains caller-controlled; this module deliberately has no CLI.
    """
    def __init__(self, client: Any, staging: str, bronze: str):
        if client is None: raise AdapterContractError('injected client required')
        self.staging,self.bronze=table_id(staging),table_id(bronze)
        if self.staging==self.bronze: raise AdapterContractError('staging must differ from bronze')
        self.client=client
    def stage(self, run_id: str, source_fingerprint: str, rows: list[dict], *, job_config: Any) -> str:
        if not isinstance(rows,list) or not rows or any(not isinstance(x,dict) for x in rows):
            raise AdapterContractError('nonempty validated row list required')
        if job_config is None: raise AdapterContractError('explicit staging job config required')
        jid=job_id(run_id,'stage',source_fingerprint)
        job=self.client.load_table_from_json(rows,self.staging,job_id=jid,job_config=job_config)
        job.result()
        if getattr(job,'errors',None): raise AdapterContractError('staging job errors')
        return jid
    def merge(self, run_id: str, sql: str, *, job_config: Any) -> str:
        if not isinstance(sql,str) or not sql.lstrip().upper().startswith('MERGE '):
            raise AdapterContractError('MERGE SQL contract required')
        if job_config is None: raise AdapterContractError('explicit merge job config required')
        jid=job_id(run_id,'merge',sql)
        job=self.client.query(sql,job_id=jid,job_config=job_config)
        job.result()
        if getattr(job,'errors',None): raise AdapterContractError('MERGE job errors')
        return jid
    def count(self, run_id: str, table: str, *, job_config: Any) -> int:
        target=table_id(table)
        if target not in (self.staging,self.bronze): raise AdapterContractError('unapproved count target')
        if job_config is None: raise AdapterContractError('explicit count job config required')
        sql=f'SELECT COUNT(*) AS n FROM `{target}`'
        job=self.client.query(sql,job_id=job_id(run_id,'reconcile',sql),job_config=job_config)
        result=list(job.result())
        if getattr(job,'errors',None) or len(result)!=1: raise AdapterContractError('invalid count response')
        n=result[0]['n']
        if type(n) is not int or n<0: raise AdapterContractError('invalid count')
        return n
