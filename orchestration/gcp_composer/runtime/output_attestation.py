"""Independent expected-vs-observed output manifest reconciliation.

Not a Dataflow producer: the Beam pipeline must emit execution-bound records
and an independently verifiable, immutable expected manifest before this can
attest output completeness. Never treat lineage row existence as completion.
"""
from __future__ import annotations
import hashlib,json,re
class AttestationBlocked(RuntimeError):pass

def canonical_digest(rows):
    if not isinstance(rows,list):raise AttestationBlocked('expected list')
    normalized=[]
    for row in rows:
        if not isinstance(row,dict) or set(row)!={'event_id','output_sha256'}:
            raise AttestationBlocked('invalid event manifest')
        if not isinstance(row['event_id'],str) or not row['event_id'] or \
           not re.fullmatch(r'[0-9a-f]{64}',row['output_sha256']):
            raise AttestationBlocked('invalid event digest')
        normalized.append((row['event_id'],row['output_sha256']))
    if len({x[0] for x in normalized})!=len(normalized):
        raise AttestationBlocked('duplicate event ID')
    return hashlib.sha256(json.dumps(sorted(normalized),separators=(',',':')).encode()).hexdigest()

def attest(*,expected,observed,manifest_verified,expected_identity,observed_identity):
    if manifest_verified is not True:
        raise AttestationBlocked('independently verified expected manifest required')
    if expected_identity != observed_identity or not isinstance(expected_identity,dict) or \
       set(expected_identity) != {'execution_id','attempt_number','job_id','spec_digest'}:
        raise AttestationBlocked('execution identity mismatch')
    if not expected or len(expected)!=len(observed):
        raise AttestationBlocked('missing or extra outputs')
    a=canonical_digest(expected); b=canonical_digest(observed)
    if a!=b:raise AttestationBlocked('expected and observed outputs differ')
    return a
