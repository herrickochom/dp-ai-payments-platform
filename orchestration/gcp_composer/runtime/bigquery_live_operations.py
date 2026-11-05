"""Read-only BigQuery lineage verifier. Never creates evidence itself.

A separate Dataflow-side producer must write the lineage table atomically with
its output protocol; this verifier refuses to manufacture missing proof.
"""
from __future__ import annotations
import hashlib
import json
import re

class EvidenceUnavailable(RuntimeError): pass

class BigQueryLineageVerifier:
    def __init__(self, client, *, project: str, dataset: str, table='payment_execution_lineage', max_bytes=100_000_000):
        if not all(re.fullmatch(r'[A-Za-z0-9_\-]+', x or '') for x in (project,dataset,table)):
            raise ValueError('invalid BigQuery identifier')
        if not isinstance(max_bytes,int) or max_bytes < 1: raise ValueError('invalid query budget')
        self.client,self.project,self.dataset,self.table,self.max_bytes=client,project,dataset,table,max_bytes

    def verify(self, claim: dict, job: dict):
        if job.get('state') != 'JOB_STATE_DONE' or not job.get('id'):
            return None
        required=('execution_id','attempt_number','digest')
        if any(k not in claim for k in required): raise EvidenceUnavailable('missing execution identity')
        if not re.fullmatch(r'[0-9a-f]{64}',str(claim['digest'])): raise EvidenceUnavailable('invalid spec digest')
        query=(f'SELECT event_id, output_sha256 FROM `{self.project}.{self.dataset}.{self.table}` '
               'WHERE execution_id=@execution_id AND attempt_number=@attempt_number '
               'AND dataflow_job_id=@job_id AND spec_digest=@spec_digest')
        try:
            from google.cloud import bigquery
            config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter('execution_id','STRING',claim['execution_id']),
                                  bigquery.ScalarQueryParameter('attempt_number','INT64',claim['attempt_number']),
                                  bigquery.ScalarQueryParameter('job_id','STRING',job['id']),
                                  bigquery.ScalarQueryParameter('spec_digest','STRING',claim['digest'])],
                maximum_bytes_billed=self.max_bytes,use_query_cache=False)
            rows=list(self.client.query(query,job_config=config).result())
        except Exception as exc:
            raise EvidenceUnavailable('BigQuery verification unavailable') from exc
        if not rows: return None
        # Row existence alone cannot prove completeness or producer identity.
        # No trusted expected manifest is supplied by the existing recovery claim.
        # Fail closed until a producer-signed manifest and expected output set are
        # bound to the execution and verified independently.
        raise EvidenceUnavailable('lineage rows exist, but independent expected-output attestation is unavailable')
        objects=[]
        for row in rows:
            event_id=row['event_id']; digest=row['output_sha256']
            if not event_id or not re.fullmatch(r'[0-9a-f]{64}',str(digest)):
                raise EvidenceUnavailable('invalid lineage row')
            objects.append((str(event_id),str(digest)))
        if len(objects)!=len(set(x[0] for x in objects)):
            raise EvidenceUnavailable('duplicate event lineage')
        canonical=json.dumps(sorted(objects),separators=(',',':')).encode()
        # Evidence is a digest of observed lineage, NOT a proof that expected events were delivered.
        return hashlib.sha256(canonical).hexdigest()
