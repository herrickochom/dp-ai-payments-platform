"""Read-only BigQuery evidence inspection; no fabricated output manifests."""
import re
from recovery_contracts import UnsafeToDeploy, verified_evidence

_IDENTIFIER=re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

class BigQueryEvidenceVerifier:
    def __init__(self, client, *, project, dataset, table, max_bytes_billed=10_000_000):
        if not project or not all(_IDENTIFIER.fullmatch(x) for x in (dataset,table)):
            raise ValueError('invalid BigQuery scope')
        if not 1 <= max_bytes_billed <= 1_000_000_000: raise ValueError('invalid query budget')
        self.client,self.project,self.dataset,self.table=client,project,dataset,table
        self.max_bytes_billed=max_bytes_billed

    def inspect_outputs(self, identity, job_id, *, manifest):
        """Only inspect; caller supplies independently verified manifest evidence.

        Existing payment_events table has no execution_id/attempt_number columns.
        Therefore table existence/count alone CANNOT bind records to an execution.
        """
        identity.validate()
        if identity.project!=self.project: raise UnsafeToDeploy('project mismatch')
        if not isinstance(manifest,dict) or manifest.get('manifest_verified') is not True:
            raise UnsafeToDeploy('independent manifest verification required')
        for k,v in {'execution_id':identity.execution_id,'attempt_number':identity.attempt,
                    'dataflow_job_id':job_id,'spec_digest':identity.spec_digest}.items():
            if manifest.get(k)!=v: raise UnsafeToDeploy('manifest identity mismatch')
        # BigQuery table metadata can confirm existence but not execution lineage.
        table=self.client.get_table(f'{self.project}.{self.dataset}.{self.table}')
        if getattr(table,'num_rows',None) is None:
            raise UnsafeToDeploy('BigQuery table metadata unavailable')
        return {**manifest,'bigquery_table':f'{self.project}.{self.dataset}.{self.table}',
                'bigquery_verified':False,'verification_blocker':
                'table schema lacks execution_id and attempt_number lineage'}

    def verify(self, claim, job):
        # Fail closed until execution-bound evidence is implemented.
        return None
