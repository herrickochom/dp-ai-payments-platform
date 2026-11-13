"""Unified Stage 14F-F offline runner. Cloud execution is intentionally absent.

Runs actual Avro parsing, canonical validation, durable offline orchestration,
quarantine accounting and evidence emission. No SDK or cloud access.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
from .offline_workflow import execute_prefetched_offline, prepare_objects
from .durable_execution_adapter import DurableLedger, ContractError

class InMemoryBronzeSink:
    """Test-only sink; never a BigQuery replacement."""
    def __init__(self):
        self.bronze={}; self.staged=[]
    def count_bronze(self): return len(self.bronze)
    def stage(self,run_id,rows):
        self.staged=list(rows)
    def count_staging(self,run_id): return len(self.staged)
    def merge(self,sql):
        for row in self.staged:
            identity=(row['_kafka_topic'],row['_kafka_partition'],row['_kafka_offset'])
            self.bronze.setdefault(identity,row)

def execute_local(*, manifest, ledger_path, run_id, staging_table,
                  bronze_table, sink, evidence_path):
    """Read only explicitly listed local Avro files; no cloud clients."""
    if not isinstance(manifest,list) or not manifest:
        raise ContractError('explicit nonempty local manifest required')
    from .durable_execution_adapter import RUN_ID
    if not isinstance(run_id, str) or not RUN_ID.fullmatch(run_id):
        raise ContractError('invalid run id')
    if sink is None: raise ContractError('injected offline sink required')
    target=Path(evidence_path)
    if not target.parent.is_dir() or target.exists():
        raise ContractError('evidence parent must exist; no overwrites')
    objects=[]; versions=[]
    for item in manifest:
        if not isinstance(item,dict) or set(item)!={'path','sha256'}:
            raise ContractError('manifest item requires path and sha256')
        path=Path(item['path'])
        if not path.is_file() or path.suffix!='.avro' or path.is_symlink():
            raise ContractError('local Avro regular file required')
        raw=path.read_bytes()
        digest=hashlib.sha256(raw).hexdigest()
        if digest!=item['sha256']:
            raise ContractError('source digest mismatch')
        objects.append((str(path),raw))
        versions.append({'path':str(path),'sha256':digest,'size':len(raw)})
    # Full parse and validation before touching the ledger.
    prepared=prepare_objects(objects)
    if not prepared['staging_rows']:
        raise ContractError('no valid records: quarantine-only runs not implemented')
    ledger=DurableLedger(ledger_path)
    try:
        result=execute_prefetched_offline(objects=objects,ledger=ledger,
            fake_client=sink,run_id=run_id,staging_table=staging_table,
            bronze_table=bronze_table)
        state=ledger.get(run_id)
    finally:
        ledger.close()
    evidence={'stage':'14F-F','mode':'OFFLINE_ONLY','run_id':run_id,
              'sources':versions,'status':result['status'],
              'reconciliation':result['reconciliation'],
              'quarantine_count':len(result['quarantine']),
              'ledger_state':state['state'],
              'cloud_calls':0,'cloud_writes':0,
              'production_accepted':False}
    target.write_text(json.dumps(evidence,indent=2,sort_keys=True)+'\n')
    return evidence

def main(argv=None):
    parser=argparse.ArgumentParser(description='Offline-only Stage 14F-F')
    parser.add_argument('--manifest',required=True)
    parser.add_argument('--ledger',required=True)
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--staging-table',required=True)
    parser.add_argument('--bronze-table',required=True)
    parser.add_argument('--evidence',required=True)
    args=parser.parse_args(argv)
    manifest=json.loads(Path(args.manifest).read_text())
    result=execute_local(manifest=manifest,ledger_path=args.ledger,run_id=args.run_id,
        staging_table=args.staging_table,bronze_table=args.bronze_table,
        sink=InMemoryBronzeSink(),evidence_path=args.evidence)
    print(json.dumps(result,indent=2))

if __name__=='__main__': main()
