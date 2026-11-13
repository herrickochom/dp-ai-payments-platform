"""14F-J: execute independent BigQuery reconciliation queries with injected client.

No client construction, no cloud credentials, no live entrypoint, no DML or DDL.
The caller must obtain separate approval before supplying a real SDK client.
The stage's executable entrypoint accepts OFFLINE FAKE clients only.
"""
from __future__ import annotations
import re
from .durable_execution_adapter import ContractError
from .cloud_pipeline_integration import validate_plan
from .remote_job_recovery import reconcile_evidence

_ID = re.compile(r'[A-Za-z][A-Za-z0-9_-]{0,127}\.[A-Za-z_][A-Za-z0-9_]{0,1023}\.[A-Za-z_][A-Za-z0-9_]{0,1023}\Z')

def _table(name):
    if not isinstance(name,str) or not _ID.fullmatch(name) or '`' in name:
        raise ContractError('unsafe BigQuery table identifier')
    return f'`{name}`'

def query_plan(plan):
    """Exact SQL; staging and quarantine are run-specific; Bronze is identity-scoped.

    No BigQuery job is submitted here. Avoid global Bronze COUNT delta, which is
    unsound under concurrent writers. Quarantine rows have no run field but live
    in a unique run-isolated table.
    """
    validate_plan(plan)
    stage=_table(plan.staging_table)
    quarantine=_table(plan.quarantine_table)
    bronze=_table(plan.bronze_table)
    return {
      'stage': f'SELECT COUNT(*) AS n FROM {stage} WHERE _ingest_run_id = @run_id',
      'quarantine': f'SELECT source_path FROM {quarantine} ORDER BY source_path',
      'bronze': ('WITH expected AS (SELECT DISTINCT _kafka_topic, _kafka_partition, _kafka_offset '
                 f'FROM {stage} WHERE _ingest_run_id = @run_id) '
                 'SELECT b._kafka_topic, b._kafka_partition, b._kafka_offset '
                 f'FROM {bronze} b JOIN expected e ON '
                 'b._kafka_topic=e._kafka_topic AND b._kafka_partition=e._kafka_partition '
                 'AND b._kafka_offset=e._kafka_offset'),
    }

def _run_query(client, sql, *, config, location):
    job=client.query(sql, job_config=config, location=location)
    return list(job.result())

def execute_offline_reconciliation(*,plan,client,configs,location='europe-west2'):
    """Actually run the three SELECTs against an injected offline fake.

    A fake must expose the explicit sentinel; real BigQuery clients are refused.
    Query parameters must be supplied in stage and Bronze job configs; no SQL
    string interpolation of run IDs. No successful result means production accepted.
    """
    validate_plan(plan)
    if not getattr(client,'__stage14fj_offline_fake__',False) or type(client).__module__.startswith('google.'):
        raise ContractError('LIVE_BLOCKED: only injected offline fake query clients')
    if location!='europe-west2': raise ContractError('unapproved location')
    if not isinstance(configs,dict) or set(configs)!= {'stage','quarantine','bronze'} or any(configs[k] is None for k in configs):
        raise ContractError('three explicit read-only query configurations required')
    sql=query_plan(plan)
    try:
        stage_rows=_run_query(client,sql['stage'],config=configs['stage'],location=location)
        quarantine_rows=_run_query(client,sql['quarantine'],config=configs['quarantine'],location=location)
        bronze_rows=_run_query(client,sql['bronze'],config=configs['bronze'],location=location)
        if len(stage_rows)!=1 or type(stage_rows[0]['n']) is not int:
            raise ContractError('invalid stage count response')
        paths=[r['source_path'] for r in quarantine_rows]
        identities=[(r['_kafka_topic'],r['_kafka_partition'],r['_kafka_offset']) for r in bronze_rows]
        result=reconcile_evidence(plan=plan,observed_stage_count=stage_rows[0]['n'],
            observed_quarantine_count=len(paths),observed_bronze_identities=identities,
            observed_quarantine_source_paths=paths)
    except ContractError: raise
    except Exception as exc: raise ContractError('remote query evidence incomplete; acceptance blocked') from exc
    return {**result,'execution_mode':'OFFLINE_FAKE_QUERY','production_accepted':False,'cloud_calls':0}
