"""Automatic, bounded reconciliation DAG; deliberately cannot submit jobs.

Requires Composer 3 / Airflow 3, provider SDK, and Secret Manager-managed
PostgreSQL credentials configured as an Airflow connection. No hardcoded secrets.
"""
from __future__ import annotations
from datetime import datetime,timezone
from airflow.sdk import DAG,task

with DAG(dag_id='pdm_gcp_reconciliation',start_date=datetime(2026,10,29,tzinfo=timezone.utc),
         schedule='*/5 * * * *',catchup=False,max_active_runs=1,max_active_tasks=1,
         default_args={'retries':0},tags=['pdm','gcp','reconciliation']) as dag:
    @task(retries=0)
    def reconcile():
        import os,sys
        from pathlib import Path
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from google.cloud import dataflow_v1beta3,bigquery
        # Package this runtime directory with the DAG in the Composer deployment.
        runtime=Path(__file__).resolve().parent.parent/'runtime'
        if not runtime.is_dir(): raise RuntimeError('Stage 10 runtime not packaged')
        sys.path.insert(0,str(runtime))
        from transactional_recovery import RecoveryStore
        from dataflow_provider import ReadOnlyDataflowProvider
        from bigquery_live_operations import BigQueryLineageVerifier
        from live_reconciler import reconcile_once
        project=os.environ['PDM_GCP_PROJECT_ID']; region=os.environ['PDM_GCP_REGION']
        dataset=os.environ['PDM_BQ_DATASET']; table=os.environ.get('PDM_BQ_LINEAGE_TABLE','payment_execution_lineage')
        # Airflow connection must be configured via Secret Manager backend.
        conn_id=os.environ.get('PDM_POSTGRES_CONN_ID','pdm_postgres')
        def connect(): return PostgresHook(postgres_conn_id=conn_id).get_conn()
        store=RecoveryStore(connect)
        provider=ReadOnlyDataflowProvider(dataflow_v1beta3.JobsV1Beta3Client(),project=project,region=region)
        verifier=BigQueryLineageVerifier(bigquery.Client(project=project),project=project,dataset=dataset,table=table)
        return reconcile_once(store,provider,verifier)
    reconcile()
