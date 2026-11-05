"""Scheduled read-only cost watchdog. Never issues Dataflow mutation.
Deployment requires Composer runtime packaging and Airflow connection setup.
"""
from datetime import datetime,timezone
from airflow.sdk import DAG,task

with DAG(dag_id='pdm_gcp_cost_watchdog',
         start_date=datetime(2026,10,29,tzinfo=timezone.utc),
         schedule='*/5 * * * *',catchup=False,max_active_runs=1,
         max_active_tasks=1,default_args={'retries':0},
         tags=['pdm','gcp','safety','read-only']) as dag:
    @task(retries=0)
    def inspect():
        import os,sys,json
        from pathlib import Path
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from google.cloud import dataflow_v1beta3
        runtime=Path(__file__).resolve().parent.parent/'runtime'
        if not runtime.is_dir():
            raise RuntimeError('Stage 10 runtime not packaged')
        sys.path.insert(0,str(runtime))
        from ledger_watchdog import inspect_ledger
        project=os.environ['PDM_GCP_PROJECT_ID']
        region=os.environ['PDM_GCP_REGION']
        if project!='project-e48f194e-f44f-458d-b4f' or region!='europe-west2':
            raise RuntimeError('unexpected project or region')
        conn_id=os.environ.get('PDM_POSTGRES_CONN_ID','pdm_postgres')
        def connect():
            return PostgresHook(postgres_conn_id=conn_id).get_conn()
        client=dataflow_v1beta3.JobsV1Beta3Client()
        results=inspect_ledger(client,connect,limit=100)
        # Avoid logging execution IDs or credentials.
        counts={}
        for item in results:
            counts[item['decision']]=counts.get(item['decision'],0)+1
        print(json.dumps({'watchdog':'read-only','counts':counts},sort_keys=True))
        if any(x['decision']=='BLOCKED' for x in results):
            raise RuntimeError('watchdog blocked: review provider/ledger evidence')
        return counts
    inspect()
