"""Stage 8–9 recurring handoff DAG.

Deployment and activation are separate approval gates.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from scheduled_recovery_entrypoint import (
    run_scheduled_handoff,
)

with DAG(
    dag_id="pdm_stage89_recurring_handoff",
    description="Bounded Stage 8 to Stage 9 handoff",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 11, 20),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "payments-platform",
        "retries": 0,
        "execution_timeout": timedelta(minutes=4),
    },
    is_paused_upon_creation=True,
    tags=["payments", "stage8", "stage9"],
) as dag:

    prepare_handoff = PythonOperator(
        task_id="prepare_transactional_handoff",
        python_callable=run_scheduled_handoff,
    )
