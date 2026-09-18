"""
Governed downstream orchestration for the DP-AI Payments Platform.

Kafka producers and consumers remain event-driven and are not started
or stopped by this DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, task

from platform_jobs import run_scheduled_job


DEFAULT_ARGS = {
    "owner": "dp-ai-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="pdm_platform_pipeline",
    description=(
        "Governed downstream DP-AI Payments Platform pipeline"
    ),
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=[
        "pdm",
        "payments",
        "data-platform",
        "governed",
    ],
) as dag:

    @task(execution_timeout=timedelta(minutes=5))
    def platform_preflight():
        return run_scheduled_job("platform_preflight")

    @task(execution_timeout=timedelta(minutes=5))
    def raw_readiness():
        return run_scheduled_job("raw_readiness")

    @task(execution_timeout=timedelta(minutes=30))
    def lakehouse_transform():
        return run_scheduled_job("lakehouse_transform")

    @task(execution_timeout=timedelta(minutes=15))
    def data_quality():
        return run_scheduled_job("data_quality")

    @task(execution_timeout=timedelta(minutes=20))
    def ml_feature_generation():
        return run_scheduled_job(
            "ml_feature_generation"
        )

    @task(execution_timeout=timedelta(minutes=20))
    def ml_default_risk_scoring():
        return run_scheduled_job(
            "ml_default_risk_scoring"
        )

    @task(execution_timeout=timedelta(minutes=10))
    def reconciliation():
        return run_scheduled_job("reconciliation")

    @task(execution_timeout=timedelta(minutes=10))
    def acceptance():
        return run_scheduled_job("acceptance")

    preflight = platform_preflight()
    raw = raw_readiness()
    transform = lakehouse_transform()
    dq = data_quality()
    features = ml_feature_generation()
    scoring = ml_default_risk_scoring()
    reconcile = reconciliation()
    accepted = acceptance()

    (
        preflight
        >> raw
        >> transform
        >> dq
        >> features
        >> scoring
        >> reconcile
        >> accepted
    )
