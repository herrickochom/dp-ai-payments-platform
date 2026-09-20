"""
Governed downstream orchestration for the DP-AI Payments Platform.

Kafka producers and consumers remain event-driven and are not started
or stopped by this DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, task

from platform_jobs import run_scheduled_job
from transform_jobs import create_transform_run, submit_and_wait_batch


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

    @task(execution_timeout=timedelta(minutes=5), pool="transform_execution")
    def create_runtime_run():
        from airflow.sdk import get_current_context
        context = get_current_context()
        return create_transform_run(f"{context['dag_run'].run_id}:transform")

    @task(execution_timeout=timedelta(minutes=60), pool="transform_execution", retries=1)
    def transform_batch(run_id: str, batch_id: str):
        from airflow.sdk import get_current_context
        context = get_current_context()
        return submit_and_wait_batch(run_id, batch_id, f"{context['dag_run'].run_id}:{batch_id}")

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
    run_id = create_runtime_run()
    ml = transform_batch.override(task_id="transform_c4_ml_01")(run_id, "C4_ML_01")
    raw_batch = transform_batch.override(task_id="transform_c4_raw_02")(run_id, "C4_RAW_02")
    rid_foundation = transform_batch.override(task_id="transform_c4_rid_foundation_03")(run_id, "C4_RID_FOUNDATION_03")
    ordinary_foundation = transform_batch.override(task_id="transform_c4_ord_foundation_04")(run_id, "C4_ORD_FOUNDATION_04")
    bridge = transform_batch.override(task_id="transform_c4_ord_bridge_05")(run_id, "C4_ORD_BRIDGE_05")
    pre_alert = transform_batch.override(task_id="transform_c4_ord_pre_alert_06")(run_id, "C4_ORD_PRE_ALERT_06")
    rid_alerts = transform_batch.override(task_id="transform_c4_rid_alerts_07")(run_id, "C4_RID_ALERTS_07")
    pre_signal = transform_batch.override(task_id="transform_c4_ord_pre_signal_08")(run_id, "C4_ORD_PRE_SIGNAL_08")
    rid_signals = transform_batch.override(task_id="transform_c4_rid_signals_09")(run_id, "C4_RID_SIGNALS_09")
    transform = transform_batch.override(task_id="transform_c4_ord_downstream_10")(run_id, "C4_ORD_DOWNSTREAM_10")
    dq = data_quality()
    features = ml_feature_generation()
    scoring = ml_default_risk_scoring()
    reconcile = reconciliation()
    accepted = acceptance()

    preflight >> raw >> [ml, raw_batch]
    raw_batch >> [rid_foundation, ordinary_foundation]
    [ml, raw_batch, rid_foundation, ordinary_foundation] >> bridge
    [ordinary_foundation, bridge] >> pre_alert
    [rid_foundation, ordinary_foundation, bridge] >> rid_alerts
    [ordinary_foundation, bridge, pre_alert] >> pre_signal
    rid_alerts >> rid_signals
    [ordinary_foundation, bridge, pre_alert, pre_signal, rid_signals] >> transform
    transform >> dq >> features >> scoring >> reconcile >> accepted
