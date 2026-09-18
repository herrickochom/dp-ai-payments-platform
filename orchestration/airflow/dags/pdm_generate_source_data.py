"""
Manual synthetic payment-source generation.

This DAG is intentionally separate from the normal platform pipeline.

It does not:
- run on a schedule;
- clean existing source data;
- start payment-producer;
- publish to Kafka automatically.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, get_current_context, task
from airflow.sdk.definitions.param import Param

from platform_jobs import generate_source_data


with DAG(
    dag_id="pdm_generate_source_data",
    description=(
        "Explicit on-demand synthetic payment source generation"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "record_count": Param(
            250,
            type="integer",
            minimum=1,
            maximum=10000,
            description=(
                "Number of synthetic payment source records"
            ),
        ),
    },
    tags=[
        "pdm",
        "payments",
        "manual",
        "synthetic-data",
    ],
) as dag:

    @task(execution_timeout=timedelta(minutes=20))
    def generate():
        context = get_current_context()

        record_count = int(
            context["params"]["record_count"]
        )

        return generate_source_data(
            record_count=record_count,
            requested_by="airflow-manual-trigger",
        )

    generate()
