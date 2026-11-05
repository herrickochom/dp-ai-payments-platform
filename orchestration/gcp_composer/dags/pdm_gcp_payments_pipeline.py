"""GCP payment orchestration contract.

Import-safe and deliberately unscheduled.

This DAG must not submit Dataflow jobs until bounded
manifest admission, replay protection, durable recovery,
and cloud execution approval are implemented.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow.sdk import DAG, task


DAG_ID = "pdm_gcp_payments_pipeline"

BLOCKER = (
    "GCP payment execution is disabled. "
    "Bounded manifest admission, stable idempotency, "
    "durable recovery and deployment approval are required."
)


with DAG(
    dag_id=DAG_ID,
    description="Governed GCP payment ingestion contract",
    start_date=datetime(2026, 10, 27, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 0},
    tags=["pdm", "gcp", "payments", "not-deployable"],
) as dag:

    @task(retries=0)
    def admission_gate() -> None:
        """Fail closed until the execution contract is implemented."""
        raise RuntimeError(BLOCKER)

    @task(retries=0)
    def verify_bronze_manifest() -> None:
        """Future bounded, immutable manifest verification."""
        raise RuntimeError(BLOCKER)

    @task(retries=0)
    def submit_dataflow() -> None:
        """Future idempotent Dataflow submission."""
        raise RuntimeError(BLOCKER)

    @task(retries=0)
    def observe_dataflow() -> None:
        """Future durable execution-state observation."""
        raise RuntimeError(BLOCKER)

    @task(retries=0)
    def verify_bigquery_landing() -> None:
        """Future BigQuery landing reconciliation."""
        raise RuntimeError(BLOCKER)

    @task(retries=0)
    def record_acceptance() -> None:
        """Future acceptance evidence recording."""
        raise RuntimeError(BLOCKER)

    (
        admission_gate()
        >> verify_bronze_manifest()
        >> submit_dataflow()
        >> observe_dataflow()
        >> verify_bigquery_landing()
        >> record_acceptance()
    )
