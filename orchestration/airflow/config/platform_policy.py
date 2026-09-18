"""
DP-AI Payments Platform Airflow execution policy.

This module records operations that scheduled DAGs are forbidden to invoke.
The same boundary is enforced by DAG tests and the platform job runner.
"""

FORBIDDEN_OPERATIONS = frozenset(
    {
        "kafka_offset_reset",
        "live_dlq_replay",
        "destructive_lifecycle_execution",
        "recovery_restore_execution",
        "token_link_rematerialisation",
        "ml_model_training",
    }
)

ON_DEMAND_OPERATIONS = frozenset(
    {
        "generate_payment_source_data",
    }
)

SCHEDULED_OPERATIONS = frozenset(
    {
        "platform_preflight",
        "raw_readiness",
        "lakehouse_transform",
        "data_quality",
        "ml_feature_generation",
        "ml_default_risk_scoring",
        "reconciliation",
        "acceptance",
    }
)
