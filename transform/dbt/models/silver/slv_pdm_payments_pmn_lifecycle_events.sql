{{ config(materialized='iceberg_table') }}

select
    event_id, message_id, event_family, event_type, correlation_id, instruction_id,
    end_to_end_id, transaction_id, uetr, business_reference, x_trace, x_channel,
    x_source_system, x_target_system, x_service, x_operation,
    x_component, x_node, x_host, x_payment_route, x_originating_institution,
    x_intermediary_institution, x_route_decision, x_validation_status,
    x_submission_status,
    technical_stage, technical_status, event_timestamp, processing_timestamp,
    x_latency_ms, x_error_code, x_error_category, x_retry_count, x_timeout_indicator,
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp
from {{ ref('br_pdm_payments_pmn_lifecycle_events') }}
