{{ config(materialized='iceberg_table', tags=['bronze', 'icmn', 'pmn', 'technical']) }}

select
    event_id, envelope_message_id, event_family, message_id, event_type,
    correlation_id, instruction_id, end_to_end_id, transaction_id, uetr,
    business_reference, x_trace, x_channel, x_beneficiary_sa, x_source_system,
    x_target_system, x_service, x_operation, x_component, x_node, x_host,
    component, technical_stage, technical_status,
    event_timestamp, processing_timestamp, latency_ms, error_code, error_category,
    retry_count, timeout_indicator, kafka_topic, kafka_partition, kafka_offset,
    kafka_timestamp, category, source_system, year, month, day, load_timestamp,
    record_source
from {{ ref('stg_pdm_icmn_pmn_pain001') }}
