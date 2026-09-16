{{ config(materialized='iceberg_table') }}

with technical_events as (
    select
        event_id, message_id, event_family, event_type,
        correlation_id, instruction_id, end_to_end_id, transaction_id, uetr,
        business_reference, x_trace, x_channel, x_source_system, x_target_system,
        x_service, x_operation, x_component, x_node, x_host,
        x_payment_route, x_originating_institution, x_intermediary_institution,
        x_route_decision, x_validation_status, x_submission_status,
        cast(null as varchar) as x_provider, cast(null as varchar) as x_network,
        cast(null as varchar) as x_wallet_reference,
        cast(null as varchar) as x_provider_transaction_id, cast(null as varchar) as x_credit_status,
        cast(null as varchar) as x_agent_reference, cast(null as varchar) as x_cashout_status,
        technical_stage, technical_status, event_timestamp, processing_timestamp,
        x_latency_ms, x_retry_count, x_timeout_indicator, x_error_code, x_error_category,
        kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
        'ICMN_PMN' as technical_source, 'PMN' as technical_event_type
    from {{ ref('slv_pdm_payments_pmn_lifecycle_events') }}

    union all

    select
        event_id, message_id, event_family, event_type,
        correlation_id, instruction_id, end_to_end_id, transaction_id, uetr,
        business_reference, x_trace, x_channel, x_source_system, x_target_system,
        x_service, x_operation, x_component, x_node, x_host,
        cast(null as varchar) as x_payment_route,
        cast(null as varchar) as x_originating_institution,
        cast(null as varchar) as x_intermediary_institution,
        cast(null as varchar) as x_route_decision,
        cast(null as varchar) as x_validation_status,
        cast(null as varchar) as x_submission_status,
        x_provider, x_network, x_wallet_reference,
        x_provider_transaction_id, x_credit_status, x_agent_reference, x_cashout_status,
        technical_stage, technical_status, event_timestamp, processing_timestamp,
        x_latency_ms, x_retry_count, x_timeout_indicator, x_error_code, x_error_category,
        kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
        'CPO_PLM' as technical_source, 'PLM' as technical_event_type
    from {{ ref('slv_pdm_payments_plm_lifecycle_events') }}
)
select
    event_id, message_id, event_family, event_type,
    correlation_id, instruction_id, end_to_end_id, transaction_id, uetr,
    business_reference, x_trace, x_channel, x_source_system, x_target_system,
    x_service, x_operation, x_component, x_node, x_host,
    x_payment_route, x_originating_institution, x_intermediary_institution,
    x_route_decision, x_validation_status, x_submission_status,
    x_provider, x_network, x_wallet_reference,
    x_provider_transaction_id, x_credit_status, x_agent_reference, x_cashout_status,
    technical_stage, technical_status, event_timestamp, processing_timestamp,
    x_latency_ms, x_retry_count, x_timeout_indicator, x_error_code, x_error_category,
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
    technical_source, technical_event_type
from technical_events
qualify row_number() over (
    partition by technical_source, event_id
    order by kafka_timestamp desc nulls last, kafka_offset desc nulls last
) = 1
