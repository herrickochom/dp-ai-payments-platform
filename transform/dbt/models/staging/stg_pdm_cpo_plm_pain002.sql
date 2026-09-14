{{ config(materialized='view') }}

with raw_data as (
    select event_id, message_id as envelope_message_id, event_family,
           source_system as envelope_source_system, parsed_event_data,
           _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=cpo.plm.pain002/**/*.avro',
        hive_partitioning = true
    )
    where _kafka_metadata.topic = 'cpo.plm.pain002'
      and parsed_event_data is not null
), parsed as (
    select
        event_id, envelope_message_id, event_family,
        {{ extract_json('parsed_event_data', '$.event_id') }} as message_id,
        {{ extract_json('parsed_event_data', '$.event_type') }} as event_type,
        {{ extract_json('parsed_event_data', '$.correlation_id') }} as correlation_id,
        {{ extract_json('parsed_event_data', '$.instruction_id') }} as instruction_id,
        {{ extract_json('parsed_event_data', '$.end_to_end_id') }} as end_to_end_id,
        {{ extract_json('parsed_event_data', '$.transaction_id') }} as transaction_id,
        {{ extract_json('parsed_event_data', '$.uetr') }} as uetr,
        {{ extract_json('parsed_event_data', '$.business_reference') }} as business_reference,
        {{ extract_json('parsed_event_data', '$.x_trace') }} as x_trace,
        {{ extract_json('parsed_event_data', '$.x_channel') }} as x_channel,
        {{ extract_json('parsed_event_data', '$.x_beneficiary_sa') }} as x_beneficiary_sa,
        {{ extract_json('parsed_event_data', '$.x_source_system') }} as x_source_system,
        {{ extract_json('parsed_event_data', '$.x_target_system') }} as x_target_system,
        {{ extract_json('parsed_event_data', '$.x_service') }} as x_service,
        {{ extract_json('parsed_event_data', '$.x_operation') }} as x_operation,
        {{ extract_json('parsed_event_data', '$.x_component') }} as x_component,
        {{ extract_json('parsed_event_data', '$.x_node') }} as x_node,
        {{ extract_json('parsed_event_data', '$.x_host') }} as x_host,
        {{ extract_json('parsed_event_data', '$.x_provider') }} as x_provider,
        {{ extract_json('parsed_event_data', '$.x_network') }} as x_network,
        {{ extract_json('parsed_event_data', '$.x_wallet_reference') }} as x_wallet_reference,
        {{ extract_json('parsed_event_data', '$.x_provider_transaction_id') }} as x_provider_transaction_id,
        {{ extract_json('parsed_event_data', '$.x_credit_status') }} as x_credit_status,
        {{ extract_json('parsed_event_data', '$.x_agent_reference') }} as x_agent_reference,
        {{ extract_json('parsed_event_data', '$.x_cashout_status') }} as x_cashout_status,
        {{ extract_json('parsed_event_data', '$.x_component') }} as component,
        {{ extract_json('parsed_event_data', '$.technical_stage') }} as technical_stage,
        {{ extract_json('parsed_event_data', '$.technical_status') }} as technical_status,
        try_cast({{ extract_json('parsed_event_data', '$.event_timestamp') }} as timestamp) as event_timestamp,
        try_cast({{ extract_json('parsed_event_data', '$.processing_timestamp') }} as timestamp) as processing_timestamp,
        try_cast({{ extract_json('parsed_event_data', '$.x_latency_ms') }} as bigint) as x_latency_ms,
        try_cast({{ extract_json('parsed_event_data', '$.x_latency_ms') }} as bigint) as latency_ms,
        {{ extract_json('parsed_event_data', '$.x_error_code') }} as x_error_code,
        {{ extract_json('parsed_event_data', '$.x_error_category') }} as x_error_category,
        try_cast({{ extract_json('parsed_event_data', '$.x_retry_count') }} as integer) as x_retry_count,
        try_cast({{ extract_json('parsed_event_data', '$.x_timeout_indicator') }} as boolean) as x_timeout_indicator,
        {{ extract_json('parsed_event_data', '$.x_error_code') }} as error_code,
        {{ extract_json('parsed_event_data', '$.x_error_category') }} as error_category,
        try_cast({{ extract_json('parsed_event_data', '$.x_retry_count') }} as integer) as retry_count,
        try_cast({{ extract_json('parsed_event_data', '$.x_timeout_indicator') }} as boolean) as timeout_indicator,
        _kafka_metadata.topic as kafka_topic,
        _kafka_metadata.partition as kafka_partition,
        _kafka_metadata.offset as kafka_offset,
        try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
        _kafka_metadata.category as category,
        envelope_source_system as source_system,
        year, month, day, current_timestamp as load_timestamp,
        'CPO_PLM_TECHNICAL_EVENT' as record_source
    from raw_data
)
select
    event_id, envelope_message_id, event_family, message_id, event_type,
    correlation_id, instruction_id, end_to_end_id, transaction_id, uetr,
    business_reference, x_trace, x_channel, x_beneficiary_sa,
    x_source_system, x_target_system, x_service, x_operation, x_component,
    x_node, x_host, x_provider, x_network, x_wallet_reference,
    x_provider_transaction_id, x_credit_status, x_agent_reference,
    x_cashout_status, component, technical_stage, technical_status,
    event_timestamp, processing_timestamp, x_latency_ms, x_error_code,
    x_error_category, x_retry_count, x_timeout_indicator,
    latency_ms, error_code, error_category, retry_count, timeout_indicator,
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, category,
    source_system, year, month, day, load_timestamp, record_source
from parsed
qualify row_number() over (
    partition by event_id order by kafka_timestamp desc nulls last, kafka_offset desc nulls last
) = 1
