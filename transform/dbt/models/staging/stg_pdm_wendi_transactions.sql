{{ config(materialized='iceberg_table') }}

with raw_data as (

    select
        event_id,
        message_id,
        event_data,
        parsed_event_data,
        payload,
        _kafka_metadata,
        year,
        month,
        day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=wendi.transactions/**/*.avro',
        hive_partitioning = true
    )

),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.event_id') }} as wallet_event_id,
        {{ extract_json('parsed_event_data', '$.loan_id') }} as loan_id,
        {{ extract_json('parsed_event_data', '$.beneficiary_id') }} as beneficiary_id,
        {{ extract_json('parsed_event_data', '$.beneficiary_name') }} as beneficiary_name,
        {{ extract_json('parsed_event_data', '$.sacco_id') }} as sacco_id,

        try_cast({{ extract_json('parsed_event_data', '$.event_timestamp') }} as timestamp)
            as event_timestamp,

        {{ extract_json('parsed_event_data', '$.event_type') }} as event_type,

        {{ extract_json('parsed_event_data', '$.source_account') }} as source_account,
        {{ extract_json('parsed_event_data', '$.source_account_type') }} as source_account_type,
        {{ extract_json('parsed_event_data', '$.destination_account') }} as destination_account,
        {{ extract_json('parsed_event_data', '$.destination_account_type') }} as destination_account_type,

        try_cast({{ extract_json('parsed_event_data', '$.amount') }} as double) as amount,
        {{ extract_json('parsed_event_data', '$.currency') }} as currency,
        {{ extract_json('parsed_event_data', '$.transaction_status') }} as transaction_status,
        {{ extract_json('parsed_event_data', '$.wendi_transaction_id') }} as wendi_transaction_id,
        {{ extract_json('parsed_event_data', '$.agent_id') }} as agent_id,

        {{ extract_json('parsed_event_data', '$.device_id') }} as device_id,
        {{ extract_json('parsed_event_data', '$.ip_address') }} as ip_address,
        {{ extract_json('parsed_event_data', '$.user_agent') }} as user_agent,

        {{ extract_json('parsed_event_data', '$.metadata.source_system') }} as metadata_source_system,
        {{ extract_json('parsed_event_data', '$.metadata.transaction_type') }} as metadata_transaction_type,

        try_cast({{ extract_json('parsed_event_data', '$.metadata.is_assisted') }} as boolean)
            as metadata_is_assisted,

        try_cast({{ extract_json('parsed_event_data', '$.metadata.processing_time_ms') }} as integer)
            as metadata_processing_time_ms,

        try_cast({{ extract_json('parsed_event_data', '$.created_at') }} as timestamp)
            as created_at,

        try_cast({{ extract_json('parsed_event_data', '$.updated_at') }} as timestamp)
            as updated_at,

        _kafka_metadata.topic as kafka_topic,
        _kafka_metadata.partition as kafka_partition,
        _kafka_metadata.offset as kafka_offset,
        try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,

        _kafka_metadata.category as category,
        upper(string_split(_kafka_metadata.topic, '.')[1]) as source_system,
        string_split(_kafka_metadata.topic, '.')[2] as source_group,

        year,
        month,
        day,

        event_data,
        parsed_event_data,

        current_timestamp as load_timestamp,
        'WENDI_TRANSACTIONS' as record_source

    from raw_data
    where _kafka_metadata.topic = 'wendi.transactions'
      and parsed_event_data is not null

)

select *
from parsed
