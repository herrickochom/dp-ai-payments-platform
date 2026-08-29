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
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=agent.transactions/**/*.avro',
        hive_partitioning = true
    )

),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data',
            '$.xml.AgentTransaction.transaction_id') }} as transaction_id,

        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.agent_id') }}
            as agent_id,

        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.loan_id') }}
            as loan_id,

        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.beneficiary_id') }}
            as beneficiary_id,

        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.beneficiary_name') }}
            as beneficiary_name,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.AgentTransaction.transaction_timestamp') }} as timestamp)
            as transaction_timestamp,

        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.transaction_type') }}
            as transaction_type,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.AgentTransaction.amount') }} as double) as amount,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.AgentTransaction.fee_amount') }} as double) as fee_amount,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.AgentTransaction.is_assisted_withdrawal') }} as boolean)
            as is_assisted_withdrawal,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.AgentTransaction.beneficiary_verified') }} as boolean)
            as beneficiary_verified,

        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.verification_method') }}
            as verification_method,

        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.status') }} as status,

        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.reference') }}
            as reference,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.AgentTransaction.location.latitude') }} as double) as location_latitude,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.AgentTransaction.location.longitude') }} as double) as location_longitude,

        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.location.address') }}
            as location_address,

        {# JSON transaction fixtures use top-level fields rather than an XML wrapper. #}
        {% set json_transaction_fields = [
            'transaction_id', 'agent_id', 'loan_id', 'beneficiary_id', 'beneficiary_name',
            'transaction_timestamp', 'transaction_type', 'amount', 'fee_amount', 'net_amount',
            'is_assisted_withdrawal', 'beneficiary_verified', 'verification_method', 'status',
            'reference', 'created_at', 'updated_at'
        ] %}
        {% for field in json_transaction_fields %}
        {{ extract_json('parsed_event_data', '$.' ~ field) }} as source_json_{{ field }},
        {% endfor %}

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
        'AGENT_TRANSACTIONS' as record_source

    from raw_data
    where _kafka_metadata.topic = 'agent.transactions'
      and parsed_event_data is not null

)

select *
from parsed
