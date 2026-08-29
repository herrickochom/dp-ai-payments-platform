{{ config(materialized='view') }}

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

extracted as (

    select
        event_id,

        -- XML representation
        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.transaction_id'
        ) }} as xml_transaction_id,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.agent_id'
        ) }} as xml_agent_id,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.loan_id'
        ) }} as xml_loan_id,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.beneficiary_id'
        ) }} as xml_beneficiary_id,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.beneficiary_name'
        ) }} as xml_beneficiary_name,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.transaction_timestamp'
        ) }} as xml_transaction_timestamp,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.transaction_type'
        ) }} as xml_transaction_type,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.amount'
        ) }} as xml_amount,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.fee_amount'
        ) }} as xml_fee_amount,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.is_assisted_withdrawal'
        ) }} as xml_is_assisted_withdrawal,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.beneficiary_verified'
        ) }} as xml_beneficiary_verified,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.verification_method'
        ) }} as xml_verification_method,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.status'
        ) }} as xml_status,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.reference'
        ) }} as xml_reference,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.location.latitude'
        ) }} as xml_location_latitude,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.location.longitude'
        ) }} as xml_location_longitude,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.AgentTransaction.location.address'
        ) }} as xml_location_address,

        -- JSON representation
        {{ extract_json('parsed_event_data', '$.transaction_id') }}
            as json_transaction_id,

        {{ extract_json('parsed_event_data', '$.agent_id') }}
            as json_agent_id,

        {{ extract_json('parsed_event_data', '$.loan_id') }}
            as json_loan_id,

        {{ extract_json('parsed_event_data', '$.beneficiary_id') }}
            as json_beneficiary_id,

        {{ extract_json('parsed_event_data', '$.beneficiary_name') }}
            as json_beneficiary_name,

        {{ extract_json('parsed_event_data', '$.transaction_timestamp') }}
            as json_transaction_timestamp,

        {{ extract_json('parsed_event_data', '$.transaction_type') }}
            as json_transaction_type,

        {{ extract_json('parsed_event_data', '$.amount') }}
            as json_amount,

        {{ extract_json('parsed_event_data', '$.fee_amount') }}
            as json_fee_amount,

        {{ extract_json('parsed_event_data', '$.is_assisted_withdrawal') }}
            as json_is_assisted_withdrawal,

        {{ extract_json('parsed_event_data', '$.beneficiary_verified') }}
            as json_beneficiary_verified,

        {{ extract_json('parsed_event_data', '$.verification_method') }}
            as json_verification_method,

        {{ extract_json('parsed_event_data', '$.status') }}
            as json_status,

        {{ extract_json('parsed_event_data', '$.reference') }}
            as json_reference,

        {{ extract_json('parsed_event_data', '$.location.latitude') }}
            as json_location_latitude,

        {{ extract_json('parsed_event_data', '$.location.longitude') }}
            as json_location_longitude,

        {{ extract_json('parsed_event_data', '$.location.address') }}
            as json_location_address,

        _kafka_metadata.topic as kafka_topic,
        _kafka_metadata.partition as kafka_partition,
        _kafka_metadata.offset as kafka_offset,
        try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,

        _kafka_metadata.category as category,
        _kafka_metadata.source_system as source_system,
        _kafka_metadata.source_group as source_group,

        year,
        month,
        day,

        event_data,
        parsed_event_data

    from raw_data

    where _kafka_metadata.topic = 'agent.transactions'
      and parsed_event_data is not null

),

parsed as (

    select
        event_id,

        coalesce(xml_transaction_id, json_transaction_id)
            as transaction_id,

        coalesce(xml_agent_id, json_agent_id)
            as agent_id,

        coalesce(xml_loan_id, json_loan_id)
            as loan_id,

        coalesce(xml_beneficiary_id, json_beneficiary_id)
            as beneficiary_id,

        coalesce(xml_beneficiary_name, json_beneficiary_name)
            as beneficiary_name,

        try_cast(
            coalesce(
                xml_transaction_timestamp,
                json_transaction_timestamp
            ) as timestamp
        ) as transaction_timestamp,

        coalesce(xml_transaction_type, json_transaction_type)
            as transaction_type,

        try_cast(
            coalesce(xml_amount, json_amount) as double
        ) as amount,

        try_cast(
            coalesce(xml_fee_amount, json_fee_amount) as double
        ) as fee_amount,

        try_cast(
            coalesce(
                xml_is_assisted_withdrawal,
                json_is_assisted_withdrawal
            ) as boolean
        ) as is_assisted_withdrawal,

        try_cast(
            coalesce(
                xml_beneficiary_verified,
                json_beneficiary_verified
            ) as boolean
        ) as beneficiary_verified,

        coalesce(
            xml_verification_method,
            json_verification_method
        ) as verification_method,

        coalesce(xml_status, json_status)
            as status,

        coalesce(xml_reference, json_reference)
            as reference,

        try_cast(
            coalesce(
                xml_location_latitude,
                json_location_latitude
            ) as double
        ) as location_latitude,

        try_cast(
            coalesce(
                xml_location_longitude,
                json_location_longitude
            ) as double
        ) as location_longitude,

        coalesce(
            xml_location_address,
            json_location_address
        ) as location_address,

        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        category,
        source_system,
        source_group,

        year,
        month,
        day,

        event_data,
        parsed_event_data,

        current_timestamp as load_timestamp,
        'AGENT_TRANSACTIONS' as record_source

    from extracted

)

select *
from parsed
