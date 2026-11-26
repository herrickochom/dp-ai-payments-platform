{{ config(materialized='pdm_iceberg_guarded') }}

with raw_data as (
 select *, struct(_kafka_topic as topic, _kafka_partition as `partition`,
 _kafka_offset as offset, _kafka_timestamp as timestamp,
 _kafka_category as category, _kafka_source_group as source_group,
 _kafka_source_system as source_system) as _kafka_metadata,
 extract(year from _kafka_timestamp) as year,
 extract(month from _kafka_timestamp) as month,
 extract(day from _kafka_timestamp) as day
 from {{ source('pdm_bronze_input','payment_events') }}
 where _kafka_topic = 'agent.transactions'
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


        -- Raw fields discovered by exhaustive Raw -> Bronze parity audit
        {{ extract_json('parsed_event_data', '$.creation_date') }} as raw_creation_date,
        {{ extract_json('parsed_event_data', '$.currency') }} as raw_currency,
        safe_cast({{ extract_json('parsed_event_data', '$.instructed_amount') }} as float64) as raw_instructed_amount,
        {{ extract_json('parsed_event_data', '$.message_id') }} as raw_message_id,
        {{ extract_json('parsed_event_data', '$.payment_id') }} as raw_payment_id,
        {{ extract_json('parsed_event_data', '$.source_system') }} as raw_source_system,
        {{ extract_json('parsed_event_data', '$.xml.AgentTransaction.sacco_id') }} as raw_xml_sacco_id,

        _kafka_metadata.topic as kafka_topic,
        _kafka_metadata.`partition` as kafka_partition,
        _kafka_metadata.offset as kafka_offset,
        safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,

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

        safe_cast(
            coalesce(
                xml_transaction_timestamp,
                json_transaction_timestamp
            ) as timestamp
        ) as transaction_timestamp,

        coalesce(xml_transaction_type, json_transaction_type)
            as transaction_type,

        safe_cast(
            coalesce(xml_amount, json_amount) as float64
        ) as amount,

        safe_cast(
            coalesce(xml_fee_amount, json_fee_amount) as float64
        ) as fee_amount,

        safe_cast(
            coalesce(
                xml_is_assisted_withdrawal,
                json_is_assisted_withdrawal
            ) as bool
        ) as is_assisted_withdrawal,

        safe_cast(
            coalesce(
                xml_beneficiary_verified,
                json_beneficiary_verified
            ) as bool
        ) as beneficiary_verified,

        coalesce(
            xml_verification_method,
            json_verification_method
        ) as verification_method,

        coalesce(xml_status, json_status)
            as status,

        coalesce(xml_reference, json_reference)
            as reference,

        safe_cast(
            coalesce(
                xml_location_latitude,
                json_location_latitude
            ) as float64
        ) as location_latitude,

        safe_cast(
            coalesce(
                xml_location_longitude,
                json_location_longitude
            ) as float64
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

        current_timestamp() as load_timestamp,
        'AGENT_TRANSACTIONS' as record_source

    from extracted

)

select *
from parsed
