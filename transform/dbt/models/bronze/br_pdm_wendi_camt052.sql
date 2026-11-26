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
 where _kafka_topic = 'wendi.camt052'
),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.xml.Document.BkToCstmrAcctRpt.GrpHdr.MsgId') }}
            as message_id,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.GrpHdr.CreDtTm') }} as timestamp)
            as report_created_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Acct.Id.Othr.Id') }} as account_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Acct.Id.Othr.Issr') }} as account_issuer,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.Tp.CdOrPrtry') }} as balance_type,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.Amt._text') }} as float64) as balance_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.Amt._attributes.Ccy') }} as currency,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.CdtDbtInd') }} as credit_debit_indicator,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.Dt') }} as date) as balance_date,


        -- Raw fields discovered by exhaustive Raw -> Bronze parity audit
        {{ extract_json('parsed_event_data', '$.agent_id') }} as raw_agent_id,
        {{ extract_json('parsed_event_data', '$.creation_date') }} as raw_creation_date,
        {{ extract_json('parsed_event_data', '$.currency') }} as raw_currency,
        {{ extract_json('parsed_event_data', '$.instructed_amount') }} as raw_instructed_amount,
        {{ extract_json('parsed_event_data', '$.loan_id') }} as raw_loan_id,
        {{ extract_json('parsed_event_data', '$.message_id') }} as raw_message_id,
        {{ extract_json('parsed_event_data', '$.payment_id') }} as raw_payment_id,
        {{ extract_json('parsed_event_data', '$.source_system') }} as raw_source_system,
        {{ extract_json('parsed_event_data', '$.transaction_id') }} as raw_transaction_id,
        {{ extract_json('parsed_event_data', '$.xml.Document.BkToCstmrAcctRpt.Acct.Id.Othr.SchmeNm') }} as raw_xml_schme_nm,

        _kafka_metadata.topic as kafka_topic,
        _kafka_metadata.`partition` as kafka_partition,
        _kafka_metadata.offset as kafka_offset,
        safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,

        _kafka_metadata.category as category,
        upper(SPLIT(_kafka_metadata.topic, '.')[SAFE_OFFSET(0)]) as source_system,
        SPLIT(_kafka_metadata.topic, '.')[SAFE_OFFSET(1)] as source_group,

        year,
        month,
        day,

        event_data,
        parsed_event_data,

        current_timestamp() as load_timestamp,
        'WENDI_CAMT052' as record_source

    from raw_data
    where _kafka_metadata.topic = 'wendi.camt052'
      and parsed_event_data is not null

)

select *
from parsed
