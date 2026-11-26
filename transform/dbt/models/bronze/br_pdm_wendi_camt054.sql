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
 where _kafka_topic = 'wendi.camt054'
),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.xml.Document.BkToCstmrDbtCdtNtfctn.GrpHdr.MsgId') }}
            as message_id,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.GrpHdr.CreDtTm') }} as timestamp)
            as message_created_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Refs.EndToEndId') }}
            as end_to_end_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Id') }} as notification_id,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.CreDtTm') }} as timestamp)
            as notification_created_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Acct.Id.Othr.Id') }} as account_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Acct.Id.Othr.Issr') }} as account_issuer,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.Amt._text') }} as float64)
            as entry_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.Amt._attributes.Ccy') }} as currency,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.CdtDbtInd') }} as credit_debit_indicator,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.Sts') }} as entry_status,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.BookgDt.Dt') }} as date)
            as booking_date,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Refs.TxId') }}
            as transaction_id,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Amt.Amt._text') }}
            as float64) as transaction_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Amt.Amt._attributes.Ccy') }}
            as transaction_currency,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Amt.CdtDbtInd') }}
            as transaction_credit_debit_indicator,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.RmtInf.Ustrd') }}
            as remittance_information,


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
        'WENDI_CAMT054' as record_source

    from raw_data
    where _kafka_metadata.topic = 'wendi.camt054'
      and parsed_event_data is not null

)

select *
from parsed
