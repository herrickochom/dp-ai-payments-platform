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
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=wendi.camt054/**/*.avro',
        hive_partitioning = true
    )

),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.xml.Document.BkToCstmrDbtCdtNtfctn.GrpHdr.MsgId') }}
            as message_id,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.GrpHdr.CreDtTm') }} as timestamp)
            as message_created_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Refs.EndToEndId') }}
            as end_to_end_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Id') }} as notification_id,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.CreDtTm') }} as timestamp)
            as notification_created_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Acct.Id.Othr.Id') }} as account_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Acct.Id.Othr.Issr') }} as account_issuer,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.Amt._text') }} as double)
            as entry_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.Amt._attributes.Ccy') }} as currency,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.CdtDbtInd') }} as credit_debit_indicator,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.Sts') }} as entry_status,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.BookgDt.Dt') }} as date)
            as booking_date,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Refs.TxId') }}
            as transaction_id,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Amt.Amt._text') }}
            as double) as transaction_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Amt.Amt._attributes.Ccy') }}
            as transaction_currency,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.Amt.CdtDbtInd') }}
            as transaction_credit_debit_indicator,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrDbtCdtNtfctn.Ntfctn.Ntry.NtryDtls.TxDtls.RmtInf.Ustrd') }}
            as remittance_information,

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
        'WENDI_CAMT054' as record_source

    from raw_data
    where _kafka_metadata.topic = 'wendi.camt054'
      and parsed_event_data is not null

)

select *
from parsed
