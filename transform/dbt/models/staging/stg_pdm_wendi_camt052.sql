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
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=wendi.camt052/**/*.avro',
        hive_partitioning = true
    )

),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.xml.Document.BkToCstmrAcctRpt.GrpHdr.MsgId') }}
            as message_id,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.GrpHdr.CreDtTm') }} as timestamp)
            as report_created_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Acct.Id.Othr.Id') }} as account_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Acct.Id.Othr.Issr') }} as account_issuer,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.Tp.CdOrPrtry') }} as balance_type,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.Amt._text') }} as double) as balance_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.Amt._attributes.Ccy') }} as currency,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.CdtDbtInd') }} as credit_debit_indicator,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrAcctRpt.Bal.Dt') }} as date) as balance_date,

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
        'WENDI_CAMT052' as record_source

    from raw_data
    where _kafka_metadata.topic = 'wendi.camt052'
      and parsed_event_data is not null

)

select *
from parsed
