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
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=wendi.camt053/**/*.avro',
        hive_partitioning = true
    )

),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.xml.Document.BkToCstmrStmt.GrpHdr.MsgId') }}
            as message_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Refs.EndToEndId') }}
            as end_to_end_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Refs.TxId') }}
            as transaction_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Id') }} as statement_id,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.GrpHdr.CreDtTm') }} as timestamp)
            as message_created_at,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.CreDtTm') }} as timestamp)
            as statement_created_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.ElctrncSeqNb') }} as electronic_sequence_number,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.FrToDt.FrDtTm') }} as timestamp)
            as period_from_at,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.FrToDt.ToDtTm') }} as timestamp)
            as period_to_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Acct.Id.Othr.Id') }} as account_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Acct.Id.Othr.Issr') }} as account_issuer,

        coalesce(
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[1].Tp.CdOrPrtry') }},
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[0].Tp.CdOrPrtry') }}
        ) as balance_type,

        try_cast(coalesce(
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[1].Amt._text') }},
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[0].Amt._text') }}
        ) as double) as balance_amount,

        coalesce(
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[1].Amt._attributes.Ccy') }},
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[0].Amt._attributes.Ccy') }}
        ) as currency,

        coalesce(
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[1].CdtDbtInd') }},
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[0].CdtDbtInd') }}
        ) as credit_debit_indicator,

        try_cast(coalesce(
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[1].Dt') }},
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[0].Dt') }}
        ) as date) as balance_date,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.Amt._text') }} as double) as entry_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.Amt._attributes.Ccy') }}
            as entry_currency,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.CdtDbtInd') }} as entry_credit_debit_indicator,

        {{ extract_json('parsed_event_data', '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.Sts') }}
            as entry_status,

        {{ extract_json('parsed_event_data', '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryRef') }}
            as entry_reference,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.AcctSvcrRef') }}
            as account_servicer_reference,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.BookgDt.Dt') }} as date)
            as booking_date,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.ValDt.Dt') }} as date)
            as value_date,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.BkTxCd') }} as bank_transaction_code,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Refs.InstrId') }}
            as instruction_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Refs.UETR') }}
            as uetr,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Amt.Amt._text') }}
            as double) as transaction_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Amt.Amt._attributes.Ccy') }}
            as transaction_currency,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Amt.CdtDbtInd') }}
            as transaction_credit_debit_indicator,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Dbtr.Nm') }}
            as debtor_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Cdtr.Nm') }}
            as creditor_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.DbtrAgt.FinInstnId.BICFI') }}
            as debtor_agent_bic,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.DbtrAgt.FinInstnId.Nm') }}
            as debtor_agent_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.CdtrAgt.FinInstnId.BICFI') }}
            as creditor_agent_bic,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.CdtrAgt.FinInstnId.Nm') }}
            as creditor_agent_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.RmtInf.Ustrd') }}
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
        'WENDI_CAMT053' as record_source

    from raw_data
    where _kafka_metadata.topic = 'wendi.camt053'
      and parsed_event_data is not null

)

select *
from parsed
