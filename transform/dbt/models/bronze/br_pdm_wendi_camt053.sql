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
 where _kafka_topic = 'wendi.camt053'
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

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.GrpHdr.CreDtTm') }} as timestamp)
            as message_created_at,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.CreDtTm') }} as timestamp)
            as statement_created_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.ElctrncSeqNb') }} as electronic_sequence_number,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.FrToDt.FrDtTm') }} as timestamp)
            as period_from_at,

        safe_cast({{ extract_json('parsed_event_data',
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

        safe_cast(coalesce(
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[1].Amt._text') }},
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[0].Amt._text') }}
        ) as float64) as balance_amount,

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

        safe_cast(coalesce(
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[1].Dt') }},
            {{ extract_json('parsed_event_data',
                '$.xml.Document.BkToCstmrStmt.Stmt.Bal[0].Dt') }}
        ) as date) as balance_date,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.Amt._text') }} as float64) as entry_amount,

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

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.BookgDt.Dt') }} as date)
            as booking_date,

        safe_cast({{ extract_json('parsed_event_data',
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

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.BkToCstmrStmt.Stmt.Ntry.NtryDtls.TxDtls.Amt.Amt._text') }}
            as float64) as transaction_amount,

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
        'WENDI_CAMT053' as record_source

    from raw_data
    where _kafka_metadata.topic = 'wendi.camt053'
      and parsed_event_data is not null

)

select *
from parsed
