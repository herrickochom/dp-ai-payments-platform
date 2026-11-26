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
 where _kafka_topic = 'mobile.mtn.pacs008'
),

parsed as (

    select
        event_id,

        -- Raw envelope fields retained explicitly alongside ISO 20022 fields
        {{ extract_json('parsed_event_data', '$.agent_id') }} as raw_agent_id,
        safe_cast({{ extract_json('parsed_event_data', '$.creation_date') }} as timestamp)
            as raw_creation_date,
        {{ extract_json('parsed_event_data', '$.currency') }} as raw_currency,
        safe_cast({{ extract_json('parsed_event_data', '$.instructed_amount') }} as float64)
            as raw_instructed_amount,
        {{ extract_json('parsed_event_data', '$.loan_id') }} as raw_loan_id,
        {{ extract_json('parsed_event_data', '$.message_id') }} as raw_message_id,
        {{ extract_json('parsed_event_data', '$.payment_id') }} as raw_payment_id,
        {{ extract_json('parsed_event_data', '$.source_system') }} as raw_source_system,
        {{ extract_json('parsed_event_data', '$.transaction_id') }} as raw_transaction_id,

        -- Current Raw PACS.008 fields
        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr.SchmeNm') }}
            as creditor_account_scheme_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.Othr.SchmeNm') }}
            as debtor_account_scheme_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.IntrBkSttlmAmt._attributes.Ccy') }}
            as interbank_settlement_currency,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.IntrBkSttlmAmt._text') }} as float64)
            as interbank_settlement_amount,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.IntrBkSttlmDt') }} as date)
            as interbank_settlement_date,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.InstrId') }}
            as instruction_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.SttlmInf.SttlmMtd') }}
            as settlement_method,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.MsgId') }}
            as message_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.EndToEndId') }}
            as end_to_end_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.TxId') }}
            as transaction_id,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.CreDtTm') }} as timestamp)
            as creation_at,

        {{ extract_json('parsed_event_data', '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.NbOfTxs') }}
            as number_of_transactions,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.CtrlSum') }} as float64) as control_sum,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt._text') }} as float64)
            as instructed_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.InstdAmt._attributes.Ccy') }}
            as currency,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.Nm') }} as debtor_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.Nm') }} as creditor_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.BICFI') }}
            as debtor_agent_bic,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.Nm') }}
            as debtor_agent_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.BICFI') }}
            as creditor_agent_bic,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.Nm') }}
            as creditor_agent_name,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.Othr.Id') }}
            as debtor_account_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAcct.Id.Othr.Issr') }}
            as debtor_account_issuer,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr.Id') }}
            as creditor_account_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr.Issr') }}
            as creditor_account_issuer,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr.SchmeNm.Prtry') }}
            as creditor_account_scheme,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RmtInf.Ustrd') }}
            as remittance_information,


        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.InitgPty.Nm'
        ) }} as initiating_party_name,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.InitgPty.Id.OrgId.Othr.Id'
        ) }} as initiating_party_id,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.InitgPty.Id.OrgId.Othr.Issr'
        ) }} as initiating_party_id_issuer,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.ClrSysRef'
        ) }} as clearing_system_reference,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.UETR'
        ) }} as uetr,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Amt.EqvtAmt._text'
        ) }} as equivalent_amount,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Amt.EqvtAmt._attributes.Ccy'
        ) }} as equivalent_amount_currency,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Amt.CntrValAmt._text'
        ) }} as countervalue_amount,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Amt.CntrValAmt._attributes.Ccy'
        ) }} as countervalue_amount_currency,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Amt.ChrgAmt._text'
        ) }} as charge_amount,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Amt.ChrgAmt._attributes.Ccy'
        ) }} as charge_amount_currency,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.ChrgBr'
        ) }} as charge_bearer,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Purp.Cd'
        ) }} as purpose_code,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Purp.Prtry'
        ) }} as purpose_proprietary,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.UltmtDbtr.Nm'
        ) }} as ultimate_debtor_name,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.UltmtCdtr.Nm'
        ) }} as ultimate_creditor_name,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RltdPties.Cdtr.Nm'
        ) }} as related_creditor_name,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RmtInf.Strd.CdtrRefInf'
        ) }} as creditor_reference_information,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RmtInf.Strd.RfrdDocInf.Tp.Cd'
        ) }} as referred_document_type,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RmtInf.Strd.RfrdDocInf.Nb'
        ) }} as referred_document_number,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RmtInf.Strd.RfrdDocInf.Dt'
        ) }} as referred_document_date,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RgltryRptg.Inf.Tp.Cd'
        ) }} as regulatory_report_type,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RgltryRptg.Inf.Id'
        ) }} as regulatory_report_id,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RgltryRptg.Inf.Dt'
        ) }} as regulatory_report_date,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RgltryRptg.Inf.Amt._text'
        ) }} as regulatory_report_amount,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RgltryRptg.Inf.Amt._attributes.Ccy'
        ) }} as regulatory_report_currency,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.SplmtryData.Id'
        ) }} as supplementary_data_id,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.SplmtryData.Envlp.Any'
        ) }} as supplementary_data,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.PstlAdr.AdrLine'
        ) }} as debtor_address_lines,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.PstlAdr.TwnNm'
        ) }} as debtor_town_name,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.PstlAdr.CtrySubDvsn'
        ) }} as debtor_country_subdivision,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.PstlAdr.Ctry'
        ) }} as debtor_country,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Dbtr.PstlAdr.PstCd'
        ) }} as debtor_postal_code,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.PstlAdr.AdrLine'
        ) }} as creditor_address_lines,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.PstlAdr.TwnNm'
        ) }} as creditor_town_name,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.PstlAdr.CtrySubDvsn'
        ) }} as creditor_country_subdivision,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.PstlAdr.Ctry'
        ) }} as creditor_country,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Cdtr.PstlAdr.PstCd'
        ) }} as creditor_postal_code,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.PstlAdr.AdrLine'
        ) }} as debtor_agent_address_lines,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.PstlAdr.TwnNm'
        ) }} as debtor_agent_town_name,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.PstlAdr.CtrySubDvsn'
        ) }} as debtor_agent_country_subdivision,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.PstlAdr.Ctry'
        ) }} as debtor_agent_country,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.DbtrAgt.FinInstnId.PstlAdr.PstCd'
        ) }} as debtor_agent_postal_code,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.PstlAdr.AdrLine'
        ) }} as creditor_agent_address_lines,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.PstlAdr.TwnNm'
        ) }} as creditor_agent_town_name,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.PstlAdr.CtrySubDvsn'
        ) }} as creditor_agent_country_subdivision,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.PstlAdr.Ctry'
        ) }} as creditor_agent_country,

        {{ extract_json(
            'parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAgt.FinInstnId.PstlAdr.PstCd'
        ) }} as creditor_agent_postal_code,

        'MTN' as mobile_network,

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
        'MOBILE_MTN_PACS008' as record_source

    from raw_data
    where _kafka_metadata.topic = 'mobile.mtn.pacs008'
      and parsed_event_data is not null

)

select *
from parsed
