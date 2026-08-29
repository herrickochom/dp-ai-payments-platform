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
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=mobile.airtel.pacs008/**/*.avro',
        hive_partitioning = true
    )

),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.MsgId') }}
            as message_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.EndToEndId') }}
            as end_to_end_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.PmtId.TxId') }}
            as transaction_id,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.CreDtTm') }} as timestamp)
            as creation_at,

        {{ extract_json('parsed_event_data', '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.NbOfTxs') }}
            as number_of_transactions,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.GrpHdr.CtrlSum') }} as double) as control_sum,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Amt.InstdAmt._text') }} as double)
            as instructed_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.Amt.InstdAmt._attributes.Ccy') }}
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
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.CdtrAcct.Id.Othr.SchmeNm') }}
            as creditor_account_scheme,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.RmtInf.Ustrd') }}
            as remittance_information,

        {# Complete source projection for the rich PACS.008 fixture variant. #}
        {% set pacs008_fields = [
            ('GrpHdr.InitgPty.Nm', 'initiating_party_name'),
            ('GrpHdr.InitgPty.Id.OrgId.Othr.Id', 'initiating_party_id'),
            ('GrpHdr.InitgPty.Id.OrgId.Othr.Issr', 'initiating_party_id_issuer'),
            ('CdtTrfTxInf.PmtId.ClrSysRef', 'clearing_system_reference'),
            ('CdtTrfTxInf.PmtId.UETR', 'uetr'),
            ('CdtTrfTxInf.Amt.EqvtAmt._text', 'equivalent_amount'),
            ('CdtTrfTxInf.Amt.EqvtAmt._attributes.Ccy', 'equivalent_amount_currency'),
            ('CdtTrfTxInf.Amt.CntrValAmt._text', 'countervalue_amount'),
            ('CdtTrfTxInf.Amt.CntrValAmt._attributes.Ccy', 'countervalue_amount_currency'),
            ('CdtTrfTxInf.Amt.ChrgAmt._text', 'charge_amount'),
            ('CdtTrfTxInf.Amt.ChrgAmt._attributes.Ccy', 'charge_amount_currency'),
            ('CdtTrfTxInf.ChrgBr', 'charge_bearer'),
            ('CdtTrfTxInf.Purp.Cd', 'purpose_code'),
            ('CdtTrfTxInf.Purp.Prtry', 'purpose_proprietary'),
            ('CdtTrfTxInf.UltmtDbtr.Nm', 'ultimate_debtor_name'),
            ('CdtTrfTxInf.UltmtCdtr.Nm', 'ultimate_creditor_name'),
            ('CdtTrfTxInf.RltdPties.Cdtr.Nm', 'related_creditor_name'),
            ('CdtTrfTxInf.RmtInf.Strd.CdtrRefInf', 'creditor_reference_information'),
            ('CdtTrfTxInf.RmtInf.Strd.RfrdDocInf.Tp.Cd', 'referred_document_type'),
            ('CdtTrfTxInf.RmtInf.Strd.RfrdDocInf.Nb', 'referred_document_number'),
            ('CdtTrfTxInf.RmtInf.Strd.RfrdDocInf.Dt', 'referred_document_date'),
            ('CdtTrfTxInf.RgltryRptg.Inf.Tp.Cd', 'regulatory_report_type'),
            ('CdtTrfTxInf.RgltryRptg.Inf.Id', 'regulatory_report_id'),
            ('CdtTrfTxInf.RgltryRptg.Inf.Dt', 'regulatory_report_date'),
            ('CdtTrfTxInf.RgltryRptg.Inf.Amt._text', 'regulatory_report_amount'),
            ('CdtTrfTxInf.RgltryRptg.Inf.Amt._attributes.Ccy', 'regulatory_report_currency'),
            ('CdtTrfTxInf.SplmtryData.Id', 'supplementary_data_id'),
            ('CdtTrfTxInf.SplmtryData.Envlp.Any', 'supplementary_data')
        ] %}
        {% for field_path, field_alias in pacs008_fields %}
        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.' ~ field_path) }} as {{ field_alias }},
        {% endfor %}

        {% set address_roles = [
            ('CdtTrfTxInf.Dbtr.PstlAdr', 'debtor'),
            ('CdtTrfTxInf.Cdtr.PstlAdr', 'creditor'),
            ('CdtTrfTxInf.DbtrAgt.FinInstnId.PstlAdr', 'debtor_agent'),
            ('CdtTrfTxInf.CdtrAgt.FinInstnId.PstlAdr', 'creditor_agent')
        ] %}
        {% set address_fields = [
            ('AdrLine', 'address_lines'), ('TwnNm', 'town_name'), ('CtrySubDvsn', 'country_subdivision'),
            ('Ctry', 'country'), ('PstCd', 'postal_code')
        ] %}
        {% for role_path, role_alias in address_roles %}
            {% for field_path, field_alias in address_fields %}
        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.' ~ role_path ~ '.' ~ field_path) }}
            as {{ role_alias }}_{{ field_alias }},
            {% endfor %}
        {% endfor %}

        {% set technical_fields = [
            'correlationId', 'environment', 'flags', 'messageType', 'messageVersion',
            'parentSpanId', 'processingNode', 'processingPriority', 'requestId', 'retryCount',
            'sampled', 'spanId', 'tenantId', 'timeout', 'timestamp', 'traceId', 'version'
        ] %}
        {% for field in technical_fields %}
        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFICstmrCdtTrf.CdtTrfTxInf.x-' ~ field) }}
            as transaction_x_{{ field | lower }},
        {% endfor %}

        'AIRTEL' as mobile_network,

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
        'MOBILE_AIRTEL_PACS008' as record_source

    from raw_data
    where _kafka_metadata.topic = 'mobile.airtel.pacs008'
      and parsed_event_data is not null

)

select *
from parsed
