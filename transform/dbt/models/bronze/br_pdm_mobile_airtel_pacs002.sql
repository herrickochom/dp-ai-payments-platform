{{ config(enabled=false) }}

{#
  Raw -> Bronze transformation definition.

  Execution ownership:
      Raw Avro -> DuckDB -> bulk Parquet
      -> transform-Trino -> Iceberg/Nessie Bronze

  This model is intentionally disabled in dbt because read_avro() and
  extract_json() are executed by DuckDB, not Trino.
#}


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
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=mobile.airtel.pacs002/**/*.avro',
        hive_partitioning = true
    )

),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.xml.Document.FIToFIPmtStsRpt.GrpHdr.MsgId') }}
            as message_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.OrgnlEndToEndId') }}
            as end_to_end_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.OrgnlTxId') }}
            as original_transaction_id,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.GrpHdr.CreDtTm') }} as timestamp)
            as creation_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.TxSts') }} as transaction_status,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.StsReqId') }} as status_request_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.StsRsnInf.Rsn.Cd') }} as status_reason_code,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.StsRsnInf.AddtlInf') }} as status_additional_info,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.SttlmInf.SttlmMtd.Cd') }} as settlement_method,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.SttlmInf.ClrSys') }} as clearing_system,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.AccptncDtTm') }} as timestamp)
            as acceptance_datetime,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.AcctSvcrRef') }} as account_servicer_reference,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.ClrSysRef') }} as clearing_system_reference,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.InstgAgt.FinInstnId.BICFI') }}
            as instructing_agent_bic,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.InstgAgt.FinInstnId.Nm') }}
            as instructing_agent_name,

        'AIRTEL' as mobile_network,


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
        {{ extract_json('parsed_event_data', '$.xml.Document.FIToFIPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgId') }} as raw_xml_orgnl_msg_id,
        {{ extract_json('parsed_event_data', '$.xml.Document.FIToFIPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgNmId') }} as raw_xml_orgnl_msg_nm_id,
        {{ extract_json('parsed_event_data', '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.OrgnlInstrId') }} as raw_xml_orgnl_instr_id,

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
        'MOBILE_AIRTEL_PACS002' as record_source

    from raw_data
    where _kafka_metadata.topic = 'mobile.airtel.pacs002'
      and parsed_event_data is not null

)

select *
from parsed
