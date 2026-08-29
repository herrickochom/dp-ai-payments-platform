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
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=mobile.mtn.pacs002/**/*.avro',
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
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.OrgnlEndToEndId.EndToEndId') }}
            as end_to_end_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.OrgnlTxId.TxId') }}
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

        {% set technical_fields = [
            'correlationId', 'environment', 'flags', 'messageType', 'messageVersion',
            'parentSpanId', 'processingNode', 'processingPriority', 'requestId', 'retryCount',
            'sampled', 'spanId', 'tenantId', 'timeout', 'timestamp', 'traceId', 'version'
        ] %}
        {% for field in technical_fields %}
        {{ extract_json('parsed_event_data',
            '$.xml.Document.FIToFIPmtStsRpt.TxInfAndSts.x-' ~ field) }}
            as transaction_x_{{ field | lower }},
        {% endfor %}

        'MTN' as mobile_network,

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
        'MOBILE_MTN_PACS002' as record_source

    from raw_data
    where _kafka_metadata.topic = 'mobile.mtn.pacs002'
      and parsed_event_data is not null

)

select *
from parsed
