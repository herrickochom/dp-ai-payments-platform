{{ config(materialized='view') }}

with raw_data as (

    select
        event_id,
        message_id as envelope_message_id,
        event_data,
        parsed_event_data,
        _kafka_metadata,
        year,
        month,
        day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=cpo.plm.pain002/**/*.avro',
        hive_partitioning = true
    )
    where parsed_event_data is not null
      and _kafka_metadata.topic = 'cpo.plm.pain002'

),

parsed as (

    select
        event_id,
        envelope_message_id,

        {{ extract_json('parsed_event_data', '$.header.message_id') }} as message_id,
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.GrpHdr.MsgId') }} as xml_message_id,
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgId') }} as original_message_id,
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlEndToEndId') }} as end_to_end_id,

        try_cast({{ extract_json('parsed_event_data', '$.header.creation_date') }} as timestamp) as creation_at,
        try_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.GrpHdr.CreDtTm') }} as timestamp) as xml_creation_at,

        {{ extract_json('parsed_event_data', '$.header.initiating_party') }} as initiating_party,
        {{ extract_json('parsed_event_data', '$.header.original_message_id') }} as original_message_id_flat,
        {{ extract_json('parsed_event_data', '$.header.original_message_type') }} as original_message_type,
        {{ extract_json('parsed_event_data', '$.header.group_status') }} as group_status,
        {{ extract_json('parsed_event_data', '$.payload.transaction_status') }} as transaction_status,

        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.GrpSts') }} as xml_group_status,
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.TxSts') }} as xml_transaction_status,
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgNmId') }} as original_message_name_id,
        try_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlNbOfTxs') }} as integer) as original_number_of_transactions,
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlPmtInfId') }} as original_payment_information_id,
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlInstrId') }} as original_instruction_id,
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlTxId') }} as original_transaction_id,

        coalesce(
            nullif({{ extract_json('parsed_event_data', '$.payload.reason_code') }}, ''),
            {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.StsRsnInf.Rsn.Cd') }}
        ) as reason_code,

        {{ extract_json('parsed_event_data', '$.payload.additional_info') }} as additional_info,

        {{ extract_json('parsed_event_data', '$.x_attributes.x-processingStatus') }} as processing_status,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-errorCode') }} as error_code,
        try_cast({{ extract_json('parsed_event_data', '$.x_attributes.x-retryAttempt') }} as integer) as retry_attempt,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-systemLatency') }} as system_latency,
        try_cast(
            regexp_extract(
                {{ extract_json('parsed_event_data', '$.x_attributes.x-systemLatency') }},
                '([0-9]+)',
                1
            ) as integer
        ) as system_latency_ms,
        try_cast({{ extract_json('parsed_event_data', '$.x_attributes.x-priority') }} as integer) as priority,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-correlationId') }} as correlation_id,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-traceId') }} as trace_id,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-spanId') }} as span_id,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-parentSpanId') }} as parent_span_id,
        try_cast({{ extract_json('parsed_event_data', '$.x_attributes.x-sampled') }} as integer) as sampled,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-flags') }} as trace_flags,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-tenantId') }} as tenant_id,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-environment') }} as environment,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-version') }} as source_version,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-messageType') }} as source_message_type,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-messageVersion') }} as source_message_version,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-processingNode') }} as processing_node,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-requestId') }} as request_id,
        try_cast({{ extract_json('parsed_event_data', '$.x_attributes.x-timestamp') }} as timestamp) as source_timestamp,

        _kafka_metadata.topic as kafka_topic,
        try_cast(_kafka_metadata.partition as integer) as kafka_partition,
        try_cast(_kafka_metadata.offset as bigint) as kafka_offset,
        try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
        _kafka_metadata.category as category,

        upper(string_split(_kafka_metadata.topic, '.')[1]) as source_system,
        string_split(_kafka_metadata.topic, '.')[2] as source_group,

        try_cast(year as integer) as year,
        try_cast(month as integer) as month,
        try_cast(day as integer) as day,

        event_data,
        parsed_event_data,

        current_timestamp as load_timestamp,
        'CPO_PLM_PAIN002' as record_source

    from raw_data

),

deduplicated as (

    select * exclude (_event_rank)
    from (
        select
            parsed.*,
            row_number() over (
                partition by event_id
                order by
                    kafka_timestamp desc nulls last,
                    kafka_offset desc nulls last,
                    load_timestamp desc
            ) as _event_rank
        from parsed
    )
    where _event_rank = 1

)

select *
from deduplicated
