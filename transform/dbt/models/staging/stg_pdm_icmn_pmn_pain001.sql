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
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=icmn.pmn.pain001/**/*.avro',
        hive_partitioning = true
    )

),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.MsgId') }}
            as message_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.EndToEndId') }}
            as end_to_end_id,

        try_cast({{ extract_json('parsed_event_data', '$.creation_date') }} as timestamp)
            as creation_at,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.CreDtTm') }} as timestamp)
            as xml_creation_at,

        try_cast({{ extract_json('parsed_event_data', '$.instructed_amount') }} as double)
            as instructed_amount,

        {{ extract_json('parsed_event_data', '$.currency') }} as currency,

        {{ extract_json('parsed_event_data', '$.debtor') }} as debtor_name,
        {{ extract_json('parsed_event_data', '$.creditor') }} as creditor_name,

        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.NbOfTxs') }}
            as number_of_transactions,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.CtrlSum') }} as double)
            as group_control_sum,

        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.InitgPty') }}
            as initiating_party,

        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.PmtInfId') }}
            as payment_information_id,

        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.PmtMtd') }}
            as payment_method,

        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.NbOfTxs') }}
            as payment_number_of_transactions,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CtrlSum') }} as double)
            as payment_control_sum,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.BtchBookg') }} as boolean)
            as batch_booking,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.ReqdExctnDt') }} as timestamp)
            as requested_execution_at,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.DbtrAcct.Id.Othr.Id') }}
            as debtor_account_id,

        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.DbtrAgt.FinInstnId') }}
            as debtor_agent_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAcct.Id.Othr.Id') }}
            as creditor_account_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAgt.FinInstnId') }}
            as creditor_agent_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.InstrId') }}
            as instruction_id,

        try_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Amt.InstdAmt._text') }}
            as double) as xml_instructed_amount,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Amt.InstdAmt._attributes.Ccy') }}
            as xml_currency,

        {# Technical fields occur independently at all three message levels. #}
        {% set technical_fields = [
            'backoffDelay', 'bulkhead', 'circuitBreaker', 'correlationId', 'deadline',
            'environment', 'errorRate', 'fallbackEnabled', 'flags', 'healthStatus',
            'latencyP95', 'maxRetries', 'messageType', 'messageVersion', 'parentSpanId',
            'processingNode', 'processingPriority', 'processingTime', 'queueDepth',
            'rateLimiter', 'requestId', 'retryCount', 'retryPolicy', 'sampled', 'spanId',
            'successRate', 'tenantId', 'throughput', 'timeout', 'timeoutThreshold',
            'timestamp', 'traceId', 'version'
        ] %}
        {% set technical_levels = [
            ('group', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr'),
            ('payment', '$.xml.Document.CstmrCdtTrfInitn.PmtInf'),
            ('transaction', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf')
        ] %}
        {% for level_name, level_path in technical_levels %}
            {% for field in technical_fields %}
        {{ extract_json('parsed_event_data', level_path ~ '.x-' ~ field) }}
            as {{ level_name }}_x_{{ field | lower }},
            {% endfor %}
        {% endfor %}

        -- technical lifecycle x-* attributes
        coalesce(
            {{ extract_json('parsed_event_data', '$.x_attributes.x-messageType') }},
            _kafka_metadata.topic
        ) as x_message_type,

        try_cast({{ extract_json('parsed_event_data', '$.x_attributes.x-processingPriority') }} as integer)
            as processing_priority,

        try_cast({{ extract_json('parsed_event_data', '$.x_attributes.x-retryCount') }} as integer)
            as retry_count,

        {{ extract_json('parsed_event_data', '$.x_attributes.x-timeout') }} as timeout,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-correlationId') }} as correlation_id,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-traceId') }} as trace_id,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-spanId') }} as span_id,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-parentSpanId') }} as parent_span_id,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-tenantId') }} as tenant_id,
        {{ extract_json('parsed_event_data', '$.x_attributes.x-environment') }} as environment,

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
        'ICMN_PMN_PAIN001' as record_source

    from raw_data
    where _kafka_metadata.topic = 'icmn.pmn.pain001'
      and parsed_event_data is not null

)

select *
from parsed
