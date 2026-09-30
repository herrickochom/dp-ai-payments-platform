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
    select event_id, message_id as envelope_message_id, event_family,
           source_system as envelope_source_system, parsed_event_data,
           _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=icmn.pmn.pain001/**/*.avro',
        hive_partitioning = true
    )
    where _kafka_metadata.topic = 'icmn.pmn.pain001'
      and parsed_event_data is not null
), parsed as (
    select
        event_id, envelope_message_id, event_family,
        {{ extract_json('parsed_event_data', '$.message_id') }} as message_id,
        {{ extract_json('parsed_event_data', '$.event_type') }} as event_type,
        {{ extract_json('parsed_event_data', '$.correlation_id') }} as correlation_id,
        {{ extract_json('parsed_event_data', '$.instruction_id') }} as instruction_id,
        {{ extract_json('parsed_event_data', '$.end_to_end_id') }} as end_to_end_id,
        {{ extract_json('parsed_event_data', '$.transaction_id') }} as transaction_id,
        {{ extract_json('parsed_event_data', '$.uetr') }} as uetr,
        {{ extract_json('parsed_event_data', '$.business_reference') }} as business_reference,
        {{ extract_json('parsed_event_data', '$.x_trace') }} as x_trace,
        {{ extract_json('parsed_event_data', '$.x_channel') }} as x_channel,
        {{ extract_json('parsed_event_data', '$.x_source_system') }} as x_source_system,
        {{ extract_json('parsed_event_data', '$.x_target_system') }} as x_target_system,
        {{ extract_json('parsed_event_data', '$.x_service') }} as x_service,
        {{ extract_json('parsed_event_data', '$.x_operation') }} as x_operation,
        {{ extract_json('parsed_event_data', '$.x_component') }} as x_component,
        {{ extract_json('parsed_event_data', '$.x_node') }} as x_node,
        {{ extract_json('parsed_event_data', '$.x_host') }} as x_host,
        {{ extract_json('parsed_event_data', '$.x_payment_route') }} as x_payment_route,
        {{ extract_json('parsed_event_data', '$.x_originating_institution') }} as x_originating_institution,
        {{ extract_json('parsed_event_data', '$.x_intermediary_institution') }} as x_intermediary_institution,
        {{ extract_json('parsed_event_data', '$.x_route_decision') }} as x_route_decision,
        {{ extract_json('parsed_event_data', '$.x_validation_status') }} as x_validation_status,
        {{ extract_json('parsed_event_data', '$.x_submission_status') }} as x_submission_status,
        {{ extract_json('parsed_event_data', '$.x_component') }} as component,
        {{ extract_json('parsed_event_data', '$.technical_stage') }} as technical_stage,
        {{ extract_json('parsed_event_data', '$.technical_status') }} as technical_status,
        try_cast({{ extract_json('parsed_event_data', '$.event_timestamp') }} as timestamp) as event_timestamp,
        try_cast({{ extract_json('parsed_event_data', '$.processing_timestamp') }} as timestamp) as processing_timestamp,
        try_cast({{ extract_json('parsed_event_data', '$.x_latency_ms') }} as bigint) as x_latency_ms,
        try_cast({{ extract_json('parsed_event_data', '$.x_latency_ms') }} as bigint) as latency_ms,
        {{ extract_json('parsed_event_data', '$.x_error_code') }} as x_error_code,
        {{ extract_json('parsed_event_data', '$.x_error_category') }} as x_error_category,
        try_cast({{ extract_json('parsed_event_data', '$.x_retry_count') }} as integer) as x_retry_count,
        try_cast({{ extract_json('parsed_event_data', '$.x_timeout_indicator') }} as boolean) as x_timeout_indicator,
        {{ extract_json('parsed_event_data', '$.x_error_code') }} as error_code,
        {{ extract_json('parsed_event_data', '$.x_error_category') }} as error_category,
        try_cast({{ extract_json('parsed_event_data', '$.x_retry_count') }} as integer) as retry_count,
        try_cast({{ extract_json('parsed_event_data', '$.x_timeout_indicator') }} as boolean) as timeout_indicator,

        -- Raw fields discovered by exhaustive Raw -> Bronze parity audit
        {{ extract_json('parsed_event_data', '$.event_family') }} as raw_event_family,
        {{ extract_json('parsed_event_data', '$.event_id') }} as raw_event_id,
        {{ extract_json('parsed_event_data', '$.source_system') }} as raw_source_system,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.BusinessReference') }} as raw_xml_technical_business_reference,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.CorrelationId') }} as raw_xml_technical_correlation_id,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.EndToEndId') }} as raw_xml_technical_end_to_end_id,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.EventFamily') }} as raw_xml_technical_event_family,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.EventId') }} as raw_xml_technical_event_id,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.EventTimestamp') }} as raw_xml_technical_event_timestamp,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.EventType') }} as raw_xml_technical_event_type,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.MessageId') }} as raw_xml_technical_message_id,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.PaymentInstructionId') }} as raw_xml_technical_payment_instruction_id,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.ProcessingTimestamp') }} as raw_xml_technical_processing_timestamp,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.TechnicalStage') }} as raw_xml_technical_technical_stage,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.TechnicalStatus') }} as raw_xml_technical_technical_status,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.TransactionId') }} as raw_xml_technical_transaction_id,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.UETR') }} as raw_xml_technical_uetr,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XChannel') }} as raw_xml_technical_xchannel,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XComponent') }} as raw_xml_technical_xcomponent,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XErrorCategory') }} as raw_xml_technical_xerror_category,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XErrorCode') }} as raw_xml_technical_xerror_code,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XHost') }} as raw_xml_technical_xhost,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XIntermediaryInstitution') }} as raw_xml_technical_xintermediary_institution,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XLatencyMs') }} as raw_xml_technical_xlatency_ms,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XNode') }} as raw_xml_technical_xnode,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XOperation') }} as raw_xml_technical_xoperation,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XOriginatingInstitution') }} as raw_xml_technical_xoriginating_institution,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XPaymentRoute') }} as raw_xml_technical_xpayment_route,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XRetryCount') }} as raw_xml_technical_xretry_count,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XRouteDecision') }} as raw_xml_technical_xroute_decision,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XService') }} as raw_xml_technical_xservice,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XSourceSystem') }} as raw_xml_technical_xsource_system,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XSubmissionStatus') }} as raw_xml_technical_xsubmission_status,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XTargetSystem') }} as raw_xml_technical_xtarget_system,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XTimeoutIndicator') }} as raw_xml_technical_xtimeout_indicator,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XTrace') }} as raw_xml_technical_xtrace,
        {{ extract_json('parsed_event_data', '$.xml.TechnicalPaymentEvent.XValidationStatus') }} as raw_xml_technical_xvalidation_status,

        _kafka_metadata.topic as kafka_topic,
        _kafka_metadata.partition as kafka_partition,
        _kafka_metadata.offset as kafka_offset,
        try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
        _kafka_metadata.category as category,
        envelope_source_system as source_system,
        year, month, day, current_timestamp as load_timestamp,
        'ICMN_PMN_TECHNICAL_EVENT' as record_source
    from raw_data
)
select
    event_id, envelope_message_id, event_family, message_id, event_type,
    correlation_id, instruction_id, end_to_end_id, transaction_id, uetr,
    business_reference, x_trace, x_channel, x_source_system, x_target_system,
    x_service, x_operation, x_component, x_node, x_host,
    x_payment_route, x_originating_institution, x_intermediary_institution,
    x_route_decision, x_validation_status, x_submission_status, component,
    technical_stage, technical_status, event_timestamp, processing_timestamp,
    x_latency_ms, x_error_code, x_error_category, x_retry_count, x_timeout_indicator,
    latency_ms, error_code, error_category, retry_count, timeout_indicator,
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, category,
    source_system, year, month, day, load_timestamp, record_source,
    raw_event_family, raw_event_id, raw_source_system, raw_xml_technical_business_reference, raw_xml_technical_correlation_id, raw_xml_technical_end_to_end_id, raw_xml_technical_event_family, raw_xml_technical_event_id, raw_xml_technical_event_timestamp, raw_xml_technical_event_type, raw_xml_technical_message_id, raw_xml_technical_payment_instruction_id, raw_xml_technical_processing_timestamp, raw_xml_technical_technical_stage, raw_xml_technical_technical_status, raw_xml_technical_transaction_id, raw_xml_technical_uetr, raw_xml_technical_xchannel, raw_xml_technical_xcomponent, raw_xml_technical_xerror_category, raw_xml_technical_xerror_code, raw_xml_technical_xhost, raw_xml_technical_xintermediary_institution, raw_xml_technical_xlatency_ms, raw_xml_technical_xnode, raw_xml_technical_xoperation, raw_xml_technical_xoriginating_institution, raw_xml_technical_xpayment_route, raw_xml_technical_xretry_count, raw_xml_technical_xroute_decision, raw_xml_technical_xservice, raw_xml_technical_xsource_system, raw_xml_technical_xsubmission_status, raw_xml_technical_xtarget_system, raw_xml_technical_xtimeout_indicator, raw_xml_technical_xtrace, raw_xml_technical_xvalidation_status

from parsed
qualify row_number() over (
    partition by event_id order by kafka_timestamp desc nulls last, kafka_offset desc nulls last
) = 1
