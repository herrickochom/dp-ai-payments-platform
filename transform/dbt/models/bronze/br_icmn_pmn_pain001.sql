{{ config(materialized='iceberg_table') }}

select
    event_id,
    message_id,
    try_cast({{ extract_json('parsed_event_data', '$.creation_date') }} as timestamp) as creation_at,
    try_cast({{ extract_json('parsed_event_data', '$.instructed_amount') }} as double) as instructed_amount,
    {{ extract_json('parsed_event_data', '$.currency') }} as currency,
    {{ extract_json('parsed_event_data', '$.debtor') }} as debtor_name,
    {{ extract_json('parsed_event_data', '$.creditor') }} as creditor_name,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.NbOfTxs') }} as number_of_transactions,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.InitgPty') }} as initiating_party,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.PmtInfId') }} as payment_information_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.PmtMtd') }} as payment_method,
    try_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.BtchBookg') }} as boolean) as batch_booking,
    try_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.ReqdExctnDt') }} as timestamp) as requested_execution_at,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.DbtrAcct.Id') }} as debtor_account_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.DbtrAgt.FinInstnId') }} as debtor_agent_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAcct.Id') }} as creditor_account_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAgt.FinInstnId') }} as creditor_agent_id,
    try_cast({{ extract_json('parsed_event_data', '$.x_attributes.x-processingPriority') }} as integer) as processing_priority,
    try_cast({{ extract_json('parsed_event_data', '$.x_attributes.x-retryCount') }} as integer) as retry_count,
    {{ extract_json('parsed_event_data', '$.x_attributes.x-timeout') }} as timeout,
    {{ extract_json('parsed_event_data', '$.x_attributes.x-correlationId') }} as correlation_id,
    {{ extract_json('parsed_event_data', '$.x_attributes.x-traceId') }} as trace_id,
    {{ extract_json('parsed_event_data', '$.x_attributes.x-spanId') }} as span_id,
    {{ extract_json('parsed_event_data', '$.x_attributes.x-parentSpanId') }} as parent_span_id,
    {{ extract_json('parsed_event_data', '$.x_attributes.x-tenantId') }} as tenant_id,
    {{ extract_json('parsed_event_data', '$.x_attributes.x-environment') }} as environment,
    event_data,
    parsed_event_data,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    _kafka_metadata.timestamp as kafka_timestamp
from {{ bronze_valid_events() }}
where _kafka_metadata.topic = 'icmn.pmn.pain001'
  and parsed_event_data is not null
