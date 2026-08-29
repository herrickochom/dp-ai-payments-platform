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
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Purp') }} as purpose_code,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.InvRef1.Ref') }} as invoice_reference_1,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.InvRef2.Ref') }} as invoice_reference_2,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.InvRef3.Ref') }} as invoice_reference_3,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.InvRef4.Ref') }} as invoice_reference_4,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.InvRef5.Ref') }} as invoice_reference_5,
    event_data,
    parsed_event_data,
    _kafka_metadata.topic as kafka_topic,
    _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset,
    _kafka_metadata.timestamp as kafka_timestamp
from {{ raw_valid_events() }}
where _kafka_metadata.topic = 'icmn.vpm.pain001'
  and parsed_event_data is not null
