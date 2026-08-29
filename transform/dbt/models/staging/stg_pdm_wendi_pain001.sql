{{ config(materialized='view') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=wendi.pain001/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.MsgId') }} as message_id,
    try_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.CreDtTm') }} as timestamp) as creation_at,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.NbOfTxs') }} as number_of_transactions,
    try_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.CtrlSum') }} as decimal(18, 2)) as control_sum,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.PmtInfId') }} as payment_information_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.PmtMtd') }} as payment_method,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.InstrId') }} as instruction_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.EndToEndId') }} as end_to_end_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.TxId') }} as transaction_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.UETR') }} as uetr,
    try_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Amt.InstdAmt._text') }} as decimal(18, 2)) as instructed_amount,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Amt.InstdAmt._attributes.Ccy') }} as currency,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.Dbtr.Nm') }} as debtor_name,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.DbtrAcct.Id.Othr.Id') }} as debtor_account_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Cdtr.Nm') }} as creditor_name,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAcct.Id.Othr.Id') }} as creditor_account_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.RmtInf.Ustrd') }} as remittance_information,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn') }} as complete_message,
    _kafka_metadata.topic as kafka_topic, _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset, try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    year, month, day, event_data, parsed_event_data, current_timestamp as load_timestamp,
    'WENDI_PAIN001' as record_source
from raw_data
where _kafka_metadata.topic = 'wendi.pain001' and parsed_event_data is not null
