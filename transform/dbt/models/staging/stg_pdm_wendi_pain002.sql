{{ config(materialized='iceberg_table') }}

with raw_data as (
    select event_id, event_data, parsed_event_data, _kafka_metadata, year, month, day
    from read_avro(
        's3://{{ var("s3_bucket") }}/{{ var("s3_path") }}/v2/**/topic=wendi.pain002/**/*.avro',
        hive_partitioning = true
    )
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.GrpHdr.MsgId') }} as message_id,
    try_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.GrpHdr.CreDtTm') }} as timestamp) as creation_at,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgId') }} as original_message_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.GrpSts') }} as group_status,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlPmtInfId') }} as original_payment_information_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlEndToEndId') }} as end_to_end_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlTxId') }} as original_transaction_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.TxSts') }} as transaction_status,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.StsRsnInf.Rsn.Cd') }} as status_reason_code,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.StsRsnInf.AddtlInf') }} as status_additional_info,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt') }} as complete_status_report,
    _kafka_metadata.topic as kafka_topic, _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset, try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    year, month, day, event_data, parsed_event_data, current_timestamp as load_timestamp,
    'WENDI_PAIN002' as record_source
from raw_data
where _kafka_metadata.topic = 'wendi.pain002' and parsed_event_data is not null
