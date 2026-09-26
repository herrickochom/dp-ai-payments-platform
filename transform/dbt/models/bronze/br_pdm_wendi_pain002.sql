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

    -- Raw fields discovered by exhaustive Raw -> Bronze parity audit
    {{ extract_json('parsed_event_data', '$.header.creation_date') }} as raw_header_creation_date,
    {{ extract_json('parsed_event_data', '$.header.group_status') }} as raw_header_group_status,
    {{ extract_json('parsed_event_data', '$.header.initiating_party') }} as raw_header_initiating_party,
    {{ extract_json('parsed_event_data', '$.header.message_id') }} as raw_header_message_id,
    {{ extract_json('parsed_event_data', '$.header.original_message_id') }} as raw_header_original_message_id,
    {{ extract_json('parsed_event_data', '$.header.original_message_type') }} as raw_header_original_message_type,
    {{ extract_json('parsed_event_data', '$.payload.additional_info') }} as raw_payload_additional_info,
    {{ extract_json('parsed_event_data', '$.payload.business_date') }} as raw_payload_business_date,
    {{ extract_json('parsed_event_data', '$.payload.reason_code') }} as raw_payload_reason_code,
    {{ extract_json('parsed_event_data', '$.payload.settlement_status') }} as raw_payload_settlement_status,
    {{ extract_json('parsed_event_data', '$.payload.transaction_status') }} as raw_payload_transaction_status,
    {{ extract_json('parsed_event_data', '$.source_system') }} as raw_source_system,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlCtrlSum') }} as raw_xml_orgnl_ctrl_sum,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgNmId') }} as raw_xml_orgnl_msg_nm_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlNbOfTxs') }} as raw_xml_orgnl_nb_of_txs,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlInstrId') }} as raw_xml_orgnl_instr_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlUETR') }} as raw_xml_orgnl_uetr,

    _kafka_metadata.topic as kafka_topic, _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset, try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    year, month, day, event_data, parsed_event_data, current_timestamp as load_timestamp,
    'WENDI_PAIN002' as record_source
from raw_data
where _kafka_metadata.topic = 'wendi.pain002' and parsed_event_data is not null
