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

    -- Raw fields discovered by exhaustive Raw -> Bronze parity audit
    {{ extract_json('parsed_event_data', '$.creation_date') }} as raw_creation_date,
    {{ extract_json('parsed_event_data', '$.creditor') }} as raw_creditor,
    {{ extract_json('parsed_event_data', '$.currency') }} as raw_currency,
    {{ extract_json('parsed_event_data', '$.debtor') }} as raw_debtor,
    {{ extract_json('parsed_event_data', '$.event_id') }} as raw_event_id,
    try_cast({{ extract_json('parsed_event_data', '$.instructed_amount') }} as double) as raw_instructed_amount,
    {{ extract_json('parsed_event_data', '$.message_id') }} as raw_message_id,
    {{ extract_json('parsed_event_data', '$.source_system') }} as raw_source_system,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.InitgPty.Nm') }} as raw_xml_nm,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.BtchBookg') }} as raw_xml_btch_bookg,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAcct.Id.Othr.Issr') }} as raw_xml_issr,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAcct.Id.Othr.SchmeNm.Prtry') }} as raw_xml_prtry,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAgt.FinInstnId.BICFI') }} as raw_xml_bicfi,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.CdtrAgt.FinInstnId.Nm') }} as raw_xml_nm_2,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CtrlSum') }} as raw_xml_ctrl_sum,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.DbtrAcct.Id.Othr.Issr') }} as raw_xml_issr_2,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.DbtrAcct.Id.Othr.SchmeNm.Prtry') }} as raw_xml_prtry_2,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.DbtrAgt.FinInstnId.BICFI') }} as raw_xml_bicfi_2,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.DbtrAgt.FinInstnId.Nm') }} as raw_xml_nm_3,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.NbOfTxs') }} as raw_xml_nb_of_txs,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.ReqdExctnDt.Dt') }} as raw_xml_dt,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.UltmtDbtr.Nm') }} as raw_xml_nm_4,

    _kafka_metadata.topic as kafka_topic, _kafka_metadata.partition as kafka_partition,
    _kafka_metadata.offset as kafka_offset, try_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    year, month, day, event_data, parsed_event_data, current_timestamp as load_timestamp,
    'WENDI_PAIN001' as record_source
from raw_data
where _kafka_metadata.topic = 'wendi.pain001' and parsed_event_data is not null
