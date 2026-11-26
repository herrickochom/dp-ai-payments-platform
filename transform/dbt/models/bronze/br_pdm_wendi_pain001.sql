{{ config(materialized='pdm_iceberg_guarded') }}

with raw_data as (
 select *, struct(_kafka_topic as topic, _kafka_partition as `partition`,
 _kafka_offset as offset, _kafka_timestamp as timestamp,
 _kafka_category as category, _kafka_source_group as source_group,
 _kafka_source_system as source_system) as _kafka_metadata,
 extract(year from _kafka_timestamp) as year,
 extract(month from _kafka_timestamp) as month,
 extract(day from _kafka_timestamp) as day
 from {{ source('pdm_bronze_input','payment_events') }}
 where _kafka_topic = 'wendi.pain001'
)

select
    event_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.MsgId') }} as message_id,
    safe_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.CreDtTm') }} as timestamp) as creation_at,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.NbOfTxs') }} as number_of_transactions,
    safe_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.GrpHdr.CtrlSum') }} as numeric) as control_sum,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.PmtInfId') }} as payment_information_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.PmtMtd') }} as payment_method,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.InstrId') }} as instruction_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.EndToEndId') }} as end_to_end_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.TxId') }} as transaction_id,
    {{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.PmtId.UETR') }} as uetr,
    safe_cast({{ extract_json('parsed_event_data', '$.xml.Document.CstmrCdtTrfInitn.PmtInf.CdtTrfTxInf.Amt.InstdAmt._text') }} as numeric) as instructed_amount,
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
    safe_cast({{ extract_json('parsed_event_data', '$.instructed_amount') }} as float64) as raw_instructed_amount,
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

    _kafka_metadata.topic as kafka_topic, _kafka_metadata.`partition` as kafka_partition,
    _kafka_metadata.offset as kafka_offset, safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,
    year, month, day, event_data, parsed_event_data, current_timestamp() as load_timestamp,
    'WENDI_PAIN001' as record_source
from raw_data
where _kafka_metadata.topic = 'wendi.pain001' and parsed_event_data is not null
