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
 where _kafka_topic = 'cpo.psn.pain002'
),

parsed as (

    select
        event_id,

        -- message-specific fields
        {{ extract_json('parsed_event_data', '$.header.message_id') }} as message_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.GrpHdr.MsgId') }} as xml_message_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgId') }}
            as original_message_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlEndToEndId') }}
            as end_to_end_id,

        safe_cast({{ extract_json('parsed_event_data', '$.header.creation_date') }} as timestamp)
            as creation_at,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.GrpHdr.CreDtTm') }} as timestamp)
            as xml_creation_at,

        {{ extract_json('parsed_event_data', '$.xml.Document.CstmrPmtStsRpt.GrpHdr.InitgPty.Nm') }} as initiating_party,
        {{ extract_json('parsed_event_data', '$.header.original_message_id') }} as original_message_id_flat,
        {{ extract_json('parsed_event_data', '$.header.original_message_type') }} as original_message_type,
        {{ extract_json('parsed_event_data', '$.header.group_status') }} as group_status,
        {{ extract_json('parsed_event_data', '$.payload.transaction_status') }} as transaction_status,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.GrpSts') }}
            as xml_group_status,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.TxSts') }}
            as xml_transaction_status,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlMsgNmId') }}
            as original_message_name_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlNbOfTxs') }}
            as original_number_of_transactions,

        safe_cast({{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlGrpInfAndSts.OrgnlCtrlSum') }} as float64)
            as original_control_sum,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlPmtInfId') }}
            as original_payment_information_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlInstrId') }}
            as original_instruction_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlTxId') }}
            as original_transaction_id,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.OrgnlUETR') }}
            as original_uetr,
        coalesce(
            nullif({{ extract_json('parsed_event_data', '$.payload.reason_code') }}, ''),
            {{ extract_json('parsed_event_data',
                '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.StsRsnInf.Rsn.Cd') }}
        ) as reason_code,
        {{ extract_json('parsed_event_data', '$.payload.additional_info') }} as additional_info,

        {{ extract_json('parsed_event_data',
            '$.xml.Document.CstmrPmtStsRpt.OrgnlPmtInfAndSts.StsRsnInf.AddtlInf') }}
            as xml_additional_info,
        {{ extract_json('parsed_event_data', '$.payload.settlement_status') }} as settlement_status,
        safe_cast({{ extract_json('parsed_event_data', '$.payload.business_date') }} as date)
            as business_date,


        -- Raw fields discovered by exhaustive Raw -> Bronze parity audit
        {{ extract_json('parsed_event_data', '$.header.initiating_party') }} as raw_header_initiating_party,
        {{ extract_json('parsed_event_data', '$.source_system') }} as raw_source_system,

        _kafka_metadata.topic as kafka_topic,
        _kafka_metadata.`partition` as kafka_partition,
        _kafka_metadata.offset as kafka_offset,
        safe_cast(_kafka_metadata.timestamp as timestamp) as kafka_timestamp,

        _kafka_metadata.category as category,
        upper(SPLIT(_kafka_metadata.topic, '.')[SAFE_OFFSET(0)]) as source_system,
        SPLIT(_kafka_metadata.topic, '.')[SAFE_OFFSET(1)] as source_group,

        year,
        month,
        day,

        event_data,
        parsed_event_data,

        current_timestamp() as load_timestamp,
        'CPO_PSN_PAIN002' as record_source

    from raw_data
    where _kafka_metadata.topic = 'cpo.psn.pain002'
      and parsed_event_data is not null

)

select *
from parsed
