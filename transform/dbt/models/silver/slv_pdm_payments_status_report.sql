{{ config(materialized='iceberg_table') }}

with status_events as (
    select 'CPO_PSN' as status_source, message_id, original_message_id,
           original_transaction_id, end_to_end_id, creation_at as status_at,
           group_status, transaction_status, reason_code, additional_info,
           kafka_timestamp, kafka_offset
    from {{ ref('br_pdm_cpo_psn_pain002') }}
    union all
    select 'CPO_PLM', message_id, original_message_id, original_transaction_id,
           end_to_end_id, creation_at, group_status, transaction_status,
           reason_code, additional_info, kafka_timestamp, kafka_offset
    from {{ ref('br_pdm_cpo_plm_pain002') }}
    union all
    select 'WENDI', message_id, original_message_id, original_transaction_id,
           end_to_end_id, creation_at, group_status, transaction_status,
           status_reason_code, status_additional_info, kafka_timestamp, kafka_offset
    from {{ ref('br_pdm_wendi_pain002') }}
    union all
    select 'MTN', message_id, cast(null as varchar), original_transaction_id,
           end_to_end_id, creation_at, cast(null as varchar), transaction_status,
           status_reason_code, status_additional_info, kafka_timestamp, kafka_offset
    from {{ ref('br_pdm_mobile_mtn_pacs002') }}
    union all
    select 'AIRTEL', message_id, null, original_transaction_id,
           end_to_end_id, creation_at, null, transaction_status,
           status_reason_code, status_additional_info, kafka_timestamp, kafka_offset
    from {{ ref('br_pdm_mobile_airtel_pacs002') }}
)
select * exclude (kafka_timestamp, kafka_offset)
from status_events
qualify row_number() over (
    partition by status_source, message_id order by kafka_timestamp desc, kafka_offset desc
) = 1
