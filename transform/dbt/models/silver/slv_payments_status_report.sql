{{ config(materialized='iceberg_table') }}

select
    event_id,
    message_id,
    creation_at,
    initiating_party,
    original_message_id,
    original_message_type,
    group_status,
    transaction_status,
    reason_code,
    additional_info,
    'psn' as status_source,
    'business' as status_kind,
    settlement_status,
    business_date,
    event_data,
    parsed_event_data,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
from {{ ref('stg_cpo_psn_pain002') }}
