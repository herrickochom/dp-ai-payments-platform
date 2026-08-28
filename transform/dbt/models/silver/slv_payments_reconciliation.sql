{{ config(materialized='iceberg_table') }}

select
    status.event_id as status_event_id,
    status.message_id as status_message_id,
    status.original_message_id,
    payment.event_id as payment_event_id,
    payment.message_id as payment_message_id,
    'psn' as status_source,
    'business' as status_kind,
    status.group_status,
    status.transaction_status,
    status.reason_code,
    case
        when payment.message_id is null then 'UNMATCHED'
        else 'MATCHED'
    end as reconciliation_status,
    status.kafka_topic as status_kafka_topic,
    status.kafka_partition as status_kafka_partition,
    status.kafka_offset as status_kafka_offset
from {{ ref('stg_cpo_psn_pain002') }} status
left join {{ ref('stg_icmn_vpm_pain001') }} payment
  on status.original_message_id = payment.message_id
