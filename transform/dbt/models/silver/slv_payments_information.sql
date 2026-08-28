{{ config(materialized='iceberg_table') }}

select
    event_id,
    message_id,
    'vpm' as payment_source,
    payment_information_id,
    payment_method,
    batch_booking,
    requested_execution_at,
    number_of_transactions,
    instructed_amount,
    currency,
    purpose_code,
    creation_at,
    kafka_topic,
    kafka_partition,
    kafka_offset
from {{ ref('stg_icmn_vpm_pain001') }}
