{{ config(materialized='iceberg_table') }}

-- One business batch record per VPM payment-information instruction.
select
    event_id,
    message_id,
    payment_information_id as batch_id,
    batch_booking,
    try_cast(number_of_transactions as bigint) as number_of_transactions,
    instructed_amount as control_sum,
    currency,
    requested_execution_at,
    creation_at,
    kafka_topic,
    kafka_partition,
    kafka_offset
from {{ ref('br_pdm_icmn_vpm_pain001') }}
