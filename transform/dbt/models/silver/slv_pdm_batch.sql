{{ config(materialized='iceberg_table') }}

-- One business batch record per VPM payment-information instruction.
select
    message_id,
    payment_information_id as batch_id,
    batch_booking,
    try_cast(number_of_transactions as bigint) as number_of_transactions,
    group_control_sum as control_sum,
    currency,
    requested_execution_at,
    creation_at
from {{ ref('br_pdm_icmn_vpm_pain001') }}
qualify row_number() over (
    partition by payment_information_id order by kafka_timestamp desc, kafka_offset desc
) = 1
