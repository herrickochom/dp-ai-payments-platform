{{ config(materialized='iceberg_table') }}

-- One business batch record per VPM payment-information instruction.
with ranked as (
    select
        message_id,
        payment_information_id as batch_id,
        batch_booking,
        try_cast(number_of_transactions as bigint) as number_of_transactions,
        group_control_sum as control_sum,
        currency,
        requested_execution_at,
        creation_at,
        row_number() over (
            partition by payment_information_id
            order by kafka_timestamp desc, kafka_offset desc
        ) as _row_number
    from {{ source('bronze', 'br_pdm_icmn_vpm_pain001') }}
)

select
    message_id,
    batch_id,
    batch_booking,
    number_of_transactions,
    control_sum,
    currency,
    requested_execution_at,
    creation_at
from ranked
where _row_number = 1
