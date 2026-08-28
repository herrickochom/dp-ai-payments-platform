{{ config(materialized='iceberg_table') }}

-- API-facing PAIN.001 contract sourced from the VPM business instruction.
select
    event_id,
    message_id,
    creation_at,
    payment_information_id,
    payment_method,
    batch_booking,
    requested_execution_at,
    instructed_amount,
    currency,
    debtor_name,
    creditor_name,
    debtor_account_id,
    creditor_account_id,
    purpose_code,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
from {{ ref('br_icmn_vpm_pain001') }}
