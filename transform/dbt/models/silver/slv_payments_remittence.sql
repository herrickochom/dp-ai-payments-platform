{{ config(materialized='iceberg_table') }}

select
    event_id,
    message_id,
    'vpm' as payment_source,
    payment_information_id,
    remittance_type,
    creditor_reference,
    invoice_reference,
    creation_at,
    created_year,
    created_month,
    created_day,
    created_hour,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
from {{ ref('stg_icm_vpm_pain001_remittence') }}
