{{ config(materialized='iceberg_table') }}

select
    event_id,
    message_id,
    'vpm' as source_system,
    'icmn' as source_category,
    creation_at,
    instructed_amount,
    currency,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp
from {{ ref('br_pdm_icmn_vpm_pain001') }}
