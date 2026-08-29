{{ config(materialized='iceberg_table') }}

select
    message_id, original_message_id, end_to_end_id, creation_at,
    group_status, transaction_status, reason_code, processing_status,
    error_code, retry_attempt, correlation_id, trace_id, source_timestamp
from {{ ref('br_pdm_cpo_plm_pain002') }}
qualify row_number() over (
    partition by message_id order by kafka_timestamp desc, kafka_offset desc
) = 1
