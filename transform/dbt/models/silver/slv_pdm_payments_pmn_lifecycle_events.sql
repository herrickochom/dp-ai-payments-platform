{{ config(materialized='iceberg_table') }}

select
    message_id, end_to_end_id, creation_at, payment_information_id,
    instruction_id, processing_priority, retry_count, timeout,
    correlation_id, trace_id, tenant_id, environment
from {{ ref('br_pdm_icmn_pmn_pain001') }}
qualify row_number() over (
    partition by message_id order by kafka_timestamp desc, kafka_offset desc
) = 1
