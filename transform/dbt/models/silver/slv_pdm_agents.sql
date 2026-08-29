{{ config(materialized='iceberg_table') }}

select
    agent_id, agent_code, agent_name, phone, registration_number, registration_date,
    network_provider, commission_rate, parish, district, region, verified, is_active,
    created_at, updated_at
from {{ ref('br_pdm_agent_profiles') }}
qualify row_number() over (
    partition by agent_id order by updated_at desc nulls last, kafka_timestamp desc, kafka_offset desc
) = 1
