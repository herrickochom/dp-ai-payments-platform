{{ config(materialized='iceberg_table') }}

select agent_id, location_type, address, latitude, longitude, is_active
from {{ ref('br_pdm_agent_locations') }}
qualify row_number() over (
    partition by agent_id, location_type order by kafka_timestamp desc, kafka_offset desc
) = 1
