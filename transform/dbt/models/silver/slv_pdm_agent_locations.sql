{{ config(materialized='table') }}

with ranked_locations as (

    select
        agent_id,
        location_type,
        address,
        latitude,
        longitude,
        is_active,

        row_number() over (
            partition by agent_id, location_type
            order by
                kafka_timestamp desc,
                kafka_offset desc
        ) as rn

    from {{ source('bronze', 'br_pdm_agent_locations') }}

)

select
    agent_id,
    location_type,
    address,
    latitude,
    longitude,
    is_active

from ranked_locations
where rn = 1
