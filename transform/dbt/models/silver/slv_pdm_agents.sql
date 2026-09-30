{{ config(materialized='table') }}

with ranked_agents as (

    select
        agent_id,
        agent_code,
        agent_name,
        phone,
        registration_number,
        registration_date,
        network_provider,
        commission_rate,
        country,
        village,
        parish,
        sub_county,
        county,
        district,
        region,
        verified,
        is_active,
        created_at,
        updated_at,

        row_number() over (
            partition by agent_id
            order by
                updated_at desc nulls last,
                kafka_timestamp desc,
                kafka_offset desc
        ) as rn

    from {{ source('bronze', 'br_pdm_agent_profiles') }}

)

select
    agent_id,
    agent_code,
    agent_name,
    phone,
    registration_number,
    registration_date,
    network_provider,
    commission_rate,
    country,
    village,
    parish,
    sub_county,
    county,
    district,
    region,
    verified,
    is_active,
    created_at,
    updated_at

from ranked_agents
where rn = 1
