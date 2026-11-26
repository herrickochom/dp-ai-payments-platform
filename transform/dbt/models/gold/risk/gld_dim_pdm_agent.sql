{{ config(materialized='pdm_iceberg_guarded') }}

select
    {{ gold_surrogate_key(['agent.agent_id']) }} as agent_sk,
    {{ gold_surrogate_key(['agent.region', 'agent.district', 'agent.county', 'agent.sub_county']) }} as geography_sk,
    agent.agent_id, agent.agent_code, agent.agent_name,
    agent.network_provider, agent.commission_rate, agent.verified, agent.is_active,
    location.latitude, location.longitude
from {{ ref('slv_pdm_agents') }} agent
left join {{ ref('slv_pdm_agent_locations') }} location
  on agent.agent_id = location.agent_id
 and location.location_type = 'PRIMARY'
where agent.agent_id is not null
union all
select
    {{ gold_surrogate_key(["cast(null as string)"]) }},
    {{ gold_surrogate_key(["cast(null as string)", "cast(null as string)", "cast(null as string)", "cast(null as string)"]) }},
    null, null, 'Unknown', null, null, false, false, null, null
