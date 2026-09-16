{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'risk', 'geography', 'village']
) }}

-- Gate 2 forbids projecting or inferring beneficiary identity risk at village
-- grain. This product is retained as legitimate agent-location coverage only;
-- risk and financial measures are explicitly unavailable, never fabricated.

with village_coverage as (
    select
        location.region,
        location.district,
        location.parish,
        location.village,
        {{ gold_surrogate_key(['location.region', 'location.district', 'location.parish']) }} as parish_sk,
        count(*) as mapped_agent_count,
        avg(agent.latitude) as latitude,
        avg(agent.longitude) as longitude
    from {{ ref('gld_dim_pdm_agent') }} agent
    join {{ ref('slv_pdm_agents') }} location using (agent_id)
    where location.village is not null
      and agent.latitude between -1.6 and 4.3
      and agent.longitude between 29.4 and 35.1
    group by 1, 2, 3, 4
)

select
    region,
    district,
    parish,
    village,
    parish_sk,
    cast(null as integer) as geographic_risk_score,
    cast(null as varchar) as geographic_risk_band,
    'OPERATIONAL_AGENT_COVERAGE_ONLY' as risk_grain_source,
    cast(null as double) as disbursement_rate,
    cast(null as double) as principal_repayment_rate,
    cast(null as double) as disbursement_peer_zscore,
    cast(null as double) as repayment_peer_zscore,
    cast(null as bigint) as high_identity_alert_count,
    cast(null as double) as account_substitution_amount,
    mapped_agent_count,
    latitude,
    longitude,
    'AGENT_GPS' as coordinate_source,
    cast(null as varchar) as superset_district_iso,
    'NOT_APPLICABLE_AT_VILLAGE' as map_mapping_status
from village_coverage
