{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'geography', 'parish']) }}

with identity_alerts as (
    select
        {{ gold_surrogate_key(['geography.region', 'geography.district', 'geography.parish']) }} as parish_sk,
        count(*) filter (where identity_risk_band = 'HIGH') as high_identity_alert_count,
        sum(account_substitution_amount) as account_substitution_amount
    from {{ ref('cns_pdm_beneficiary_identity_alerts') }} identity
    left join {{ ref('gld_dim_pdm_geography') }} geography
      on identity.geography_sk = geography.geography_sk
    group by 1

), agent_location_centroids as (
    select
        geography.region,
        geography.district,
        geography.parish,
        avg(agent.latitude) as latitude,
        avg(agent.longitude) as longitude,
        count(*) as mapped_agent_count
    from {{ ref('gld_dim_pdm_agent') }} agent
    join {{ ref('gld_dim_pdm_geography') }} geography using (geography_sk)
    where agent.latitude between -1.6 and 4.3
      and agent.longitude between 29.4 and 35.1
    group by 1, 2, 3

), reference_coordinates(region, district, parish, latitude, longitude) as (
    values
        ('Central', 'Masaka', 'Nyendo', -0.293889, 31.735000),
        ('Eastern', 'Bukedea', 'Bukedea Central', 1.347500, 34.044444),
        ('Eastern', 'Kamuli', 'Kamuli Central', 0.945000, 33.125000),
        ('Eastern', 'Kumi', 'Kumi Central', 1.493334, 33.937500),
        ('Eastern', 'Mbale', 'Namakwekwe', 1.089897, 34.182768),
        ('Karamoja', 'Moroto', 'Camp Swahili', 2.524600, 34.661910)

), parish_metrics as (
    select
        parish.*,
        (
            parish.disbursement_rate
            - avg(parish.disbursement_rate) over (partition by parish.region)
        )
        / nullif(
            stddev_pop(parish.disbursement_rate) over (partition by parish.region),
            0
        ) as disbursement_peer_zscore,
        (
            parish.principal_repayment_rate
            - avg(parish.principal_repayment_rate) over (partition by parish.region)
        )
        / nullif(
            stddev_pop(parish.principal_repayment_rate) over (partition by parish.region),
            0
        ) as repayment_peer_zscore
    from {{ ref('cns_pdm_parish_performance') }} parish

), district_iso_map as (
    select
        lower(trim(district)) as district_key,
        superset_district_iso
    from {{ ref('uganda_superset_district_iso') }}

), scored as (
    select
        parish.*,
        map.superset_district_iso,
        case
            when map.superset_district_iso is null then 'UNMAPPED'
            else 'MAPPED'
        end as map_mapping_status,
        coalesce(location.latitude, reference.latitude) as latitude,
        coalesce(location.longitude, reference.longitude) as longitude,
        case
            when location.latitude is not null then 'AGENT_GPS'
            when reference.latitude is not null then 'REFERENCE'
            else 'UNMAPPED'
        end as coordinate_source,
        coalesce(location.mapped_agent_count, 0) as mapped_agent_count,
        coalesce(identity.high_identity_alert_count, 0) as high_identity_alert_count,
        coalesce(identity.account_substitution_amount, 0) as account_substitution_amount
    from parish_metrics parish
    left join identity_alerts identity using (parish_sk)
    left join agent_location_centroids location
      on parish.region = location.region
     and parish.district = location.district
     and parish.parish = location.parish
    left join reference_coordinates reference
      on parish.region = reference.region
     and parish.district = reference.district
     and parish.parish = reference.parish
    left join district_iso_map map
      on lower(trim(parish.district)) = map.district_key
)

select
    scored.*,
    case
        when superset_district_iso is null then 'NO DATA'
        when disbursement_peer_zscore is null
         and repayment_peer_zscore is null
         and high_identity_alert_count = 0 then 'NO DATA'
        when high_identity_alert_count >= 3
          or abs(coalesce(disbursement_peer_zscore, 0)) >= 3
          or coalesce(repayment_peer_zscore, 0) <= -3 then 'SEVERE'
        when high_identity_alert_count > 0
          or abs(coalesce(disbursement_peer_zscore, 0)) >= 2
          or coalesce(repayment_peer_zscore, 0) <= -2 then 'HIGH'
        when abs(coalesce(disbursement_peer_zscore, 0)) >= 1
          or coalesce(repayment_peer_zscore, 0) <= -1 then 'MEDIUM'
        else 'LOW'
    end as geographic_risk_band,

    case
        when superset_district_iso is null then null
        when disbursement_peer_zscore is null
         and repayment_peer_zscore is null
         and high_identity_alert_count = 0 then null
        when high_identity_alert_count >= 3
          or abs(coalesce(disbursement_peer_zscore, 0)) >= 3
          or coalesce(repayment_peer_zscore, 0) <= -3 then 4
        when high_identity_alert_count > 0
          or abs(coalesce(disbursement_peer_zscore, 0)) >= 2
          or coalesce(repayment_peer_zscore, 0) <= -2 then 3
        when abs(coalesce(disbursement_peer_zscore, 0)) >= 1
          or coalesce(repayment_peer_zscore, 0) <= -1 then 2
        else 1
    end as geographic_risk_score
from scored
