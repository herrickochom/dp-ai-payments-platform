{{ config(
    materialized='iceberg_table',
    tags=['consumption', 'risk', 'geography', 'village']
) }}

-- Village risk is inherited from the parish until village-grain performance
-- facts are available. This preserves the current drill hierarchy without
-- overstating the granularity of the underlying risk calculation.

with parish_risk as (
    select *
    from {{ ref('cns_pdm_parish_geographic_risk') }}
),

geo as (
    select distinct
        region,
        district,
        parish,
        village
    from {{ ref('gld_dim_pdm_geography') }}
    where village is not null
)

select
    p.region,
    p.district,
    p.parish,
    g.village,
    p.parish_sk,

    p.geographic_risk_score,
    p.geographic_risk_band,
    'PARISH_INHERITED' as risk_grain_source,

    p.disbursement_rate,
    p.principal_repayment_rate,
    p.disbursement_peer_zscore,
    p.repayment_peer_zscore,

    p.high_identity_alert_count,
    p.account_substitution_amount,

    p.mapped_agent_count,
    p.latitude,
    p.longitude,
    p.coordinate_source,

    p.superset_district_iso,
    p.map_mapping_status

from parish_risk p
join geo g
  on p.region = g.region
 and p.district = g.district
 and p.parish = g.parish