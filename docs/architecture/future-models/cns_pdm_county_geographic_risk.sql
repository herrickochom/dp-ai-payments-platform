{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'geography', 'county']) }}

with parish_risk as (
    select * from {{ ref('cns_pdm_parish_geographic_risk') }}
),
geo as (
    select distinct
        region,
        district,
        county,
        parish
    from {{ ref('gld_dim_pdm_geography') }}
    where county is not null
)

select
    p.region,
    p.district,
    g.county,

    count(distinct p.parish_sk) as parish_count,
    count(distinct case when p.geographic_risk_score is not null then p.parish_sk end) as assessed_parish_count,

    max(p.geographic_risk_score) as geographic_risk_score,
    case max(p.geographic_risk_score)
        when 4 then 'SEVERE'
        when 3 then 'HIGH'
        when 2 then 'MEDIUM'
        when 1 then 'LOW'
        else 'NO DATA'
    end as geographic_risk_band,

    avg(p.disbursement_rate) as avg_disbursement_rate,
    avg(p.principal_repayment_rate) as avg_principal_repayment_rate,
    sum(p.high_identity_alert_count) as high_identity_alert_count,
    sum(p.account_substitution_amount) as account_substitution_amount,
    sum(p.mapped_agent_count) as mapped_agent_count,
    avg(p.latitude) filter (where p.latitude is not null) as latitude,
    avg(p.longitude) filter (where p.longitude is not null) as longitude

from parish_risk p
join geo g
  on p.region = g.region
 and p.district = g.district
 and p.parish = g.parish
group by 1, 2, 3
