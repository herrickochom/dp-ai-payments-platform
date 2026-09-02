{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'geography', 'district', 'dashboard']) }}

with district_spine as (
    select
        trim(district) as district,
        trim(superset_district_iso) as superset_district_iso,
        lower(trim(district)) as district_key
    from {{ ref('uganda_superset_district_iso') }}

), parish_base as (
    select *
    from {{ ref('cns_pdm_parish_geographic_risk') }}

), district_region_reference as (
    select
        lower(trim(district)) as district_key,
        max(region) as region
    from parish_base
    where district is not null
    group by 1

), district_metrics as (
    select
        lower(trim(district)) as district_key,

        count(distinct parish_sk) as parish_count,
        count(distinct case when geographic_risk_score is not null then parish_sk end) as assessed_parish_count,
        count(distinct case when geographic_risk_band = 'SEVERE' then parish_sk end) as severe_parish_count,
        count(distinct case when geographic_risk_band = 'HIGH' then parish_sk end) as high_parish_count,
        count(distinct case when geographic_risk_band = 'MEDIUM' then parish_sk end) as medium_parish_count,
        count(distinct case when geographic_risk_band = 'LOW' then parish_sk end) as low_parish_count,

        sum(loan_count) as loan_count,
        sum(beneficiary_count) as beneficiary_count,
        sum(approved_amount) as approved_amount,
        sum(disbursed_amount) as disbursed_amount,
        sum(repaid_amount) as repaid_amount,
        sum(outstanding_amount) as outstanding_amount,

        max(geographic_risk_score) as geographic_risk_score,

        avg(disbursement_rate) as avg_disbursement_rate,
        avg(principal_repayment_rate) as avg_principal_repayment_rate,
        avg(disbursement_peer_zscore) as avg_disbursement_peer_zscore,
        avg(repayment_peer_zscore) as avg_repayment_peer_zscore,

        sum(high_identity_alert_count) as high_identity_alert_count,
        sum(account_substitution_amount) as account_substitution_amount,
        sum(mapped_agent_count) as mapped_agent_count,

        avg(latitude) filter (where latitude is not null) as latitude,
        avg(longitude) filter (where longitude is not null) as longitude

    from parish_base
    where district is not null
    group by 1
)

select
    region.region,
    spine.district,
    spine.superset_district_iso,

    case
        when spine.superset_district_iso is null then 'UNMAPPED'
        else 'MAPPED'
    end as map_mapping_status,

    case
        when metrics.district_key is null then 'NO DATA'
        else 'HAS DATA'
    end as district_data_status,

    coalesce(metrics.parish_count, 0) as parish_count,
    coalesce(metrics.assessed_parish_count, 0) as assessed_parish_count,
    coalesce(metrics.severe_parish_count, 0) as severe_parish_count,
    coalesce(metrics.high_parish_count, 0) as high_parish_count,
    coalesce(metrics.medium_parish_count, 0) as medium_parish_count,
    coalesce(metrics.low_parish_count, 0) as low_parish_count,

    metrics.loan_count,
    metrics.beneficiary_count,
    metrics.approved_amount,
    metrics.disbursed_amount,
    metrics.repaid_amount,
    metrics.outstanding_amount,

    metrics.geographic_risk_score,

    case metrics.geographic_risk_score
        when 4 then 'SEVERE'
        when 3 then 'HIGH'
        when 2 then 'MEDIUM'
        when 1 then 'LOW'
        else 'NO DATA'
    end as geographic_risk_band,

    metrics.avg_disbursement_rate,
    metrics.avg_principal_repayment_rate,
    metrics.avg_disbursement_peer_zscore,
    metrics.avg_repayment_peer_zscore,

    metrics.high_identity_alert_count,
    metrics.account_substitution_amount,
    metrics.mapped_agent_count,
    metrics.latitude,
    metrics.longitude

from district_spine spine
left join district_metrics metrics
  on spine.district_key = metrics.district_key
left join district_region_reference region
  on spine.district_key = region.district_key
order by spine.district
