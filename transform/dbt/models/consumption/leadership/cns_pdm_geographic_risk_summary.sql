{{ config(materialized='iceberg_table', tags=['consumption', 'risk', 'geography', 'summary', 'dashboard']) }}

with district as (
    select * from {{ ref('cns_pdm_district_geographic_risk') }}
),
sub_county as (
    select * from {{ ref('cns_pdm_subcounty_geographic_risk') }}
),
parish as (
    select * from {{ ref('cns_pdm_parish_geographic_risk') }}
)

select
    current_date as snapshot_date,

    count(distinct district) as district_count,
    count(distinct case when map_mapping_status = 'MAPPED' then district end) as mapped_district_count,
    count(distinct case when map_mapping_status = 'UNMAPPED' then district end) as unmapped_district_count,

    count(distinct case when geographic_risk_band = 'SEVERE' then district end) as severe_district_count,
    count(distinct case when geographic_risk_band = 'HIGH' then district end) as high_district_count,
    count(distinct case when geographic_risk_band = 'MEDIUM' then district end) as medium_district_count,
    count(distinct case when geographic_risk_band = 'LOW' then district end) as low_district_count,
    count(distinct case when geographic_risk_band = 'NO DATA' then district end) as no_data_district_count,

    (select count(distinct parish_sk) from parish) as parish_count,
    (select count(distinct case when geographic_risk_band = 'SEVERE' then parish_sk end) from parish) as severe_parish_count,
    (select count(distinct case when geographic_risk_band = 'HIGH' then parish_sk end) from parish) as high_parish_count,
    (select count(distinct case when geographic_risk_band = 'MEDIUM' then parish_sk end) from parish) as medium_parish_count,
    (select count(distinct case when geographic_risk_band = 'LOW' then parish_sk end) from parish) as low_parish_count,
    (select count(distinct case when geographic_risk_band = 'NO DATA' then parish_sk end) from parish) as no_data_parish_count,

    (select count(*) from sub_county) as sub_county_count,
    (select count(*) from sub_county where geographic_risk_band = 'SEVERE') as severe_sub_county_count,
    (select count(*) from sub_county where geographic_risk_band = 'HIGH') as high_sub_county_count,
    (select count(*) from sub_county where geographic_risk_band = 'MEDIUM') as medium_sub_county_count,
    (select count(*) from sub_county where geographic_risk_band = 'LOW') as low_sub_county_count,
    (select count(*) from sub_county where geographic_risk_band = 'NO DATA') as no_data_sub_county_count,

    100.0 * count(distinct case when map_mapping_status = 'MAPPED' then district end)
        / nullif(count(distinct district), 0) as district_mapping_pct,

    sum(high_identity_alert_count) as high_identity_alert_count,
    sum(account_substitution_amount) as account_substitution_amount

from district
