{{ config(materialized='iceberg_table') }}

with geographies as (
    select region, district, county, sub_county from {{ ref('slv_pdm_beneficiaries') }}
    union
    select region, district, county, sub_county from {{ ref('slv_pdm_saccos') }}
    union
    select region, district, county, sub_county from {{ ref('slv_pdm_agents') }}
    union
    select null, null, null, null
)
select
    {{ gold_surrogate_key(['region', 'district', 'county', 'sub_county']) }} as geography_sk,
    {{ gold_surrogate_key(['region', 'district']) }} as district_sk,
    {{ gold_surrogate_key(['region', 'district', 'county']) }} as county_sk,
    {{ gold_surrogate_key(['region', 'district', 'county', 'sub_county']) }} as sub_county_sk,
    region, district, county, sub_county,
    'Uganda' as country_name,
    'UG' as country_code
from geographies
