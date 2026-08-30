{{ config(materialized='iceberg_table') }}

with geographies as (
    select region, district, parish, village from {{ ref('slv_pdm_beneficiaries') }}
    union
    select region, district, parish, cast(null as varchar) from {{ ref('slv_pdm_saccos') }}
    union
    select region, district, parish, cast(null as varchar) from {{ ref('slv_pdm_agents') }}
    union
    select null, null, null, null
)
select
    {{ gold_surrogate_key(['region', 'district', 'parish', 'village']) }} as geography_sk,
    region, district, parish, village,
    'Uganda' as country_name,
    'UG' as country_code
from geographies
