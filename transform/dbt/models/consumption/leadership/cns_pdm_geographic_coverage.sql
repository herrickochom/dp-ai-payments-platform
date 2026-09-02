{{ config(materialized='iceberg_table', tags=['consumption', 'geography', 'coverage', 'dashboard']) }}

with base as (
    select * from {{ ref('cns_pdm_parish_geographic_risk') }}
)

select
    region,
    district,

    count(distinct parish_sk) as parish_count,
    count(distinct case when superset_district_iso is not null then parish_sk end) as iso_mapped_parish_count,
    count(distinct case when latitude is not null and longitude is not null then parish_sk end) as coordinate_mapped_parish_count,

    count(distinct case when coordinate_source = 'AGENT_GPS' then parish_sk end) as agent_gps_parish_count,
    count(distinct case when coordinate_source = 'REFERENCE' then parish_sk end) as reference_coordinate_parish_count,
    count(distinct case when coordinate_source = 'UNMAPPED' then parish_sk end) as unmapped_coordinate_parish_count,

    100.0 * count(distinct case when superset_district_iso is not null then parish_sk end)
        / nullif(count(distinct parish_sk), 0) as iso_mapping_pct,

    100.0 * count(distinct case when latitude is not null and longitude is not null then parish_sk end)
        / nullif(count(distinct parish_sk), 0) as coordinate_mapping_pct,

    sum(mapped_agent_count) as mapped_agent_count

from base
group by 1, 2
