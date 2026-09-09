with iso_reference as (
    select
        trim(district) as district,
        trim(superset_district_iso) as superset_district_iso
    from {{ ref('uganda_superset_district_iso') }}
),
geometry as (
    select
        trim(district) as district,
        trim(superset_district_iso) as superset_district_iso,
        count(*) as geometry_count
    from {{ ref('uganda_district_geojson') }}
    group by 1, 2
)

select
    coalesce(iso.district, geometry.district) as district,
    coalesce(iso.superset_district_iso, geometry.superset_district_iso) as superset_district_iso,
    case
        when iso.district is null then 'geometry_without_iso_reference'
        when geometry.district is null then 'iso_reference_without_geometry'
        when geometry.geometry_count <> 1 then 'duplicate_geometry'
    end as failure
from iso_reference iso
full outer join geometry
    on iso.district = geometry.district
    and iso.superset_district_iso = geometry.superset_district_iso
where
    iso.district is null
    or geometry.district is null
    or geometry.geometry_count <> 1
