-- Every named child represented by gld_dim_pdm_geography must resolve
-- to exactly one parent within its district.
--
-- The Gold geography dimension intentionally stops at sub-county grain.
-- Parish-level analytical geography is modelled separately in Consumption;
-- village geography is operational agent coverage only.

with hierarchy_violations as (
    select
        'county' as level_name,
        district,
        county as child_name
    from {{ ref('gld_dim_pdm_geography') }}
    where county is not null
    group by district, county
    having count(distinct region) > 1

    union all

    select
        'sub_county' as level_name,
        district,
        sub_county as child_name
    from {{ ref('gld_dim_pdm_geography') }}
    where sub_county is not null
    group by district, sub_county
    having count(distinct county) > 1
)

select *
from hierarchy_violations
