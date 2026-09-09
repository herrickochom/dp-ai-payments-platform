-- Every named child must resolve to exactly one parent within its district.
with hierarchy_violations as (
    select 'county' as level_name, district, county as child_name
    from {{ ref('gld_dim_pdm_geography') }}
    where county is not null
    group by district, county
    having count(distinct region) > 1

    union all

    select 'sub_county', district, sub_county
    from {{ ref('gld_dim_pdm_geography') }}
    where sub_county is not null
    group by district, sub_county
    having count(distinct county) > 1

    union all

    select 'parish', district, parish
    from {{ ref('gld_dim_pdm_geography') }}
    where parish is not null and sub_county is not null
    group by district, parish
    having count(distinct sub_county) > 1

    union all

    select 'village', district, village
    from {{ ref('gld_dim_pdm_geography') }}
    where village is not null
    group by district, village
    having count(distinct parish) > 1
)

select * from hierarchy_violations
