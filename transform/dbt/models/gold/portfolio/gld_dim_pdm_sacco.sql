{{ config(materialized='iceberg_table') }}

select
    {{ gold_surrogate_key(['sacco.sacco_id']) }} as sacco_sk,
    {{ gold_surrogate_key(['sacco.region', 'sacco.district', 'sacco.parish', "cast(null as varchar)"]) }} as geography_sk,
    sacco.sacco_id, sacco.sacco_name, sacco.registration_number,
    sacco.registration_date, sacco.office_exists, sacco.number_of_beneficiaries,
    sacco.is_active
from {{ ref('slv_pdm_saccos') }} sacco
where sacco.sacco_id is not null
union all
select
    {{ gold_surrogate_key(["cast(null as varchar)"]) }},
    {{ gold_surrogate_key(["cast(null as varchar)", "cast(null as varchar)", "cast(null as varchar)", "cast(null as varchar)"]) }},
    null, 'Unknown', null, null, null, null, false
