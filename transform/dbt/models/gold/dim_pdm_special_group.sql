{{ config(materialized='iceberg_table') }}

select
    {{ gold_surrogate_key(['group_code']) }} as special_group_sk,
    group_code, group_name, description, quota_percentage
from {{ ref('slv_pdm_special_groups') }}
where group_code is not null
union all
select {{ gold_surrogate_key(["cast(null as varchar)"]) }}, null, 'Unknown',
       'No special group supplied', null
