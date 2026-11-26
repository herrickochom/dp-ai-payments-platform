{{ config(materialized='pdm_iceberg_guarded') }}

with ranked as (
    select
        group_code,
        group_name,
        description,
        quota_percentage,
        row_number() over (
            partition by group_code
            order by kafka_timestamp desc,
                     kafka_offset desc
        ) as _row_number
    from {{ ref('br_pdm_pdmis_special_groups') }}
)

select
    group_code,
    group_name,
    description,
    quota_percentage
from ranked
where _row_number = 1
