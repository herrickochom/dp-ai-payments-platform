{{ config(materialized='iceberg_table') }}

select group_code, group_name, description, quota_percentage
from {{ ref('br_pdm_pdmis_special_groups') }}
qualify row_number() over (
    partition by group_code order by kafka_timestamp desc, kafka_offset desc
) = 1
