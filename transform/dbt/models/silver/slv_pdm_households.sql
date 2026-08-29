{{ config(materialized='iceberg_table') }}

select
    household_id, head_of_household, head_phone, member_count, adults_count,
    children_count, economic_status, food_security_status, housing_type,
    land_ownership, village, parish, district, region, registration_date,
    created_at, updated_at
from {{ ref('br_pdm_pdmis_households') }}
qualify row_number() over (
    partition by household_id order by updated_at desc nulls last, kafka_timestamp desc, kafka_offset desc
) = 1
