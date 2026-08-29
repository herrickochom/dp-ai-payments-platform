{{ config(materialized='iceberg_table') }}

select
    beneficiary_id, beneficiary_token, nin, nin_hashed, nin_verified,
    beneficiary_name, date_of_birth, gender, phone, alternative_phone, email,
    phone_verified, household_id, special_group_code, village, parish, district,
    region, registration_date, is_active, created_at, updated_at
from {{ ref('br_pdm_pdmis_beneficiaries') }}
qualify row_number() over (
    partition by beneficiary_id order by updated_at desc nulls last, kafka_timestamp desc, kafka_offset desc
) = 1
