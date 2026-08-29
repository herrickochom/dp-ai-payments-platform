{{ config(materialized='iceberg_table') }}

select
    sacco_id, sacco_name, registration_number, registration_date, wendi_account,
    postbank_account, chairperson, secretary, treasurer, office_address,
    office_exists, parish, sub_county, district, region, number_of_beneficiaries,
    total_funds_received, total_funds_disbursed, total_repayments, is_active,
    created_at, updated_at
from {{ ref('br_pdm_pdmis_saccos') }}
qualify row_number() over (
    partition by sacco_id order by updated_at desc nulls last, kafka_timestamp desc, kafka_offset desc
) = 1
