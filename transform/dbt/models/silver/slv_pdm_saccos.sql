{{ config(materialized='iceberg_table') }}

with ranked as (
    select
        sacco_id,
        sacco_name,
        registration_number,
        registration_date,
        wendi_account,
        postbank_account,
        chairperson,
        secretary,
        treasurer,
        office_address,
        office_exists,
        village,
        parish,
        sub_county,
        county,
        district,
        region,
        number_of_beneficiaries,
        total_funds_received,
        total_funds_disbursed,
        total_repayments,
        is_active,
        created_at,
        updated_at,
        row_number() over (
            partition by sacco_id
            order by updated_at desc nulls last,
                     kafka_timestamp desc,
                     kafka_offset desc
        ) as _row_number
    from {{ source('bronze', 'br_pdm_pdmis_saccos') }}
)

select
    sacco_id,
    sacco_name,
    registration_number,
    registration_date,
    wendi_account,
    postbank_account,
    chairperson,
    secretary,
    treasurer,
    office_address,
    office_exists,
    village,
    parish,
    sub_county,
    county,
    district,
    region,
    number_of_beneficiaries,
    total_funds_received,
    total_funds_disbursed,
    total_repayments,
    is_active,
    created_at,
    updated_at
from ranked
where _row_number = 1
