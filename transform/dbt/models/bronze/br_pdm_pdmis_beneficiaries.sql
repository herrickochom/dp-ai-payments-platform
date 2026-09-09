{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
        beneficiary_id,
        beneficiary_token,
        nin,
        nin_hashed,
        nin_verified,
        beneficiary_name,
        date_of_birth,
        gender,
        phone,
        alternative_phone,
        email,
        phone_verified,
        household_id,
        special_group_code,
        village,
        parish,
        sub_county,
        county,
        district,
        region,
        registration_date,
        is_active,
        created_at,
        updated_at,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        category,
        source_system,
        source_group,
        year,
        month,
        day,
        load_timestamp,
        record_source
    from {{ ref('stg_pdm_pdmis_beneficiaries') }}

)

select *
from staging
