{{ config(materialized='iceberg_table') }}

with staging as (

    select
        event_id,
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
    from {{ ref('stg_pdm_pdmis_saccos') }}

)

select *
from staging
